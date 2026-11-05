"""Send events to Splunk's HTTP Event Collector (HEC)."""

from __future__ import annotations

import json
import logging
import math
import socket
import time
from collections.abc import Iterable, Mapping
from ipaddress import IPv6Address, ip_address
from typing import Any

import requests

_LOGGER = logging.getLogger(__name__)
_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


class SplunkHECError(IOError):
    """Raised when Splunk HEC rejects an event or a request cannot be sent."""


# Keep the established public constructor and its attributes stable.
# pylint: disable=too-many-instance-attributes
class SplunkHECWriter:
    """Write JSON events to a Splunk HTTP Event Collector.

    Args:
        splunk_host: HEC hostname or IP address. Do not include a scheme or port.
        splunk_hec_token: HEC token used for authorization.
        splunk_port: HEC listener port (usually 8088).
        sourcetype: Default HEC sourcetype.
        index: Default Splunk index.
        http_scheme: ``http`` or ``https``.
        source: Default event source; defaults to this machine's FQDN.
        host: Default event host; defaults to this machine's FQDN.
        verify_ssl: Verify TLS certificates. Keep enabled for HTTPS in production.
        timeout: Requests timeout in seconds, or a (connect, read) timeout pair.
        max_retries: Number of retries after the initial request for transient failures.
        retry_backoff: Base delay in seconds for exponential retry backoff.
    """

    # Preserve the established constructor arguments for existing callers.
    # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals,too-many-branches
    def __init__(
        self,
        splunk_host: str,
        splunk_hec_token: str,
        splunk_port: int = 8088,
        sourcetype: str = "httpevent",
        index: str = "main",
        http_scheme: str = "https",
        source: str | None = None,
        host: str | None = None,
        verify_ssl: bool = True,
        timeout: float | tuple[float, float] = (5.0, 30.0),
        max_retries: int = 3,
        retry_backoff: float = 0.5,
    ) -> None:
        if not splunk_host or not splunk_host.strip():
            raise ValueError("splunk_host must not be empty")
        if http_scheme not in {"http", "https"}:
            raise ValueError("http_scheme must be 'http' or 'https'")
        if (
            not isinstance(splunk_port, int)
            or isinstance(splunk_port, bool)
            or not 1 <= splunk_port <= 65535
        ):
            raise ValueError("splunk_port must be between 1 and 65535")
        if not splunk_hec_token or not splunk_hec_token.strip():
            raise ValueError("splunk_hec_token must not be empty")
        if (
            not isinstance(max_retries, int)
            or isinstance(max_retries, bool)
            or max_retries < 0
        ):
            raise ValueError("max_retries must be non-negative")
        if (
            isinstance(retry_backoff, bool)
            or not isinstance(retry_backoff, (int, float))
            or not math.isfinite(retry_backoff)
            or retry_backoff < 0
        ):
            raise ValueError("retry_backoff must be a finite non-negative number")

        host_part = splunk_host.strip()
        if any(char in host_part for char in "/?#@") or any(
            char.isspace() for char in host_part
        ):
            raise ValueError("splunk_host must be a hostname or IP address only")
        if host_part.startswith("[") and host_part.endswith("]"):
            host_part = host_part[1:-1]
        try:
            parsed_ip = ip_address(host_part)
        except ValueError:
            if ":" in host_part:
                raise ValueError("splunk_host must not include a port") from None
        else:
            if isinstance(parsed_ip, IPv6Address):
                host_part = f"[{host_part}]"  # Bracket IPv6 literals in the URL.

        if isinstance(timeout, bool):
            raise ValueError("timeout must be a positive number or a pair of positive numbers")
        if isinstance(timeout, (int, float)):
            timeout_values = (timeout,)
        elif isinstance(timeout, tuple) and len(timeout) == 2:
            timeout_values = timeout
        else:
            raise ValueError("timeout must be a positive number or a pair of positive numbers")
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
            for value in timeout_values
        ):
            raise ValueError("timeout must be a positive number or a pair of positive numbers")

        self.url = f"{http_scheme}://{host_part}:{splunk_port}/services/collector/event"
        self.headers = {"Authorization": f"Splunk {splunk_hec_token.strip()}"}
        self.sourcetype = sourcetype
        self.source = source if source is not None else socket.getfqdn()
        self.host = host if host is not None else socket.getfqdn()
        self.index = index
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.hec_session = requests.Session()

    def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        self.hec_session.close()

    def __enter__(self) -> SplunkHECWriter:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _event_payload(self, event: Any, event_time: float | None) -> dict[str, Any]:
        if event_time is not None and (
            isinstance(event_time, bool)
            or not isinstance(event_time, (int, float))
            or not math.isfinite(event_time)
        ):
            raise ValueError("event_time must be a finite Unix timestamp")
        payload: dict[str, Any] = {
            "source": self.source,
            "sourcetype": self.sourcetype,
            "host": self.host,
            "index": self.index,
            "event": event,
        }
        if event_time is not None:
            payload["time"] = event_time
        return payload

    def _post(self, body: dict[str, Any] | str) -> requests.Response:
        """POST a request, retrying only network and transient server errors."""
        # Reject non-standard NaN/Infinity JSON values before making a request.
        json.dumps(body, allow_nan=False)
        for attempt in range(self.max_retries + 1):
            try:
                response = self.hec_session.post(
                    url=self.url,
                    headers=self.headers,
                    json=body if isinstance(body, dict) else None,
                    data=body if isinstance(body, str) else None,
                    verify=self.verify_ssl,
                    timeout=self.timeout,
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                if attempt >= self.max_retries:
                    raise SplunkHECError(f"Unable to send event to Splunk HEC: {exc}") from exc
                self._wait_before_retry(attempt)
                continue
            except requests.RequestException as exc:
                raise SplunkHECError(f"Unable to send event to Splunk HEC: {exc}") from exc

            if response.status_code == 200:
                return response

            if response.status_code not in _RETRYABLE_STATUS_CODES or attempt >= self.max_retries:
                detail = response.text[:1000]
                raise SplunkHECError(
                    f"Splunk HEC returned HTTP {response.status_code}: {detail}"
                )

            self._wait_before_retry(attempt)

        raise AssertionError("unreachable")

    def _wait_before_retry(self, attempt: int) -> None:
        delay = self.retry_backoff * (2**attempt)
        _LOGGER.warning("Temporary Splunk HEC failure; retrying in %.1f seconds", delay)
        time.sleep(delay)

    def send_msg(
        self, msg: Mapping[str, Any], event_time: float | None = None
    ) -> requests.Response:
        """Send one event and return Splunk's HTTP response.

        ``event_time`` is a Unix timestamp. If omitted, Splunk extracts the
        timestamp from the event or assigns one according to HEC settings.
        """
        if not isinstance(msg, Mapping):
            raise TypeError("msg must be a mapping")
        return self._post(self._event_payload(dict(msg), event_time))

    def send_msgs(
        self,
        *,
        msgs: Iterable[Mapping[str, Any]],
        event_time: float | None = None,
        limit: int = 100,
    ) -> requests.Response | None:
        """Send events in newline-delimited HEC batches.

        Returns the last response, or ``None`` for an empty iterable. ``limit``
        is the maximum number of events in each request.
        """
        if limit <= 0:
            raise ValueError("limit must be greater than zero")

        last_response = None
        batch: list[str] = []
        for msg in msgs:
            if not isinstance(msg, Mapping):
                raise TypeError("every item in msgs must be a mapping")
            batch.append(
                json.dumps(
                    self._event_payload(dict(msg), event_time),
                    allow_nan=False,
                )
            )
            if len(batch) == limit:
                last_response = self._post("\n".join(batch) + "\n")
                batch.clear()

        if batch:
            last_response = self._post("\n".join(batch) + "\n")
        return last_response


__all__ = ["SplunkHECError", "SplunkHECWriter"]
