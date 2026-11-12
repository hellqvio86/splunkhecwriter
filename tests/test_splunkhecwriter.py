"""Unit tests for splunkhecwriter."""

from __future__ import annotations

import math
from unittest.mock import MagicMock, patch

import pytest
import requests

from splunkhecwriter import SplunkHECError, SplunkHECWriter


def test_constructor_validation() -> None:
    """Verify input validation in SplunkHECWriter constructor."""
    with pytest.raises(ValueError, match="splunk_host must not be empty"):
        SplunkHECWriter("", "tok")
    with pytest.raises(ValueError, match="http_scheme"):
        SplunkHECWriter("host", "tok", http_scheme="ftp")
    with pytest.raises(ValueError, match="splunk_port"):
        SplunkHECWriter("host", "tok", splunk_port=0)
    with pytest.raises(ValueError, match="splunk_port"):
        SplunkHECWriter("host", "tok", splunk_port=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="splunk_hec_token"):
        SplunkHECWriter("host", "   ")
    with pytest.raises(ValueError, match="max_retries"):
        SplunkHECWriter("host", "tok", max_retries=-1)
    with pytest.raises(ValueError, match="max_retries"):
        SplunkHECWriter("host", "tok", max_retries=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="retry_backoff"):
        SplunkHECWriter("host", "tok", retry_backoff=-0.1)
    with pytest.raises(ValueError, match="retry_backoff"):
        SplunkHECWriter("host", "tok", retry_backoff=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="hostname or IP"):
        SplunkHECWriter("https://host/path", "tok")
    with pytest.raises(ValueError, match="must not include a port"):
        SplunkHECWriter("splunk.example.com:8088", "tok")
    with pytest.raises(ValueError, match="timeout"):
        SplunkHECWriter("host", "tok", timeout=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="timeout"):
        SplunkHECWriter("host", "tok", timeout=(1.0, 2.0, 3.0))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="timeout"):
        SplunkHECWriter("host", "tok", timeout=0)


def test_ipv6_and_context_manager() -> None:
    """Verify IPv6 literal bracketing and context manager lifecycle."""
    with SplunkHECWriter("::1", "tok", timeout=5.0) as writer:
        assert writer.url == "https://[::1]:8088/services/collector/event"
    with SplunkHECWriter("[2001:db8::1]", "tok") as writer2:
        assert writer2.url == "https://[2001:db8::1]:8088/services/collector/event"


def test_send_msg_and_event_time() -> None:
    """Verify single message sending, event_time handling, and validation."""
    writer = SplunkHECWriter("splunk.local", "secret-token", source="src1", host="h1")
    mock_resp = MagicMock(status_code=200)
    writer.hec_session.post = MagicMock(return_value=mock_resp)

    resp = writer.send_msg({"key": "val"}, event_time=1700000000.5)
    assert resp is mock_resp
    call_kwargs = writer.hec_session.post.call_args.kwargs
    assert call_kwargs["json"]["time"] == 1700000000.5
    assert call_kwargs["json"]["source"] == "src1"
    assert call_kwargs["json"]["host"] == "h1"

    with pytest.raises(TypeError, match="msg must be a mapping"):
        writer.send_msg(["not", "mapping"])  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="event_time"):
        writer.send_msg({"k": "v"}, event_time=math.nan)

    with pytest.raises(ValueError, match="event_time"):
        writer.send_msg({"k": "v"}, event_time=True)  # type: ignore[arg-type]


def test_send_msgs_batching() -> None:
    """Verify batch message sending with limit and empty iterables."""
    writer = SplunkHECWriter("splunk.local", "secret-token")
    mock_resp = MagicMock(status_code=200)
    writer.hec_session.post = MagicMock(return_value=mock_resp)

    assert writer.send_msgs(msgs=[]) is None

    res = writer.send_msgs(msgs=[{"a": 1}, {"b": 2}, {"c": 3}], limit=2)
    assert res is mock_resp
    assert writer.hec_session.post.call_count == 2

    with pytest.raises(ValueError, match="limit must be greater than zero"):
        writer.send_msgs(msgs=[{"a": 1}], limit=0)

    with pytest.raises(TypeError, match="every item in msgs must be a mapping"):
        writer.send_msgs(msgs=["bad"])  # type: ignore[list-item]


def test_retries_and_errors() -> None:
    """Verify retry behavior on timeouts, transient HTTP errors, and fatal errors."""
    writer = SplunkHECWriter("splunk.local", "tok", max_retries=2, retry_backoff=0.01)

    # Retry on 503 then succeed on 200
    r503 = MagicMock(status_code=503, text="Busy")
    r200 = MagicMock(status_code=200, text="OK")
    writer.hec_session.post = MagicMock(side_effect=[r503, r200])
    with patch("time.sleep"):
        assert writer.send_msg({"ok": True}) is r200

    # Non-retryable 400 raises immediately
    r400 = MagicMock(status_code=400, text="Bad Request")
    writer.hec_session.post = MagicMock(return_value=r400)
    with pytest.raises(SplunkHECError, match="HTTP 400"):
        writer.send_msg({"bad": True})

    # Exhaust retries on Timeout
    writer.hec_session.post = MagicMock(side_effect=requests.Timeout("timed out"))
    with patch("time.sleep"), pytest.raises(SplunkHECError, match="Unable to send"):
        writer.send_msg({"timeout": True})

    # Non-retryable RequestException
    writer.hec_session.post = MagicMock(side_effect=requests.RequestException("fatal"))
    with pytest.raises(SplunkHECError, match="Unable to send"):
        writer.send_msg({"fatal": True})
