![PyPI - Python Version](https://img.shields.io/pypi/pyversions/splunkhecwriter) ![PyPI](https://img.shields.io/pypi/v/splunkhecwriter) [![Coverage](coverage.svg)](https://github.com/hellqvio86/splunkhecwriter/actions/workflows/pylint.yml) ![GitHub](https://img.shields.io/github/license/hellqvio86/splunkhecwriter) ![GitHub issues](https://img.shields.io/github/issues-raw/hellqvio86/splunkhecwriter) ![GitHub last commit](https://img.shields.io/github/last-commit/hellqvio86/splunkhecwriter) ![PyPI - Downloads](https://img.shields.io/pypi/dm/splunkhecwriter)

# splunkhecwriter

A small Python client for sending JSON events to Splunk HTTP Event Collector (HEC).
Requires Python 3.11 or newer.

## Usage

```python
from splunkhecwriter import SplunkHECWriter

with SplunkHECWriter(
    splunk_host="splunk.example.com",
    splunk_hec_token="<HEC-TOKEN>",
    sourcetype="my_app:json",
    timeout=(5, 30),  # connection and response timeout, in seconds
) as writer:
    writer.send_msg({"message": "service started", "severity": "INFO"})

    # Batches are sent as newline-delimited JSON event envelopes.
    writer.send_msgs(msgs=[{"message": "one"}, {"message": "two"}], limit=100)
```

TLS certificate verification is enabled by default. Keep it enabled in production;
configure a trusted certificate on Splunk instead of disabling verification.
Optional `event_time` arguments are Unix timestamps. When omitted, Splunk applies
its configured timestamp extraction and fallback behavior.

The client retries network failures and transient HTTP responses up to three times
by default, with exponential backoff. A timed-out POST might already have reached
Splunk, so retrying can result in duplicate events. Set `max_retries=0` to disable
retries when that tradeoff is unsuitable. Failed requests raise `SplunkHECError`;
the underlying `requests.Session` can also be managed with `close()` or a `with`
statement.

## Development

The package supports Python 3.11 and newer. CI checks the currently supported
Python releases.

Use [uv](https://docs.astral.sh/uv/) to set up the development environment and run
linting:

```sh
uv sync --locked --group dev
uv run --locked pylint src/splunkhecwriter
```

## License

This project is licensed under the Apache License; see [LICENSE.md](LICENSE.md).

## Disclaimer

This project is neither affiliated with nor endorsed by Splunk.

<p align="right">
  <a href="https://www.buymeacoffee.com/olofhellqvc">
    <img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me a Coffee" height="48">
  </a>
</p>
