# Changelog

## 0.5.0

- Add bounded retries with exponential backoff for network errors and transient
  HTTP responses. Requests now have a finite timeout by default, and failures
  raise `SplunkHECError` instead of returning `None` after retry exhaustion.
- Add context manager and `close()` support for the HTTP session.
- Stream batch inputs into bounded newline-delimited requests and skip network
  calls for empty batches.
- Emit event objects as JSON values, omit timestamps unless explicitly passed,
  and validate configuration and event types.
- Align package license metadata with the Apache-2.0 license file.
- Add a GitHub Actions release workflow for publishing to PyPI with trusted
  publishing (OIDC).
