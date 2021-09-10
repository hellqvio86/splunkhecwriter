# splunkhecwriter
Library for sending log events to Splunk

## Example Code block 1
```python

  from splunkhecwriter import SplunkHECWriter
  hec_token = '<HEC-TOKEN-HERE>'
  splunk_server = '127.0.0.1'

  hec_writer = SplunkHECWriter(splunk_host=splunk_server, splunk_hec_token=hec_token)

  msg =  { 'foo': 'bar' }
  hec_writer.send_msg(msg=msg)

```

## Example Code block 2
```python

  from splunkhecwriter import SplunkHECWriter
  hec_token = '<HEC-TOKEN-HERE>'
  splunk_server = '127.0.0.1'

  hec_writer = SplunkHECWriter(splunk_host=splunk_server, splunk_hec_token=hec_token)

  msgs = []
  msgs.append({ 'foo': 'bar' })
  msgs.append({ 'bar': 'foo'})
  hec_writer.send_msgs(msgs=msgs)

```

## License

This project is licensed under the Apache License - see the [LICENSE.md](LICENSE.md) file for details

## Disclaimer
This custom component is neither affiliated with nor endorsed by Splunk.
