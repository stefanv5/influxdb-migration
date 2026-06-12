# Target Adapter Spec Delta

## Requirements

### V2 Write Parameters

Influx V2 target writes SHALL send `org` and `bucket` on the actual `/api/v2/write` request URL.

### Line Protocol

Target adapters SHALL escape measurement names according to line protocol rules.

Target adapters SHALL preserve a record timestamp of `0` by writing explicit timestamp `0`.
