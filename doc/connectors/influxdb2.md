# InfluxDB 2.x

Connector type: `influxdb2`, `influxdb_v2`

Time-series database connector for [InfluxDB 2.x](https://docs.influxdata.com/influxdb/v2/).
InfluxDB 2.x introduces the Flux query language, token-based authentication, and a unified API
for data ingestion, querying, and management. It supports organisations and buckets for
multi-tenant data isolation, built-in task scheduling, and dashboarding. Migration from 1.x
requires adapting queries and authentication workflows.

## How it works

On **connect**, the connector builds an HTTP(S) URL from the configured host and port, creates
an `InfluxDBClient` with token authentication, and verifies that the target bucket exists
(creating it with infinite retention if missing).

On **read**, channels are grouped by `measurement` and optional `tag`. For each group a Flux
query is built using `from(bucket) |> range() |> filter()` and the results are pivoted into a
pandas DataFrame.

On **write**, data is written via the synchronous write API as pandas DataFrames, grouped by
measurement and tag.

## Dependencies

```
pip install influxdb-client    # InfluxDB 2.x Python client
```

## Configuration

### Connection

| Key | Type | Default | Description |
|---|---|---|---|
| `host` | str | `"localhost"` | InfluxDB hostname |
| `port` | int | `8086` | InfluxDB HTTP API port |
| `org` | str | *(required)* | Organisation name |
| `bucket` | str | *(required)* | Bucket name (created automatically if missing) |
| `token` | str | *(required)* | API token |
| `timeout` | float | `10.0` | Request timeout (s) |

### SSL

| Key | Type | Default | Description |
|---|---|---|---|
| `ssl` | bool | `false` | Use HTTPS instead of HTTP |
| `ssl_verify` | bool | `true` | Verify SSL certificate |

## Channel configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `measurement` | str | *(channel group)* | InfluxDB measurement name |
| `tag` | str | -- | Optional tag key for grouping fields within a measurement |

## Example configuration

```toml
[connectors.influx]
type   = "influxdb2"
host   = "influx.example.com"
port   = 8086
org    = "my-org"
bucket = "sensor-data"
token  = "my-api-token"
ssl    = true

[data.channels]
connector = "influx"

[data.channels.temperature]
type        = "float"
measurement = "weather"

[data.channels.humidity]
type        = "float"
measurement = "weather"
```
