# InfluxDB 1.x

Connector type: `influxdb1`, `influxdb_v1`

Time-series database connector for [InfluxDB 1.x](https://docs.influxdata.com/influxdb/v1/).
InfluxDB 1.x is optimised for high-throughput ingestion and real-time querying of timestamped
data using the InfluxQL query language. It provides built-in retention policies and continuous
queries for automatic data management. However, it lacks native multi-tenancy, uses a custom
authentication model without token-based access, and its storage engine can become
resource-intensive under high cardinality workloads.

## How it works

On **connect**, the connector creates an `InfluxDBClient` and verifies that the configured
database exists, creating it if necessary.

On **read**, channels are grouped by `measurement` (defaulting to the channel's group) and
optional `tag`. For each group an InfluxQL `SELECT` query is executed, and the results are
merged into a pandas DataFrame.

On **write**, data points are serialized as InfluxDB line-protocol JSON and written in bulk
via `write_points()`.

## Dependencies

```
pip install influxdb           # InfluxDB 1.x Python client
```

## Configuration

### Connection

| Key | Type | Default | Description |
|---|---|---|---|
| `host` | str | `"localhost"` | InfluxDB hostname |
| `port` | int | `8086` | InfluxDB HTTP API port |
| `user` | str | `"admin"` | Username |
| `password` | str | `"admin"` | Password |
| `database` | str | `"lories"` | Database name (created automatically if missing) |
| `timeout` | int | `10` | Request timeout (s) |

## Channel configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `measurement` | str | *(channel group)* | InfluxDB measurement name |
| `tag` | str | -- | Optional tag key for grouping fields within a measurement |

## Example configuration

```toml
[connectors.influx]
type     = "influxdb1"
host     = "localhost"
port     = 8086
user     = "admin"
password = "admin"
database = "sensors"

[data.channels]
connector = "influx"

[data.channels.temperature]
type        = "float"
measurement = "weather"
tag         = "outdoor"

[data.channels.humidity]
type        = "float"
measurement = "weather"
tag         = "outdoor"
```
