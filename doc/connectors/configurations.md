# Connector Overview

Every connector is declared inside a `[connectors.<name>]` section in the system configuration.
The `type` key selects the connector implementation; remaining keys are connector-specific parameters
documented on each connector's page.

```toml
[connectors.my_db]
type = "sql"
host = "localhost"
# ... see SQL connector page for full options
```

## Available Connectors

### File & Database

 - [CSV](csv.md): Comma-separated value files — lightweight local storage and data exchange
 - [SQL](sql.md): Relational databases via [SQLAlchemy](https://www.sqlalchemy.org/) (PostgreSQL, MySQL, SQLite, MSSQL)
 - [HDF5 Tables](tables.md): HDF5 binary store for fast columnar access with compression

### Time-Series Databases

 - [InfluxDB 1.x](influxdb1.md): InfluxDB v1 with InfluxQL queries
 - [InfluxDB 2.x](influxdb2.md): InfluxDB v2 with Flux queries and token authentication

### Messaging & Network Protocols

 - [MQTT](mqtt.md): Publish/subscribe messaging for IoT devices
 - [OPC UA](opcua.md): Industrial communication standard (OPC Unified Architecture)
 - [Modbus](modbus.md): Industrial register protocol over TCP, UDP, or serial RTU

### Serial & Sensor Buses

 - [LoRa P2P](lora_p2p.md): Peer-to-peer LoRa via Dragino LA66 (P2P firmware)
 - [LoRa WAN](lora_wan.md): LoRaWAN via Dragino LA66 (standard firmware)
 - [SDI-12](sdi12.md): Environmental sensor bus (1200 baud, master-slave)
 - [I2C](i2c.md): Two-wire sensor bus with driver support (e.g. BME280)

### External APIs

 - [ENTSO-E](entsoe.md): European electricity market data (day-ahead prices, generation, load)

### Hardware & Platform

 - [Revolution Pi](revpi.md): Kunbus Revolution Pi industrial I/O modules
 - [OpenCV Camera](opencv.md): RTSP video streams with optional motion detection

### Computation & Testing

 - [Math](math.md): Derived channels via symbolic math expressions (SymPy)
 - [Virtual](virtual.md): In-memory test connector with optional random-walk generation
