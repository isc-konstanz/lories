# Connectors

Connectors are the bridge between lories channels and external data sources or sinks.
Each connector type handles the protocol-specific details of reading from and writing to a
particular system, while exposing a uniform interface to the rest of the framework.

A system can use multiple connectors simultaneously — for example, reading sensor data over
Modbus while storing it in an SQL database and forwarding it via MQTT.

See [Configurations](configurations.md) for a summary of all available connector types,
or jump directly to a connector page below.

```{toctree}
---
hidden: true
maxdepth: 1
---
self
configurations
csv
sql
mqtt
modbus
opcua
influxdb1
influxdb2
entsoe
revpi
math
virtual
tables
sdi12
i2c
opencv
lora_p2p
lora_wan
```
