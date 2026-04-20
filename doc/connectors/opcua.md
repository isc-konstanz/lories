# OPC UA

Connector type: `opc`, `opcua`

[OPC UA](https://opcfoundation.org/about/opc-technologies/opc-ua/) (Open Platform Communications
Unified Architecture) connector for industrial automation. OPC UA is a platform-independent
standard for secure, real-time data exchange between devices and systems. It offers robust
security features, interoperability across different platforms, and scalability for large-scale
deployments.

## How it works

On **connect**, the connector opens a TCP connection to the OPC UA server and resolves each
channel's node address. Addresses are built from the configured `settings` prefix (namespace
and other qualifiers) combined with the channel's `address` (or its ID if no address is given).

On **read**, each node's current value is fetched via `node.get_value()` and returned as a
timestamped DataFrame.

On **write**, the latest value from the data frame is pushed to the corresponding node via
`node.set_value()`.

## Dependencies

```
pip install opcua              # OPC UA client library
```

## Configuration

### Server connection

| Key | Type | Default | Description |
|---|---|---|---|
| `host` | str | `"127.0.0.1"` | OPC UA server hostname or IP |
| `port` | int | `4840` | Server port |
| `timeout` | int | `60` | Connection timeout (s) |
| `settings` | str | -- | Comma-separated namespace/qualifier prefixes (e.g. `"ns=2"`) |

### Authentication

| Key | Type | Default | Description |
|---|---|---|---|
| `username` | str | -- | OPC UA username (optional) |
| `password` | str | -- | OPC UA password (optional) |

## Channel configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `address` | str | *(channel ID)* | OPC UA node identifier (the `s=...` part of the NodeId) |

The full NodeId is assembled as `<settings>;s=<address>`.

## Example configuration

```toml
[connectors.opc_server]
type     = "opcua"
host     = "192.168.1.50"
port     = 4840
settings = "ns=2"

[data.channels]
connector = "opc_server"

[data.channels.tank_level]
type    = "float"
address = "Tank.Level"

[data.channels.pump_speed]
type    = "float"
address = "Pump.Speed"
```

In this example the full NodeId for `tank_level` would be `ns=2;s=Tank.Level`.
