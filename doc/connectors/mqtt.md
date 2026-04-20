# MQTT

Connector type: `mqtt`

Lightweight publish/subscribe messaging connector using the
[MQTT](https://mqtt.org/) protocol. Designed for IoT applications where
resource-constrained devices communicate over limited-bandwidth or high-latency networks.
The connector subscribes to MQTT topics on connect and pushes incoming messages directly to
channels via a background listener thread.

## How it works

On **connect**, the connector establishes a connection to the MQTT broker, subscribes to the
topics configured on each channel, and starts a background loop (`loop_start()`) that listens for
incoming messages. Each topic has a dedicated listener that forwards payloads to the associated
channels via `channel.set()`.

Because data arrives asynchronously, MQTT channels should set `listener = true` so the regular
read cycle does not attempt to poll them.

On **disconnect**, the background loop is stopped and the broker connection is closed.

```{note}
Writing to MQTT topics is not yet implemented. Only subscribe (read) is currently supported.
```

## Dependencies

```
pip install paho-mqtt          # MQTT client library
```

## Configuration

### Broker connection

| Key | Type | Default | Description |
|---|---|---|---|
| `host` | str | `"localhost"` | MQTT broker hostname or IP |
| `port` | int | `1883` | Broker port |
| `transport` | str | `"tcp"` | Transport protocol: `tcp` or `websockets` |
| `clean_session` | bool | `true` | Start a clean MQTT session (discard prior subscriptions) |
| `timeout` | int | `60` | Connection keep-alive timeout (s) |

### Authentication

| Key | Type | Default | Description |
|---|---|---|---|
| `username` | str | -- | MQTT username (optional) |
| `password` | str | -- | MQTT password (optional) |

## Channel configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `topic` | str | *(required)* | MQTT topic this channel subscribes to |

Channels receiving data from MQTT should set `listener = true`.

## Example configuration

```toml
[connectors.broker]
type = "mqtt"
host = "192.168.1.100"
port = 1883

[data.channels]
connector = "broker"
listener  = true

[data.channels.temperature]
type  = "float"
topic = "sensors/outdoor/temperature"

[data.channels.humidity]
type  = "float"
topic = "sensors/outdoor/humidity"
```
