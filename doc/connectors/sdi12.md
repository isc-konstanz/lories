# SDI-12

Connector type: `sdi12`

[SDI-12](https://www.sdi-12.org/) (Serial-Digital Interface at 1200 baud) connector for
environmental sensors. SDI-12 is a standard protocol that supports addressing up to 62 sensors
on a single data line using a master-slave communication model. It is widely adopted in
hydrology, meteorology, and soil science. However, its low baud rate and sequential polling
model limit throughput, and its timing-sensitive break signaling requires precise serial control.

```{note}
This connector is **read-only** -- writing to SDI-12 sensors is not supported.
```

## How it works

On **connect**, the serial port is opened with fixed SDI-12 parameters (1200 baud, 7 data bits,
even parity).

On **read**, channels are grouped by sensor address. For each sensor the connector runs a full
measurement cycle:

1. Send a break signal (>12 ms of spacing).
2. Issue the measurement command `aM!` (where `a` is the sensor address).
3. Parse the response to extract the wait time (`ttt` seconds).
4. Wait for the measurement to complete.
5. Issue data commands `aD0!`, `aD1!`, ... to retrieve values.
6. Parse the response and map values to channels by their `index` position.

## Dependencies

```
pip install pyserial           # serial port access
```

## Configuration

### Serial port

This connector inherits serial port parameters from the base serial connector. SDI-12 uses
fixed settings (1200 baud, 7E1), so typically only the port needs to be configured.

| Key | Type | Default | Description |
|---|---|---|---|
| `port` | str | -- | Serial device, e.g. `/dev/ttyUSB0` |

## Channel configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `sensor` | int | *(required)* | SDI-12 sensor address (0--9, A--Z, a--z) |
| `data` | int | `0` | Data command index (0 for `D0!`, 1 for `D1!`, ...) |
| `index` | int | `0` | Value position within the data response |

## Example configuration

```toml
[connectors.sdi]
type = "sdi12"
port = "/dev/ttyUSB0"

[data.channels]
connector = "sdi"

[data.channels.soil_moisture]
type   = "float"
sensor = 0
data   = 0
index  = 0

[data.channels.soil_temperature]
type   = "float"
sensor = 0
data   = 0
index  = 1

[data.channels.soil_ec]
type   = "float"
sensor = 0
data   = 0
index  = 2
```

In this example, all three measurements come from sensor address `0`, data response `D0!`,
at value positions 0, 1, and 2.
