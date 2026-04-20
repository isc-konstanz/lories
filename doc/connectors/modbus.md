# Modbus

Connector type: `modbus`

Industrial communication protocol connector using [pymodbus](https://pymodbus.readthedocs.io/).
Supports reading and writing holding registers, input registers, and coils from Modbus slave
devices over TCP, UDP, or serial RTU. Configurable byte order and automatic data-type decoding
(float32, int16, etc.) are supported. However, Modbus lacks built-in authentication and
encryption, and its register-based data model requires careful address mapping per device.

## How it works

On **connect**, the connector creates a pymodbus client matching the configured protocol (TCP,
UDP, or RTU) and opens the connection. Each channel's register address and data type are parsed
into internal register descriptors.

On **read**, the connector iterates over channels grouped by `device` (slave ID). For each
channel it issues the appropriate Modbus function call (`read_holding_registers`,
`read_input_registers`, or `read_coils`), decodes the raw register values according to the
configured data type and byte order, and returns a timestamped DataFrame.

On **write**, the connector converts channel values back to register format and writes them to
the device.

## Dependencies

```
pip install pymodbus           # Modbus protocol library
```

## Configuration

### Shared

| Key | Type | Default | Description |
|---|---|---|---|
| `protocol` | str | *(required)* | Transport: `tcp`, `udp`, or `rtu` |
| `endian` | str | `"big"` | Byte order: `big` or `little` |
| `timeout` | int | `3` | Response timeout (s) |
| `retries` | int | `3` | Number of retry attempts on failure |

### TCP / UDP

| Key | Type | Default | Description |
|---|---|---|---|
| `host` | str | -- | Remote hostname or IP |
| `port` | int | `502` | Remote port |

### Serial (RTU)

| Key | Type | Default | Description |
|---|---|---|---|
| `com_port` | str | -- | Serial device path, e.g. `/dev/ttyUSB0` |
| `baudrate` | int | -- | Baud rate |
| `bytesize` | int | `8` | Data bits |
| `stopbits` | int | `1` | Stop bits |
| `parity` | str | `"N"` | Parity: `N` (none), `E` (even), `O` (odd) |

## Channel configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `address` | int | *(required)* | Register start address |
| `function` | str | `"holding_register"` | Modbus function: `holding_register`, `input_register`, or `coil` |
| `device` | int | `1` | Slave device ID (unit identifier) |
| `data_type` | str | *(auto)* | Data type override (e.g. `float32`, `int16`, `string`) |

## Example configuration

### TCP

```toml
[connectors.plc]
type     = "modbus"
protocol = "tcp"
host     = "192.168.1.10"
port     = 502
endian   = "big"

[data.channels]
connector = "plc"

[data.channels.voltage]
type      = "float"
address   = 100
function  = "input_register"
data_type = "float32"
device    = 1

[data.channels.relay]
type     = "bool"
address  = 0
function = "coil"
device   = 1
```

### Serial RTU

```toml
[connectors.meter]
type     = "modbus"
protocol = "rtu"
com_port = "/dev/ttyUSB0"
baudrate = 9600
parity   = "N"

[data.channels]
connector = "meter"

[data.channels.power]
type      = "float"
address   = 40
data_type = "float32"
device    = 2
```
