# I2C

Connector type: `i2c`

Two-wire serial bus connector for reading sensors and peripherals on embedded systems such as the
Raspberry Pi. Uses the [smbus2](https://pypi.org/project/smbus2/) library to communicate over
the I2C bus. Supports named sensor drivers (currently BME280) as well as raw register read/write
access. Falls back to a mock implementation when smbus2 is unavailable (e.g. on Windows or in
development environments).

## How it works

On **connect**, the I2C bus is opened at the configured bus number. Channels are grouped by
`sensor` and `address`. For supported sensor types (e.g. BME280), a driver instance is created
that handles initialisation and calibration automatically.

On **read**, each sensor driver is polled for its named measurements (e.g. temperature, humidity,
pressure). For channels without a sensor driver, raw register reads are performed and the
result is interpreted as a big-endian integer.

On **write**, values are written as single bytes to the specified register address.

## Dependencies

```
pip install smbus2             # I2C bus access
pip install RPi.bme280         # BME280 sensor driver (optional)
```

## Configuration

### Bus

| Key | Type | Default | Description |
|---|---|---|---|
| `port` | int | `1` | I2C bus number (e.g. `1` for `/dev/i2c-1`) |

## Channel configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `address` | int | *(required)* | 7-bit I2C device address (decimal or `0x`-prefixed hex) |
| `sensor` | str | -- | Sensor driver to use (e.g. `bme280`). Omit for raw register access |
| `measurement` | str | -- | Named measurement from the sensor driver (e.g. `temperature`, `humidity`, `pressure`) |
| `register` | int | -- | Register address for raw byte access (required when `sensor` is not set) |
| `length` | int | `1` | Number of bytes to read in raw register mode |

## Supported sensors

### BME280

The [Bosch BME280](https://www.bosch-sensortec.com/products/environmental-sensors/humidity-sensors-bme280/)
is a combined temperature, humidity, and pressure sensor. Default I2C address: `0x76` or `0x77`.

Available measurements: `temperature`, `humidity`, `pressure`.

## Example configuration

```toml
[connectors.i2c]
type = "i2c"
port = 1

[data.channels]
connector = "i2c"

[data.channels.temperature]
type        = "float"
sensor      = "bme280"
address     = 0x76
measurement = "temperature"

[data.channels.humidity]
type        = "float"
sensor      = "bme280"
address     = 0x76
measurement = "humidity"

[data.channels.pressure]
type        = "float"
sensor      = "bme280"
address     = 0x76
measurement = "pressure"
```

### Raw register access

```toml
[data.channels.raw_value]
type     = "int"
address  = 0x48
register = 0x00
length   = 2
```
