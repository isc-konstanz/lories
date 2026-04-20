# Revolution Pi

Connector type: `revpi`, `revpi_io`, `revpi_aio`, `revpi_mio`, `revpi_ro`, `revolutionpi`

Connector for [KUNBUS Revolution Pi](https://revolutionpi.com/) industrial PCs, which are based
on the Raspberry Pi Compute Module. The modular hardware supports various I/O expansion modules
(DIO, AIO, MIO, RO). I/O values are accessed through a shared process image via the
[revpimodio2](https://revpimodio.org/) library.

```{note}
The process image interface is Linux-specific and requires direct hardware access on the
Revolution Pi device. This connector cannot be used remotely or on other platforms.
```

## How it works

On **connect**, the connector creates a `RevPiModIO` instance with auto-refresh enabled and
starts the main loop in non-blocking mode. Channels that set `listener = true` are registered
as event callbacks on the corresponding I/O point, triggering on rising edges.

On **read**, each channel's current value is fetched directly from the process image via
`self._core.io[address].value`.

On **write**, the latest value from the DataFrame is pushed to the corresponding I/O point.

On **disconnect**, all event listeners are unregistered and the process image is cleaned up.

## Dependencies

```
pip install revpimodio2        # Revolution Pi process image library
```

## Configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `cycletime` | int | -- | Override the process image cycle time (ms) |

## Channel configuration

Channels are addressed by the I/O point name as configured in PiCtory (the Revolution Pi
configuration tool). The channel's `address` corresponds to the I/O name in the process image.

Channels that should react to value changes asynchronously should set `listener = true`.

## Example configuration

```toml
[connectors.io]
type = "revpi"

[data.channels]
connector = "io"

[data.channels.digital_in_1]
type     = "bool"
address  = "I_1"
listener = true

[data.channels.analog_out_1]
type    = "float"
address = "AO_1"
```
