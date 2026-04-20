# LoRa P2P (LA66)

Connector type: `lora_p2p_la66`

Peer-to-peer LoRa connector for the [Dragino LA66](https://wiki.dragino.com/xwiki/bin/view/Main/User%20Manual%20for%20LoRaWAN%20End%20Nodes/LA66%20LoRaWAN%20Shield%20User%20Manual/)
module using its dedicated P2P firmware.

```{important}
The LA66 P2P firmware is a **separate firmware image** from the factory LoRaWAN firmware and must
be flashed explicitly before this connector will work.
Flashing instructions:
[Instruction for LA66 Peer to Peer firmware](https://wiki.dragino.com/xwiki/bin/view/Main/User%20Manual%20for%20LoRaWAN%20End%20Nodes/LA66%20LoRaWAN%20Shield%20User%20Manual/Instruction%20for%20LA66%20Peer%20to%20Peer%20firmware/)
```

## How it works

Radio parameters are written to the module on connect via individual AT commands.
If any value differs from the module's stored setting, the new value is written and the module is
reset (`ATZ`) so it takes effect.

A background thread polls `AT+RECV=0` on a configurable interval.
Each response contains the hex payload, RSSI, and SNR of the last received packet.
The payload is decoded (and decrypted if `aes_key` is set) and pushed directly to channels via
`channel.set()`.

Because payload values arrive asynchronously, payload channels must set `listener = true` so
the regular read loop does not try to poll them.
Special channels (`rssi`, `snr`) are read on-demand during the normal read cycle from the cached
values populated by the background thread.

## Dependencies

```
pip install pyserial          # serial port access
pip install pycryptodome      # only required when aes_key is configured
```

## Configuration

### Serial port

| Key | Type | Default | Description |
|---|---|---|---|
| `port` | str | — | Serial device, e.g. `/dev/ttyUSB0` |
| `baudrate` | int | `9600` | Baud rate |
| `timeout` | float | `3` | Serial read timeout (s) |
| `cmd_timeout` | float | `5.0` | Seconds to wait for any AT command response |

### Radio parameters

All parameters are sent to the module as `AT+<CMD>=<tx_value>,<rx_value>` pairs (TX and RX use
the same value). The module is reset after any change.

| Key | AT command | Default | Range | Description |
|---|---|---|---|---|
| `p2p_freq` | `AT+FRE` | `868.1` | any float | TX/RX frequency in MHz |
| `p2p_sf` | `AT+SF` | `12` | 5–12 | Spreading factor |
| `p2p_bw` | `AT+BW` | `0` | 0–9 | Bandwidth: 0=125 kHz, 1=250 kHz, 2=500 kHz, 3=62.5 kHz, 4=41.67 kHz, 5=31.25 kHz, 6=20.83 kHz, 7=15.63 kHz, 8=10.42 kHz, 9=7.81 kHz |
| `p2p_power` | `AT+POWER` | `20` | 0–22 | TX power in dBm |
| `p2p_cr` | `AT+CR` | `1` | 1–4 | Coding rate: 1=4/5, 2=4/6, 3=4/7, 4=4/8 |
| `p2p_header` | `AT+HEADER` | `0` | 0–1 | Header type: 0=explicit (variable length), 1=implicit (fixed length) |
| `p2p_crc` | `AT+CRC` | `1` | 0–1 | CRC: 0=off, 1=on |
| `p2p_iq` | `AT+IQ` | `0` | 0–1 | InvertIQ: 0=standard, 1=inverted |
| `p2p_preamble` | `AT+PREAMBLE` | `10` | 0–65535 | Preamble length |
| `p2p_syncword` | `AT+SYNCWORD` | `0` | 0–1 | Sync word: 0=private (0x3444), 1=public |
| `p2p_group` | `AT+GROUPMOD` | `0` | 0–255 | Group filter: 0=accept all, 1–255=accept matching group only |
| `p2p_rx_timeout` | `AT+RXMOD` (param 1) | `65535` | 0–65535 | RX window: 0=no RX, 1–65534=open for N seconds, 65535=always open |
| `p2p_ack_mode` | `AT+RXMOD` (param 2) | `0` | 0–2 | ACK mode: 0=no ACK, 1=mirror received message, 2=reply with 0x00FF |
| `p2p_poll_interval` | — | `1.0` | ≥0.1 | Seconds between `AT+RECV=0` polls in the background thread |

### Payload codec

| Key | Default | Description |
|---|---|---|
| `payload_separator` | `;` | Separator between key-value pairs |
| `payload_delimiter` | `:` | Separator between key and value within a pair |

On-air format example: `t:23.5;h:60`

### Encryption

| Key | Default | Description |
|---|---|---|
| `aes_key` | — | AES-256 key as 64 hex characters; omit to disable encryption |

When set, payloads are decrypted with **AES-256-ECB** after receipt.
The expected wire format is a 4-character XOR checksum prefix followed by the plaintext, padded to
a 16-byte boundary with spaces. This is compatible with the Shelly `Lora.SendBytes` encryption
scheme. See [Shelly integration](#shelly-integration) below.

## Channel configuration

### Payload channels

```toml
[data.channels.temperature]
type      = "float"
lora_key  = "t"      # compact key used in the on-air payload
listener  = true     # required: payload arrives via background RX thread
```

`lora_key` is the short identifier as it appears in the on-air payload (e.g. `t` matches `t:23.5`).
`listener = true` instructs the scheduler to skip polling this channel — values are pushed
asynchronously by the RX thread.

### Special channels

| `special` value | Type | Description |
|---|---|---|
| `rssi` | int | RSSI of the last received packet (dBm), cached by the RX thread |
| `snr` | int | SNR of the last received packet (dB), cached by the RX thread |

`battery` is **not** available in the P2P firmware.

```toml
[data.channels.rssi]
type    = "float"
special = "rssi"
```

## Example configuration

```toml
[connectors.lora_p2p]
type = "lora_p2p_la66"

# Serial
port     = "/dev/ttyUSB0"
baudrate = 9600

# Radio — must match all peer devices
p2p_freq     = 868.1   # MHz
p2p_bw       = 0       # 125 kHz
p2p_sf       = 7
p2p_power    = 14      # dBm
p2p_cr       = 1       # 4/5
p2p_header   = 0       # explicit
p2p_crc      = 1       # on
p2p_iq       = 0       # standard
p2p_preamble = 8
p2p_syncword = 0       # private

# Payload codec
payload_separator = ";"
payload_delimiter = ":"

# Encryption (remove to disable)
aes_key = "2664ed9a5fac9b03164bc2d57b339644d08e360e99a28fc05b307d6e15d085a5"
```

```toml
[data.channels]
connector = "lora_p2p"
freq      = "60s"

[data.channels.temperature]
type     = "float"
lora_key = "t"
listener = true

[data.channels.humidity]
type     = "float"
lora_key = "h"
listener = true

[data.channels.rssi]
type    = "float"
special = "rssi"

[data.channels.snr]
type    = "float"
special = "snr"
```

## Shelly integration

The Shelly LoRa add-on can act as a P2P sender that the LA66 receives.
The Shelly script (`lora_p2p_shelly.js`) implements the same AES-256-ECB + XOR checksum scheme
and uses `Shelly.call('Lora.SendBytes', ...)` to transmit.

### CONFIG block

```js
const CONFIG = {
  aesKey: '2664ed9a5fac9b03164bc2d57b339644d08e360e99a28fc05b307d6e15d085a5',
  loraId: 100,                // Shelly internal LoRa peripheral ID
  checksumSize: 4,            // fixed — XOR checksum length in hex chars
  blockSize: 16,              // fixed — AES block size in bytes
  maxRetries: 3,              // send retries on failure
  retryDelayMs: 1000,         // ms between retries
  powerCheckIntervalMs: 5000, // how often to poll Switch.GetStatus (ms)
  sendIntervalHours: 12,      // transmission interval
  sensorId: 's0',             // payload key prefix → on-air key becomes "s0_trig"
};
```

- `aesKey` must be identical to the `aes_key` in the lories connector config.
- The current script tracks power-on transitions (0 W → non-zero) and sends the accumulated
  counter every `sendIntervalHours` hours as `<sensorId>_trig:<count>`, then resets the counter.

### Shelly UI → LA66 parameter mapping

The Shelly UI only exposes frequency, bandwidth, and spreading factor.
Set the remaining LA66 parameters to these recommended values to match Shelly's fixed firmware defaults:

| Shelly UI | LA66 key | Notes |
|---|---|---|
| Channel (frequency) | `p2p_freq` | Sub-band L: 865.000–867.875 MHz; custom: 868.000 |
| Bandwidth 125 kHz | `p2p_bw = 0` | |
| Bandwidth 250 kHz | `p2p_bw = 1` | |
| SF (data rate) | `p2p_sf` | 7–12, direct match |
| Preset "Long range" | `p2p_bw=0, p2p_sf=12` | |
| Preset "Balanced" | `p2p_bw=0, p2p_sf=9` | |
| Preset "High Throughput" | `p2p_bw=1, p2p_sf=7` | |

Hidden Shelly defaults (not exposed in UI):

| Parameter | Recommended LA66 value |
|---|---|
| `p2p_cr` | `1` (4/5) |
| `p2p_header` | `0` (explicit) |
| `p2p_crc` | `1` (on) |
| `p2p_iq` | `0` (standard) |
| `p2p_preamble` | `8` |
| `p2p_syncword` | `0` (private — verify on real hardware) |
