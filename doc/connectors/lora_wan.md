# LoRa WAN (LA66)

Connector type: `lora_wan_la66`

LoRaWAN connector for the [Dragino LA66](https://wiki.dragino.com/xwiki/bin/view/Main/User%20Manual%20for%20LoRaWAN%20End%20Nodes/LA66%20LoRaWAN%20Shield%20User%20Manual/)
module using the standard LoRaWAN firmware (factory default).

## How it works

On connect the module joins the LoRaWAN network using either OTAA or ABP credentials.
MAC and radio settings (class, ADR, data rate, port, confirm mode) are written once and are
EEPROM-backed on the module.

Each read cycle polls `AT+RECVB=?` for the last received downlink and queries special resources
(`AT+RSSI`, `AT+SNR`, `AT+BAT`) individually.
Each write cycle encodes channel values into a `key:value;...` text payload and transmits it as
a binary uplink via `AT+SENDB`.

Unlike the P2P connector, payload channels do **not** need `listener = true` — data is pulled
synchronously on every read cycle.

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

### Join mode

| Key | Default | Description |
|---|---|---|
| `join_mode` | `"otaa"` | Join mode: `"otaa"` or `"abp"` |

**OTAA credentials** (`join_mode = "otaa"`):

| Key | Description |
|---|---|
| `appeui` | Application EUI — 8 space-separated hex bytes |
| `appkey` | Application Key — 16 space-separated hex bytes |

**ABP credentials** (`join_mode = "abp"`):

| Key | Description |
|---|---|
| `appskey` | Application Session Key — 16 space-separated hex bytes |
| `nwkskey` | Network Session Key — 16 space-separated hex bytes |
| `daddr` | Device Address — 4 space-separated hex bytes |

ABP does not require a network join; the module is ready immediately after credential write.

### MAC / radio settings

| Key | Default | Range | Description |
|---|---|---|---|
| `lora_class` | `"A"` | `"A"`, `"C"` | Device class: A=wake on TX only, C=always listening |
| `adr` | `true` | bool | Adaptive data rate |
| `dr` | `5` | 0–7 | Fixed data rate (only used when `adr = false`) |
| `app_port` | `2` | 1–223 | LoRa application port for uplinks |
| `confirmed` | `false` | bool | Confirmed uplinks (request ACK from network) |
| `join_timeout` | `60` | ≥1 | Seconds per OTAA join attempt |
| `join_retries` | `3` | ≥1 | Number of OTAA join attempts before raising an error |

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

When set, uplink payloads are encrypted with **AES-256-ECB** before transmission and downlink
payloads are decrypted after receipt.
The wire format uses a 4-character XOR checksum prefix and space-padding to the 16-byte block
boundary. This is compatible with the Shelly `Lora.SendBytes` encryption scheme.

## Channel configuration

### Payload channels

```toml
[data.channels.temperature]
type     = "float"
lora_key = "t"    # compact key in the on-air payload; no listener = true needed
```

`lora_key` is the short identifier as it appears in the on-air payload (e.g. `t` matches `t:23.5`).

### Special channels

| `special` value | Type | Description |
|---|---|---|
| `rssi` | int | RSSI of the last received packet (dBm) via `AT+RSSI=?` |
| `snr` | int | SNR of the last received packet (dB) via `AT+SNR=?` |
| `battery` | int | Module supply voltage (mV) via `AT+BAT=?` — LoRaWAN firmware only |

```toml
[data.channels.battery]
type    = "float"
special = "battery"
```

## Example configuration

```toml
[connectors.lora_wan]
type = "lora_wan_la66"

# Serial
port     = "/dev/ttyUSB0"
baudrate = 9600

# Join mode
join_mode = "otaa"

# OTAA credentials
appeui = "00 00 00 00 00 00 00 00"
appkey = "00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00"

# ABP credentials (uncomment when join_mode = "abp")
;appskey = "00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00"
;nwkskey  = "00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00"
;daddr    = "00 00 00 00"

# MAC / radio settings
;lora_class   = "A"
;adr          = true
;dr           = 5       # only when adr = false
;app_port     = 2
;confirmed    = false
;join_timeout = 60
;join_retries = 3

# Payload codec
payload_separator = ";"
payload_delimiter = ":"

# Encryption (remove to disable)
;aes_key = "0000000000000000000000000000000000000000000000000000000000000000"
```

```toml
[data.channels]
connector = "lora_wan"
freq      = "60s"

[data.channels.temperature]
type     = "float"
lora_key = "t"

[data.channels.humidity]
type     = "float"
lora_key = "h"

[data.channels.rssi]
type    = "float"
special = "rssi"

[data.channels.snr]
type    = "float"
special = "snr"

[data.channels.battery]
type    = "float"
special = "battery"
```

## Comparison with lora_p2p_la66

| Feature | `lora_wan_la66` | `lora_p2p_la66` |
|---|---|---|
| Firmware | Factory LoRaWAN | Separate P2P firmware |
| Network | LoRaWAN (gateway required) | Direct device-to-device |
| RX model | Polled (`AT+RECVB`) per read cycle | Background thread (`AT+RECV=0`) |
| `listener = true` needed | No | Yes, for payload channels |
| `battery` special | Yes | No |
| Radio config | Managed by network/ADR | Explicit per-parameter AT commands |
| Shelly compatible | No | Yes |
