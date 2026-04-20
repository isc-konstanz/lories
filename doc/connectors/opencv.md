# OpenCV Camera

Connector type: `opencv`

Camera connector that captures frames from RTSP video streams using
[OpenCV](https://opencv.org/) with the FFmpeg backend. It connects to IP cameras via RTSP over
TCP, grabs single frames on demand, and encodes them as JPEG. The connector manages the
connection lifecycle per read cycle to avoid stale frame buffers.

## How it works

On **connect**, the connector creates `VideoCapture` instances for each configured stream
address (defaulting to the camera's main or sub preview path). The RTSP connection is validated
and stale frames are flushed.

For **non-streaming** channels, the connection is opened and closed on each read to ensure fresh
frames. For **streaming** channels, the connection remains open and frames are grabbed
continuously in a background thread (with optional motion detection).

On **disconnect**, all RTSP captures are released.

## Dependencies

```
pip install opencv-python      # OpenCV with FFmpeg backend
```

## Configuration

### Camera connection

| Key | Type | Default | Description |
|---|---|---|---|
| `host` | str | *(required)* | Camera hostname or IP address |
| `port` | int | `554` | RTSP port |
| `username` | str | *(required)* | Camera authentication username |
| `password` | str | *(required)* | Camera authentication password |

## Channel configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `address` | str | *(auto)* | RTSP stream path (defaults to `Preview_01_main` for snapshots or `Preview_01_sub` for streams) |
| `listener` | bool | -- | Subscribe to continuous stream events |

## Example configuration

```toml
[connectors.camera]
type     = "opencv"
host     = "192.168.1.20"
port     = 554
username = "admin"
password = "secret"

[data.channels]
connector = "camera"

[data.channels.snapshot]
type = "bytes"

[data.channels.stream]
type     = "bytes"
listener = true
address  = "Preview_01_sub"
```
