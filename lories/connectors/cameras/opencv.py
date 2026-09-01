# -*- coding: utf-8 -*-
# ruff: noqa: E402  -- OPENCV_LOG_LEVEL must be set before `import cv2`
"""
lories.connectors.cameras.opencv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

import os
import time
from typing import Any, Dict, Optional

# Silence OpenCV WARNs at videoio init. The FFmpeg interrupt callback
# logs "Stream timeout triggered after Xms" via CV_LOG_WARNING on every
# RTSP stall; we surface real disconnects via ConnectionError. The env
# var is read once during cv2 init, so it must precede `import cv2`.
# (Older builds — e.g. the OpenCV on isc-agri — don't expose the
# cv2.utils.logging Python API, so the env var is the portable path.)
os.environ.setdefault("OPENCV_LOG_LEVEL", "ERROR")

import cv2

from lories.connectors import ConnectionError, ConnectorError, register_connector_type
from lories.connectors.cameras import CameraConnector
from lories.core.configs.parameters import ChannelParameter, Parameter
from lories.typing import Configurations, Resource, Resources


@register_connector_type("opencv")
class OpenCV(CameraConnector):
    """
    OpenCV-based camera connector that captures frames from RTSP streams using the FFmpeg backend.
    It connects to IP cameras via RTSP with TCP transport, grabs single frames on demand, and encodes
    them as JPEG. The connector manages connection lifecycle per read cycle to avoid stale frame buffers.
    Performance depends on network latency and camera firmware; some cameras may require adjusted timeouts
    or stream paths.
    """

    # Vendor → (main-stream path, sub-stream path) appended to "rtsp://<host>:<port>/".
    VENDOR_PATHS: Dict[str, tuple] = {
        "reolink": ("Preview_01_main", "Preview_01_sub"),
        "hikvision": ("Streaming/Channels/101", "Streaming/Channels/102"),
        "dahua": ("cam/realmonitor?channel=1&subtype=0", "cam/realmonitor?channel=1&subtype=1"),
        "axis": ("axis-media/media.amp", "axis-media/media.amp"),
    }
    DEFAULT_VENDOR: str = "reolink"

    _FRAME_READ_RETRIES: int = 3
    _RECONNECT_RETRIES: int = 2

    _host = Parameter(key="host", type=str, required=True, desc="RTSP camera hostname or IP address")
    _port = Parameter(key="port", type=int, default=554, min=1, max=65535, desc="RTSP camera TCP port")
    _username = Parameter(key="username", type=str, required=False, desc="RTSP authentication username")
    _password = Parameter(key="password", type=str, required=False, desc="RTSP authentication password")
    _vendor = Parameter(
        key="vendor", type=str, required=False, default="reolink", desc="This provides defaults for RTSP addresses"
    )
    _frame_std_min = Parameter(
        key="frame_std_min",
        type=float,
        default=5.0,
        min=0.0,
        desc=(
            "Minimum pixel standard deviation for a frame to count as intact. Partial h264 decodes from RTSP "
            "packet loss arrive as a successful read of a flat grey or green field rather than as a read "
            "failure, detectable only by how little the frame varies. Raise it if corrupt frames still get "
            "through, lower it for scenes that are legitimately near-uniform (a blank wall, a dark night "
            "view); 0 accepts every decodable frame."
        ),
    )

    address = ChannelParameter(
        key="address",
        type=str,
        required=False,
        desc=(
            "Vendor-specific RTSP path appended to 'rtsp://<host>:<port>/' (e.g. 'Streaming/Channels/101'). "
            "Defaults to the configured vendor's main or sub stream path."
        ),
    )

    _host: str
    _port: int
    _username: Optional[str]
    _password: Optional[str]

    _vendor: str
    _preview_main: str
    _preview_sub: str

    _vendor: str
    _preview_main: str
    _preview_sub: str

    _frame_std_min: float

    _captures: Dict[str, cv2.VideoCapture]

    def __getstate__(self) -> Dict[str, Any]:
        state = super().__getstate__()
        state.pop("_captures", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        super().__setstate__(state)
        self._captures = {}

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        if not self._host or not self._port:
            raise ValueError("Camera configuration requires 'host' and 'port'")

        if self._vendor not in OpenCV.VENDOR_PATHS:
            raise ValueError(f"Unknown camera vendor '{self._vendor}'. Supported: {sorted(OpenCV.VENDOR_PATHS)}")
        self._preview_main, self._preview_sub = OpenCV.VENDOR_PATHS[self._vendor]

        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
            "rtsp_transport;tcp|"  # use TCP only
            "max_delay;500000"  # 0.5 sec max internal delay
        )
        self._captures = {}

    def connect(self, resources: Resources) -> None:
        super().connect(resources)

        # Validate connection only to throw ConnectionError when connect is called by the manager
        for resource in resources:
            streaming = self._is_streaming(resource)
            address = resource.get("address", default=self._preview_sub if streaming else self._preview_main)

            if address not in self._captures:
                # TODO: Make timeouts configurable
                capture = cv2.VideoCapture()
                capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                capture.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 3000)
                capture.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 3000)
                self._captures[address] = capture
            else:
                capture = self._captures[address]
            if not capture.isOpened():
                self._open_capture(address, capture)
            if not streaming:
                self._disconnect(capture)

    def _open_capture(self, address: str, capture: cv2.VideoCapture) -> None:
        auth = f"{self._username}:{self._password}@" if self._username and self._password else ""
        address = f"{self._host}:{self._port}/{address}"
        url = f"rtsp://{auth}{address}"
        capture.open(url, apiPreference=cv2.CAP_FFMPEG)

        if not capture.isOpened():
            raise ConnectionError(self, f"Cannot open RTSP stream: 'rtsp://#:#@{address}'")

        # Bare grab() calls cannot tell a decodable frame from a corrupt one, so
        # flushing by count left the first read after open returning garbage.
        # Retrieve and validate instead, until a clean frame arrives.
        deadline = time.monotonic() + 2.0
        while True:
            if not capture.grab():
                raise ConnectionError(self, "Failed to grab frame")
            status, frame = capture.retrieve()
            if status and not self._is_corrupt(frame):
                break
            if time.monotonic() >= deadline:
                raise ConnectionError(self, "Received only corrupt frames while opening")

        self._logger.debug(f"Opened VideoCapture to RTSP URL 'rtsp://#:#@{address}'")

    def _is_corrupt(self, frame) -> bool:
        return bool(frame is None or frame.size == 0 or frame.std() < self._frame_std_min)

    def disconnect(self) -> None:
        super().disconnect()
        for address in list(self._captures.keys()):
            capture = self._captures.pop(address)
            self._disconnect(capture)

    def _disconnect(self, capture: cv2.VideoCapture) -> None:
        if capture.isOpened():
            capture.release()
            self._logger.debug("Released VideoCapture")

    def read_frame(self, resource: Resource) -> bytes:
        streaming = self._is_streaming(resource)

        address = resource.get("address", default=self._preview_sub if streaming else self._preview_main)

        # A non-streaming read releases its capture in _read_frame's finally, so
        # retrying re-opens it: that is the reconnect. A streaming read borrows
        # the long-lived capture owned by the stream loop and gets one attempt.
        attempts = 1 if streaming else 1 + self._RECONNECT_RETRIES
        for attempt in range(1, attempts):
            try:
                return self._read_frame(streaming, address)
            except ConnectionError as e:
                self._logger.warning(
                    f"Read failed for '{address}' ({e}); reconnecting and retrying ({attempt + 1}/{attempts})"
                )
        return self._read_frame(streaming, address)

    def _read_frame(self, streaming: bool, address: str) -> bytes:
        capture = self._captures.get(address, None)
        if capture is None:
            raise ConnectionError(self, f"Cannot open RTSP stream: 'rtsp://#:#@{self._host}:{self._port}/{address}'")
        try:
            if not streaming and not capture.isOpened():
                self._open_capture(address, capture)
            if not capture.isOpened():
                raise ConnectionError(
                    self, f"Cannot open RTSP stream: 'rtsp://#:#@{self._host}:{self._port}/{address}'"
                )

            for _ in range(self._FRAME_READ_RETRIES):
                if streaming:
                    # FFmpeg's RTSP demuxer keeps an internal FIFO. CAP_PROP_BUFFERSIZE
                    # is ignored by this backend, so we drain queued frames with cheap
                    # grab() calls (no decode) and only retrieve() the most recent one.
                    # `grab()` is blocking on RTSP: once the backlog is empty it waits
                    # for the next network frame at camera-fps. A fixed-count drain
                    # therefore caps throughput at <1 fps on a fresh buffer. Time-budget
                    # the drain so we exit as soon as the backlog is empty.
                    if not capture.grab():
                        raise ConnectionError(self, "Failed to grab frame")
                    deadline = time.monotonic() + 0.005  # 5 ms additional drain budget
                    while time.monotonic() < deadline:
                        if not capture.grab():
                            break
                    status, frame = capture.retrieve()
                else:
                    status, frame = capture.read()
                if not status or frame is None:
                    raise ConnectionError(self, "Failed to retrieve frame")
                if not self._is_corrupt(frame):
                    break
            else:
                raise ConnectionError(self, "Received only corrupt frames")

            status, buffer = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 90])
            if not status:
                raise ConnectionError(self, "Failed to encode JPEG")

            return buffer.tobytes()

        except cv2.error as e:
            raise ConnectorError(self, f"OpenCV error: {e}")
        finally:
            if not streaming:
                self._disconnect(capture)
