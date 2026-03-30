# -*- coding: utf-8 -*-
"""
lories.connectors.cameras.opencv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

import os
from typing import Dict

import cv2

from lories.connectors import ConnectionError, ConnectorError, register_connector_type
from lories.connectors.cameras import CameraConnector
from lories.core.configs.parameters import Parameter
from lories.typing import Configurations, Resource, Resources

_AVAILABLE = True
_IMPORT_ERROR = None

try:
    import cv2
except ImportError as _e:
    _AVAILABLE = False
    _IMPORT_ERROR = f"Missing dependency: opencv-python — pip install opencv-python ({_e})"
    cv2 = None  # type: ignore


@register_connector_type("opencv")
class OpenCV(CameraConnector):
    """
    OpenCV-based camera connector that captures frames from RTSP streams using the FFmpeg backend.
    It connects to IP cameras via RTSP with TCP transport, grabs single frames on demand, and encodes
    them as JPEG. The connector manages connection lifecycle per read cycle to avoid stale frame buffers.
    Performance depends on network latency and camera firmware; some cameras may require adjusted timeouts
    or stream paths.
    """

    __available__ = _AVAILABLE
    __import_error__ = _IMPORT_ERROR

    _host = Parameter(key="host", type=str, desc="Camera hostname or IP")
    _port = Parameter(key="port", type=int, default=554, min=1, max=65535, desc="RTSP port")
    _username = Parameter(key="username", type=str, desc="Camera username")
    _password = Parameter(key="password", type=str, desc="Camera password")

    _host: str
    _port: int
    _username: str
    _password: str

    _captures: Dict[str, cv2.VideoCapture]

    PREVIEW_MAIN: str = "Preview_01_main"
    PREVIEW_SUB: str = "Preview_01_sub"

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

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
            address = resource.get("address", default=OpenCV.PREVIEW_SUB if streaming else OpenCV.PREVIEW_MAIN)

            if address not in self._captures:
                # TODO: Make timeouts configurable
                capture = cv2.VideoCapture()
                capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                capture.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 3000)
                capture.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 3000)
                self._captures[address] = capture
            else:
                capture = self._captures.get(address)
            if not capture.isOpened():
                self._connect(address, capture)
            if not streaming:
                self._disconnect(capture)

    def _connect(self, address: str, capture: cv2.VideoCapture) -> None:
        auth = f"{self._username}:{self._password}"
        address = f"{self._host}:{self._port}/{address}"
        capture.open(f"rtsp://{auth}@{address}", apiPreference=cv2.CAP_FFMPEG)

        if not capture.isOpened():
            raise ConnectionError(self, f"Cannot open RTSP stream: 'rtsp://#:#@{address}'")

        status = False
        for _ in range(3):  # flush stale frames
            status = capture.grab()
        if not status:
            raise ConnectionError(self, "Failed to grab frame")

        self._logger.debug(f"Opened VideoCapture to RTSP URL 'rtsp://#:#@{address}'")

    def disconnect(self) -> None:
        super().disconnect()
        for address in self._captures.keys():
            capture = self._captures.pop(address)
            self._disconnect(capture)

    def _disconnect(self, capture: cv2.VideoCapture) -> None:
        if capture.isOpened():
            capture.release()
            self._logger.debug("Released VideoCapture")

    def read_frame(self, resource: Resource) -> bytes:
        streaming = self._is_streaming(resource)

        address = resource.get("address", default=OpenCV.PREVIEW_SUB if streaming else OpenCV.PREVIEW_MAIN)
        capture = self._captures.get(address, None)
        try:
            if not streaming and not capture.isOpened():
                self._connect(address, capture)
            if capture is None or not capture.isOpened():
                raise ConnectionError(
                    self, f"Cannot open RTSP stream: 'rtsp://#:#@{self._host}:{self._port}/{address}'"
                )

            status = capture.read()
            if not status:
                raise ConnectionError(self, "Failed to grab frame")

            status, frame = capture.retrieve()
            if not status or frame is None:
                raise ConnectionError(self, "Failed to retrieve frame")

            status, buffer = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 90])
            if not status:
                raise ConnectionError(self, "Failed to encode JPEG")

            return buffer.tobytes()

        except cv2.error as e:
            raise ConnectorError(self, f"OpenCV error: {e}")
        finally:
            if not streaming:
                self._disconnect(capture)
