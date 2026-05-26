# -*- coding: utf-8 -*-
"""
lories.connectors.cameras.opencv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

import os
import time
from typing import Any, Dict, Optional

import cv2

from lories.connectors import ConnectionError, ConnectorError, register_connector_type
from lories.connectors.cameras import CameraConnector
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

    PREVIEW_MAIN: str = "Preview_01_main"
    PREVIEW_SUB: str = "Preview_01_sub"

    _host: str
    _port: int

    _username: Optional[str]
    _password: Optional[str]

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

        self._host = configs.get("host")
        self._port = configs.get_int("port", default=554)

        self._username = configs.get("username", default=None)
        self._password = configs.get("password", default=None)

        if not self._host or not self._port:
            raise ValueError("Camera configuration requires 'host' and 'port'")

        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
            "rtsp_transport;tcp|"  # use TCP only
            "max_delay;500000"  # 0.5 sec max internal delay
        )
        # Silence OpenCV's videoio module WARNs (the FFmpeg interrupt
        # callback prints "Stream timeout triggered after Xms" to stderr
        # whenever the RTSP demuxer stalls — expected on flaky networks,
        # and we surface real disconnects via ConnectionError anyway).
        # Scoped to videoio so cv2 calls in data.processors.motion keep
        # their core/imgproc warnings.
        os.environ["OPENCV_VIDEOIO_LOG_LEVEL"] = "ERROR"
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

        status = False
        for _ in range(3):  # flush stale frames
            status = capture.grab()
        if not status:
            raise ConnectionError(self, "Failed to grab frame")

        self._logger.debug(f"Opened VideoCapture to RTSP URL 'rtsp://#:#@{address}'")

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

        address = resource.get("address")
        capture = self._captures.get(address, None)
        try:
            if not streaming and not capture.isOpened():
                self._open_capture(address, capture)
            if capture is None or not capture.isOpened():
                raise ConnectionError(
                    self, f"Cannot open RTSP stream: 'rtsp://#:#@{self._host}:{self._port}/{address}'"
                )

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

            status, buffer = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 90])
            if not status:
                raise ConnectionError(self, "Failed to encode JPEG")

            return buffer.tobytes()

        except cv2.error as e:
            raise ConnectorError(self, f"OpenCV error: {e}")
        finally:
            if not streaming:
                self._disconnect(capture)
