# -*- coding: utf-8 -*-
"""
lories.connectors.cameras.opencv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

import os
from typing import Dict, Optional

import cv2

from lories.connectors import ConnectionError, ConnectorError, register_connector_type
from lories.connectors.cameras import CameraConnector
from lories.typing import Configurations, Resource, Resources


@register_connector_type("opencv")
class OpenCV(CameraConnector):
    PREVIEW_MAIN: str = "Preview_01_main"
    PREVIEW_SUB: str = "Preview_01_sub"

    _host: str
    _port: int

    _username: Optional[str]
    _password: Optional[str]

    _captures: Dict[str, cv2.VideoCapture]

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

            if streaming:
                # FFmpeg's RTSP demuxer keeps an internal FIFO. CAP_PROP_BUFFERSIZE
                # is ignored by this backend, so we drain queued frames with cheap
                # grab() calls (no decode) and only retrieve() the most recent one.
                # Without this, the stream lags by up to seconds of buffered frames.
                grabbed = False
                for _ in range(64):
                    if not capture.grab():
                        break
                    grabbed = True
                if not grabbed:
                    raise ConnectionError(self, "Failed to grab frame")
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
