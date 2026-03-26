# -*- coding: utf-8 -*-
"""
lories.connectors.cameras.opencv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

import os

import cv2

from lories.connectors import ConnectionError, ConnectorError, register_connector_type
from lories.connectors.cameras import CameraConnector
from lories.core.configs.parameters import Parameter
from lories.typing import Configurations, Resources


@register_connector_type("opencv")
class OpenCV(CameraConnector):
    _host = Parameter(key="host", type=str, desc="Camera hostname or IP")
    _port = Parameter(key="port", type=int, default=554, min=1, max=65535, desc="RTSP port")
    _username = Parameter(key="username", type=str, desc="Camera username")
    _password = Parameter(key="password", type=str, desc="Camera password")

    _capture: cv2.VideoCapture

    _host: str
    _port: int
    _username: str
    _password: str

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
            "rtsp_transport;tcp|"  # use TCP only
            "max_delay;500000"  # 0.5 sec max internal delay
        )

        # TODO: Make timeouts configurable
        self._capture = cv2.VideoCapture()
        self._capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        self._capture.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 3000)
        self._capture.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 3000)

    # def is_connected(self) -> bool:
    #     return self._capture.isOpened()

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        # Validate connection only to throw ConnectionError when connect is called by the manager
        self._connect()
        self._disconnect()

    def _connect(self) -> None:
        auth = f"{self._username}:{self._password}"
        address = f"{self._host}:{self._port}/Preview_01_main"

        self._capture.open(f"rtsp://{auth}@{address}", apiPreference=cv2.CAP_FFMPEG)
        if not self._capture.isOpened():
            raise ConnectionError(self, f"Cannot open RTSP stream: 'rtsp://#:#@{address}'")

        status = False
        for _ in range(3):  # flush stale frames
            status = self._capture.grab()
        if not status:
            raise ConnectionError(self, "Failed to grab frame")

        self._logger.debug(f"Opened VideoCapture to RTSP URL 'rtsp://#:#@{address}'")

    def disconnect(self) -> None:
        super().disconnect()
        self._disconnect()

    def _disconnect(self) -> None:
        self._capture.release()
        self._logger.debug("Released VideoCapture")

    def read_frame(self) -> bytes:
        try:
            self._connect()

            status = self._capture.read()
            if not status:
                raise ConnectionError(self, "Failed to grab frame")

            status, frame = self._capture.retrieve()
            if not status or frame is None:
                raise ConnectionError(self, "Failed to retrieve frame")

            status, buffer = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 90])
            if not status:
                raise ConnectionError(self, "Failed to encode JPEG")

            return buffer.tobytes()

        except cv2.error as e:
            raise ConnectorError(self, f"OpenCV error: {e}")
        finally:
            self._disconnect()
