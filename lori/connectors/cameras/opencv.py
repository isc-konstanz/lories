# -*- coding: utf-8 -*-
"""
lori.connectors.cameras.opencv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

import os

import cv2

from lori.connectors import ConnectionException, ConnectorException, register_connector_type
from lori.connectors.cameras import CameraConnector
from lori.core import Configurations, Resources


@register_connector_type("opencv")
class OpenCV(CameraConnector):
    _capture: cv2.VideoCapture
    _host: str
    _port: int

    _username: str
    _password: str

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self._host = configs.get("host")
        self._port = configs.get_int("port", default=554)

        self._username = configs.get("username")
        self._password = configs.get("password")

        if not all([self._host, self._port, self._username, self._password]):
            raise ValueError("Camera configuration requires 'host', 'port', 'username' and 'password'")

        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
            "rtsp_transport;tcp|"  # use TCP only
            "max_delay;500000"  # 0.5 sec max internal delay
        )

        # TODO: Make timeouts configurable
        self._capture = cv2.VideoCapture()
        self._capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        self._capture.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 3000)
        self._capture.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 3000)

    def is_connected(self) -> bool:
        return self._capture.isOpened()

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        auth = f"{self._username}:{self._password}"
        address = f"{self._host}:{self._port}/Preview_01_main"

        self._capture.open(f"rtsp://{auth}@{address}", apiPreference=cv2.CAP_FFMPEG)
        if not self._capture.isOpened():
            raise ConnectionException(self, f"Cannot open RTSP stream: 'rtsp://#:#@{address}'")

        success = False
        for _ in range(1, 3):  # flush stale frames
            success = self._capture.grab()
        if not success:
            raise ConnectionException(self, "Failed to grab frame")

        self._logger.info(f"Opened VideoCapture to RTSP URL 'rtsp://#:#@{address}'")

    def disconnect(self) -> None:
        super().disconnect()
        self._capture.release()
        self._logger.debug("Released VideoCapture")

    def read_frame(self) -> cv2.typing.MatLike:
        try:
            success = self._capture.grab()
            if not success:
                raise ConnectionException(self, "Failed to grab frame")

            success, frame = self._capture.retrieve()
            if not success or frame is None:
                raise ConnectionException(self, "Failed to retrieve frame")

            success, buffer = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 90])
            if not success:
                raise ConnectionException(self, "Failed to encode JPEG")

            return buffer.tobytes()

        except cv2.error as e:
            raise ConnectorException(self, f"OpenCV error: {e}")
