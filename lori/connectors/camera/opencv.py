# -*- coding: utf-8 -*-
"""
lori.connectors.camera.opencv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from typing import Optional

import os
import cv2

import pandas as pd

from lori.connectors import ConnectionException, register_connector_type
from lori.connectors.camera import CameraConnector
from lori.core import Configurations, Resources


@register_connector_type("opencv")
class OpenCV(CameraConnector):
    _capture: Optional[cv2.VideoCapture] = None
    _host: str
    _port: int
    _url: str

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self._host = configs.get("host")
        self._port = configs.get_int("port", default=554)

        username = configs.get("username")
        password = configs.get("password")

        if not all([self._host, self._port, username, password]):
            raise ValueError("Camera configuration requires 'host', 'port', 'username' and 'password'")

        self._url = f"rtsp://{username}:{password}@{self._host}:{self._port}/Preview_01_main"
        self._logger.info(f"Setup OpenCV connection to RTSP URL 'rtsp://#:#@{self._host}:{self._port}/Preview_01_main'")

    def _open_capture(self):
        if self._capture and self._capture.isOpened():
            return

        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
            "rtsp_transport;tcp|"  # use TCP only
            "max_delay;500000"  # 0.5 sec max internal delay
        )
        cap = cv2.VideoCapture(self._url, apiPreference=cv2.CAP_FFMPEG)
        if not cap.isOpened():
            raise ConnectionException(
                self,
                f"Cannot open RTSP stream: 'rtsp://#:#@{self._host}:{self._port}/Preview_01_main'"
            )

        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 2000)
        cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 2000)

        self._capture = cap
        for _ in range(2):  # flush stale frames
            self._capture.grab()

        self._logger.debug(f"VideoCapture opened.")

    def _close_capture(self):
        if self._capture:
            self._capture.release()
            self._capture = None
            self._logger.debug(f"VideoCapture released.")

    def stream(self):
        try:
            self._open_capture()

            while True:
                if not self._capture.grab():
                    self._logger.warning(f"Frame grab failed, reconnecting...")
                    self._close_capture()
                    self._open_capture()
                    continue

                ret, frame = self._capture.retrieve()
                if not ret or frame is None:
                    self._logger.warning(f"Frame retrieve failed, skipping.")
                    continue

                cv2.imshow("RTSP Live", frame)
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    break

        except ConnectionException as e:
            self._logger.error(f"{e}")
        finally:
            self._close_capture()
            cv2.destroyAllWindows()

    def read(self, resources: Resources) -> pd.DataFrame:
        timestamp = pd.Timestamp.now(tz="UTC").floor(freq="s")
        try:
            self._open_capture()

            if not self._capture.grab():
                raise ConnectionException(self, f"Failed to grab frame.")

            ret, frame = self._capture.retrieve()
            if not ret or frame is None:
                raise ConnectionException(self, "Failed to retrieve frame.")

            ok, buffer = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 90])
            if not ok:
                raise ConnectionException(self, "JPEG encoding failed.")

            columns = [r.id for r in resources]
            data = [buffer.tobytes()] * len(columns)
            return pd.DataFrame(data=[data], index=[timestamp], columns=columns)

        except cv2.error as e:
            raise ConnectionException(self, f"OpenCV error: {e}")
        except Exception as e:
            raise ConnectionException(self, f"Unexpected error: {e}")
        finally:
            self._close_capture()

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("Camera connector does not support writing")
