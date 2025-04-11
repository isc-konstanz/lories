# -*- coding: utf-8 -*-
"""
lori.connectors.camera.opencv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from typing import Optional

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

        # Validate configuration parameters
        if not all([self._host, self._port, username, password]):
            raise ValueError("Camera configuration requires 'host', 'port', 'username' and 'password'")

        self._url = f"rtsp://{username}:{password}@{self._host}:{self._port}"
        self._logger.info(f"Setup OpenCV connection to RTSP URL 'rtsp://#:#@{self._host}:{self._port}'")

    def stream(self):
        # TODO: Implement stream for visualization
        pass

    def read(self, resources: Resources) -> pd.DataFrame:
        timestamp = pd.Timestamp.now(tz="UTC").floor(freq="s")
        try:
            # Use system webcam if RTSP URL is not available
            self._capture = cv2.VideoCapture(self._url, cv2.CAP_FFMPEG)
            if not self._capture.isOpened():
                self._logger.warning("RTSP URL is not valid or not running.")
                raise ConnectionException(self, f"Unable to connect to camera 'rtsp://#:#@{self._host}:{self._port}'")

            # TODO: Make Fallback configurable
            # if not self._capture.isOpened():
            #     self._capture = cv2.VideoCapture(0)  # Default to system webcam

            max_attempts = 5
            for attempt in range(max_attempts):
                ret, frame = self._capture.read()
                if ret and frame is not None and frame.size != 0:
                    # Frame captured successfully
                    break
                else:
                    self._logger.warning(f"Attempt {attempt + 1}/{max_attempts}: Failed to capture frame.")
            else:
                # If the loop completes without a successful read
                raise ConnectionException(self, "Failed to capture a frame from the camera after multiple attempts.")

            # Encode the frame to JPEG format
            ret, buffer = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 90])
            if not ret:
                raise ConnectionException(self, "Failed to encode the frame to JPEG format.")

            # Prepare the DataFrame to return
            columns = [r.id for r in resources]
            data = [buffer.tobytes()] * len(columns)

            self._capture.release()
            self._capture = None
            return pd.DataFrame(data=[data], index=[timestamp], columns=columns)

        except ConnectionException as e:
            raise e
        except cv2.error as e:
            raise ConnectionException(self, f"OpenCV error during reading: {e}")
        except Exception as e:
            raise ConnectionException(self, f"Unexpected error during reading: {e}")

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("Camera connector does not support writing")
