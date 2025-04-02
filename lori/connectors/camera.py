# -*- coding: utf-8 -*-
"""
lori.connectors.camera
~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

import cv2
import pandas as pd
from typing import Optional
from lori.connectors import Connector, ConnectionException, register_connector_type
from lori.core import Configurations, Resources


@register_connector_type("opencv")
class Camera(Connector):
    capture: Optional[cv2.VideoCapture] = None
    rtsp_url: str

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        host = configs.get("host")
        username = configs.get("username")
        password = configs.get("password")
        port = configs.get("port")

        # Validate configuration parameters
        if not all([host, username, password, port]):
            raise ValueError("Camera configuration requires 'host', 'username', 'password', and 'port'")

        self.rtsp_url = f"rtsp://{username}:{password}@{host}:{port}"

    def connect(self, resources: Resources) -> None:
        super().connect(resources)

    def disconnect(self) -> None:
        super().disconnect()

    def read(self, resources: Resources) -> pd.DataFrame:
        try:
            # Use system webcam if RTSP URL is not available
            self.capture = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
            if not self.capture.isOpened():
                print("RTSP URL is not valid or not running. Falling back to system webcam.")
                self.capture = cv2.VideoCapture(0)  # Default to system webcam

            if not self.capture.isOpened():
                raise ConnectionException(self, "Unable to connect to the camera.")

            max_attempts = 5
            for attempt in range(max_attempts):
                ret, frame = self.capture.read()
                if ret and frame is not None and frame.size != 0:
                    # Frame captured successfully
                    break
                else:
                    print(f"Attempt {attempt + 1}/{max_attempts}: Failed to capture frame.")
            else:
                # If the loop completes without a successful read
                raise ConnectionException(
                    self,
                    "Failed to capture a frame from the camera after multiple attempts."
                )

            # Encode the frame to JPEG format
            ret, buffer = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 90])
            if not ret:
                raise ConnectionException(self, "Failed to encode the frame to JPEG format.")

            # Prepare the DataFrame to return
            data = [buffer.tobytes()]
            index = [pd.Timestamp.now(tz="UTC").floor(freq="s")]
            columns = [r.id for r in resources]

            self.capture.release()
            self.capture = None
            return pd.DataFrame(data=[data], index=index, columns=columns)

        except cv2.error as e:
            raise ConnectionException(self, f"OpenCV error during reading: {e}")
        except Exception as e:
            raise ConnectionException(self, f"Unexpected error during reading: {e}")

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("Camera connector does not support writing")
