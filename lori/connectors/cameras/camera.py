# -*- coding: utf-8 -*-
"""
lori.connectors.cameras.camera
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from abc import abstractmethod
from time import sleep, time
from typing import Iterable

import pandas as pd
from lori.connectors import ConnectionException, Connector
from lori.core import Resources


class CameraConnector(Connector):
    def read(self, resources: Resources) -> pd.DataFrame:
        timestamp = pd.Timestamp.now(tz="UTC").floor(freq="s")
        
        # TODO: Wrap read_frame() and cache latest frame to only read if frame is older than a second
        data = self.read_frame()
        return pd.DataFrame(data=[data] * len(resources), index=[timestamp], columns=list(resources.ids))

    @abstractmethod
    def read_frame(self) -> bytes: ...

    def stream(self, fps: int = 30) -> Iterable[bytes]:
        while True:
            try:
                now = time()

                if self.is_connected():
                    yield self.read_frame()

                seconds = (1 / fps) - (time() - now)
                if seconds > 0:
                    sleep(seconds)

            except KeyboardInterrupt:
                pass
            except ConnectionException as e:
                self._logger.error(f"Unexpected error '{e}' while streaming")
                self.disconnect()

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("Camera connector does not support writing")
