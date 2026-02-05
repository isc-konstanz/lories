# -*- coding: utf-8 -*-
"""
lories.connectors.cameras._camera
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from abc import abstractmethod

import pandas as pd
from lories.connectors import Connector


class _CameraConnector(Connector):
    TYPE: str = "camera"

    SIZE = 33_177_600

    @abstractmethod
    def is_streaming(self) -> bool: ...

    @abstractmethod
    def read_frame(self) -> bytes: ...

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("Camera connector does not support writing")
