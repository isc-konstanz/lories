# -*- coding: utf-8 -*-
"""
lori.connectors.camera.core
~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from abc import abstractmethod

import pandas as pd
from lori.connectors import Connector


class CameraConnector(Connector):

    @abstractmethod
    def stream(self):
        pass

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("Camera connector does not support writing")
