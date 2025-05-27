# -*- coding: utf-8 -*-
"""
lori.connectors.entsoe.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Tuple
import numpy as np
import pandas as pd
#TODO: add to requirements
from entsoe import EntsoePandasClient

from lori.connectors import ConnectionException, Connector
from lori.core import ConfigurationException, Configurations, Resources
from lori.typing import TimestampType


# noinspection PyShadowingBuiltins
class EntsoeConnector(Connector):
    api_key: str

    _client: Optional[EntsoePandasClient] = None

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self.api_key = configs.get("api_key")
        if self.api_key is None:
            raise ConfigurationException("Missing security token")

    def connect(self, resources: Resources) -> None:
        self._client = EntsoePandasClient(api_key=self.api_key)

    def disconnect(self) -> None:
        self._client = None

    def is_connected(self) -> bool:
        return self._client is not None

    @abstractmethod
    def read(
        self,
        resources: Resources,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> pd.DataFrame:
        pass

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("EntsoeConnector does not support writing data")


