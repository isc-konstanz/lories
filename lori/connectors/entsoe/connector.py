# -*- coding: utf-8 -*-
"""
lori.connector.price.dwd.brightsky
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import abstractmethod
import json
import re
import xml.etree.ElementTree as ET
from abc import ABC
from typing import Optional, Tuple

import xmltodict
import isodate

import requests
from entsoe import EntsoePandasClient


import numpy as np
import pandas as pd
# from lori import ConfigurationException, Configurations, Resources, Tariff
from lori.core import ConfigurationException, Configurations, Resources
from lori.connectors import ConnectionException, Connector, register_connector_type


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
        self._logger.debug(f"Connecting to Entsoe")
        self._client = EntsoePandasClient(api_key=self.api_key)

    def disconnect(self) -> None:
        self._client = None

    def is_connected(self) -> bool:
        return self._client is not None

    @abstractmethod
    def read(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> pd.DataFrame:
        pass

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("EntsoeConnector does not support writing data")


