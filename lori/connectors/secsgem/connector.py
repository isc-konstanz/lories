# -*- coding: utf-8 -*-
"""
lori.connectors.secsgem.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import pandas as pd
import pytz as tz
from lori.connectors import ConnectionException, Connector, register_connector_type
from lori.core import Configurations, Resources


@register_connector_type("secs_gem", "secsgem")
class SecsGemConnector(Connector):
    _host: str
    _port: int

    # noinspection SpellCheckingInspection
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._host = configs.get("host")
        self._port = configs.get_int("port")

    # def is_connected(self) -> bool:
    #     # TODO: Implement connection validation
    #     return False

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        try:
            self._logger.info(f"Connecting to SECS/GEM '{self._host}:{self._port}'")
            # TODO: Implement connection to resources and optionally instance listeners

        # TODO: Choose better exception
        except IOError as e:
            self._logger.warning(f"Error connecting to SECS/GEM '{self._host}:{self._port}': {e}")
            raise ConnectionException(self, e)

    def disconnect(self) -> None:
        super().disconnect()
        # TODO: Implement disconnecting and cleanup of resources

    # noinspection PyTypeChecker, PyShadowingBuiltins
    def read(self, resources: Resources) -> pd.DataFrame:
        timestamp = pd.Timestamp.now(tz.UTC).floor(freq="s")
        results = []
        try:
            for group, group_resources in resources.groupby("PLACEHOLDER"):
                # TODO: Implement reading
                data = []
                for resource in group_resources:
                    data.append(resource.get("PLACEHOLDER", default=-1))
                results.append(pd.DataFrame(data=[data], index=[timestamp], columns=list(group_resources.ids)))

        # TODO: Choose better exception
        except IOError as e:
            raise ConnectionException(self, e)

        if len(results) == 0:
            return pd.DataFrame()
        results = sorted(results, key=lambda d: min(d.index))
        return pd.concat(results, axis="columns")

    def write(self, data: pd.DataFrame) -> None:
        try:
            for group, group_resources in self.resources.groupby("PLACEHOLDER"):
                # TODO: Implement writing
                group_data = data.loc[:, [r.id for r in group_resources if r.id in data.columns]]
                group_data = group_data.dropna(axis="index", how="all")
                if group_data.empty:
                    continue

        except IOError as e:
            raise ConnectionException(self, e)
