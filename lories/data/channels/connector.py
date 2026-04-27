# -*- coding: utf-8 -*-
"""
lories.data.channels.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
from lories._core._connector import Connector, _Connector  # noqa
from lories._core._database import _Database  # noqa
from lories.core.errors import ResourceError
from lories.data.channels._core import _ChannelWrapper


class ChannelConnector(_ChannelWrapper[Connector]):
    timestamp: pd.Timestamp = pd.NaT

    @classmethod
    def _assert_registrator(cls, connector) -> Optional[Connector]:
        if connector is None:
            return None
        if not isinstance(connector, _Connector):
            raise ResourceError(f"Invalid connector: {None if connector is None else type(connector)}")
        return connector

    @property
    def _connector(self) -> Optional[Connector]:
        return self._get_registrator()

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        return {
            _Connector.TYPE: self._connector,
            **self.__configs,
            "timestamp": self.timestamp,
            "enabled": self.enabled,
        }

    def _get_attrs(self) -> List[str]:
        return [_Connector.TYPE, *self._copy_configs().keys(), "timestamp", "enabled"]

    def is_connected(self) -> bool:
        return self._connector.is_connected() if self.enabled else False

    def is_database(self) -> bool:
        return self._is_database(self._connector) if self.enabled else False

    @staticmethod
    def _is_database(connector: Connector) -> bool:
        return isinstance(connector, _Database)
