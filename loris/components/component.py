# -*- coding: utf-8 -*-
"""
loris.components.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from typing import Any, Collection, Dict, Optional

import pandas as pd
from loris.connectors import ConnectorAccess
from loris.core import Activator, Configurations, Context, ResourceException, ResourceUnavailableException
from loris.data import DataAccess
from loris.util import to_date


# noinspection PyAbstractClass, PyProtectedMember
class Component(Activator):
    SECTION: str = "component"
    SECTIONS: Collection[str] = [DataAccess.SECTION, ConnectorAccess.SECTION]

    __connectors: ConnectorAccess

    __data: DataAccess

    def __init__(self, context: Component | Context, configs: Configurations, *args, **kwargs) -> None:
        if context is None:
            raise ComponentException(f"Invalid context: {context}")
        super().__init__(context, configs, *args, **kwargs)
        self.__connectors = ConnectorAccess(self)
        self.__data = DataAccess(self)

    def _on_configure(self, configs: Configurations) -> None:
        super()._on_configure(configs)
        self.__connectors._do_configure()
        self.__data._do_configure()

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        vars = super()._get_vars()
        vars.pop("type", None)
        return vars

    @property
    def configs(self) -> Configurations:
        return super().configs

    @property
    def context(self) -> Context:
        return super().context

    @property
    def connectors(self):
        return self.__connectors

    @property
    def data(self):
        return self.__data

    def get(
        self,
        start: Optional[pd.Timestamp, dt.datetime, str] = None,
        end: Optional[pd.Timestamp, dt.datetime, str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return self._get_range(self.__data.to_frame(), start, end, **kwargs)

    @staticmethod
    def _get_range(
        data: pd.DataFrame,
        start: Optional[pd.Timestamp, dt.datetime, str] = None,
        end: Optional[pd.Timestamp, dt.datetime, str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        if data.empty:
            return data
        if start is not None:
            start = to_date(start, **kwargs)
            data = data[data.index >= start]
        if end is not None:
            end = to_date(end, **kwargs)
            data = data[data.index <= end]
        return data


class ComponentException(ResourceException):
    """
    Raise if an error occurred accessing the connector.

    """


class ComponentUnavailableException(ResourceUnavailableException, ComponentException):
    """
    Raise if an accessed connector can not be found.

    """
