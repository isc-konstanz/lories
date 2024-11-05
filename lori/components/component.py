# -*- coding: utf-8 -*-
"""
lori.components.connector
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from functools import wraps
from typing import Any, Collection, Dict, Optional

import pandas as pd
from lori.connectors import ConnectorAccess
from lori.core import Activator, Context, ResourceException, ResourceUnavailableException
from lori.core.configs import ConfigurationException, Configurations
from lori.data import DataAccess
from lori.util import to_date


# noinspection PyAbstractClass
class Component(Activator):
    SECTION: str = "component"
    SECTIONS: Collection[str] = [DataAccess.SECTION, ConnectorAccess.SECTION]

    __connectors: ConnectorAccess

    __data: DataAccess

    def __init__(
        self,
        context: Component | Context,
        configs: Optional[Configurations] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(context, configs, *args, **kwargs)
        self.__connectors = ConnectorAccess(self)
        self.__data = DataAccess(self)

    # noinspection PyMethodMayBeStatic
    def _assert_context(self, context: Optional[Context]) -> Optional[Context]:
        if context is None:
            raise ComponentException(f"Invalid context: {context}")
        return super()._assert_context(context)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

    @wraps(configure, updated=())
    def _do_configure(self, configs: Configurations, *args, **kwargs) -> None:
        if configs is None:
            raise ConfigurationException(f"Invalid NoneType configuration for {type(self).__name__}: {self.name}")
        if not configs.enabled:
            raise ConfigurationException(f"Trying to configure disabled {type(self).__name__}: {configs.name}")

        self.__connectors.configure(configs.get_section(ConnectorAccess.SECTION, ensure_exists=True))
        self.__data.configure(configs.get_section(DataAccess.SECTION, ensure_exists=True))
        super()._do_configure(configs, *args, **kwargs)

        self.__connectors.create()
        self.__data.create()

    # Override return type, as a context is mandatory for components
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

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        vars = super()._get_vars()
        vars.pop("type", None)
        return vars


class ComponentException(ResourceException):
    """
    Raise if an error occurred accessing the connector.

    """


class ComponentUnavailableException(ResourceUnavailableException, ComponentException):
    """
    Raise if an accessed connector can not be found.

    """
