# -*- coding: utf-8 -*-
"""
lori.components.connector
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from functools import wraps
from typing import Any, Dict, List, Optional

import pandas as pd
from lori.connectors import ConnectorAccess
from lori.converters import ConverterAccess
from lori.core import Activator, Context, Registrator, ResourceException, ResourceUnavailableException
from lori.core.configs import ConfigurationException, Configurations
from lori.data import DataAccess
from lori.util import to_date


# noinspection PyAbstractClass
class Component(Registrator, Activator):
    SECTION: str = "component"
    INCLUDES: List[str] = [ConverterAccess.SECTION, ConnectorAccess.SECTION, DataAccess.SECTION]

    __converters: ConverterAccess
    __connectors: ConnectorAccess
    __data: DataAccess

    def __init__(
        self,
        context: Component | Context,
        configs: Optional[Configurations] = None,
        **kwargs,
    ) -> None:
        super().__init__(context=context, configs=configs, **kwargs)
        self.__converters = ConverterAccess(self)
        self.__connectors = ConnectorAccess(self)
        self.__data = DataAccess(self)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

    @wraps(configure, updated=())
    def _do_configure(self, configs: Configurations, *args, **kwargs) -> None:
        if configs is None:
            raise ConfigurationException(f"Invalid NoneType configuration for {type(self).__name__}: {self.name}")
        if not configs.enabled:
            raise ConfigurationException(f"Trying to configure disabled {type(self).__name__}: {configs.name}")

        self.__converters.configure(configs.get_sections([ConverterAccess.SECTION], ensure_exists=True))
        self.__connectors.configure(configs.get_sections([ConnectorAccess.SECTION], ensure_exists=True))
        self.__data.configure(configs.get_section(DataAccess.SECTION, ensure_exists=True))
        super()._do_configure(configs, *args, **kwargs)

        self.__data.create()

    @property
    def converters(self):
        return self.__converters

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

    # noinspection PyArgumentList
    def __init__(self, component: Component, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.component = component


class ComponentUnavailableException(ResourceUnavailableException, ComponentException):
    """
    Raise if an accessed connector can not be found.

    """
