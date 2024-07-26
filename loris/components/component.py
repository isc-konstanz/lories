# -*- coding: utf-8 -*-
"""
loris.components.component
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import abstractmethod
from typing import Any, Dict, Optional

import pandas as pd
from loris import LocalResourceException, LocalResourceUnavailableException
from loris.components import Activator
from loris.configs import ConfigurationException, Configurations, Context
from loris.data import DataAccess
from loris.util import get_context, parse_id, parse_name, to_date


class Component(Activator):
    _uuid: str
    _id: str
    _name: str

    __data: DataAccess

    # noinspection PyProtectedMember
    def __init__(self, context: Component | Context, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(get_context(context, Context), configs, *args, **kwargs)
        from loris.data.context import DataContext
        from loris.components.context import ComponentContext

        if context is None or not isinstance(context, (Component, ComponentContext, DataContext)):
            raise ComponentException(f"Invalid component context: {None if context is None else type(context)}")
        if configs is None:
            raise ConfigurationException("Invalid component for empty configuration")

        if "id" in configs:
            self._id = parse_id(configs["id"])
            self._name = configs.get("name", default=parse_name(configs["id"]))
        elif "name" in configs:
            self._id = parse_id(configs["name"])
            self._name = configs["name"]
        else:
            raise ConfigurationException("Invalid configuration, missing specified component ID")

        self._uuid = self._id if not isinstance(context, Activator) else f"{context.uuid}.{self._id}"
        self.__data = DataAccess(self, get_context(context, DataContext))

    # noinspection PyProtectedMember
    def _on_configure(self, configs: Configurations) -> None:
        super()._on_configure(configs)
        self.__data._do_configure()

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        vars = super()._get_vars()
        vars.pop("type", None)
        return vars

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def configs(self) -> Configurations:
        return super().configs

    @property
    def context(self) -> Context:
        return super().context

    @property
    def data(self):
        return self.__data

    @property
    @abstractmethod
    def type(self) -> str:
        pass

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


class ComponentException(LocalResourceException):
    """
    Raise if an error occurred accessing the component.

    """


class ComponentUnavailableException(LocalResourceUnavailableException, ComponentException):
    """
    Raise if an accessed component can not be found.

    """
