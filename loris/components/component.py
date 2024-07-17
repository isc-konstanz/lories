# -*- coding: utf-8 -*-
"""
loris.components.component
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import abstractmethod
from typing import Collection, Optional

import pandas as pd
from loris import LocalResourceException, LocalResourceUnavailableException
from loris.components import Activator
from loris.configs import ConfigurationException, Configurations, Configurator
from loris.data import DataAccess
from loris.util import parse_id, to_date


class Component(Activator):
    _uuid: str
    _id: str
    _name: str

    __data: DataAccess

    # noinspection PyProtectedMember
    def __init__(self, context, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        if "id" in configs:
            self._id = parse_id(configs.get("id"))
            self._name = configs.get("name", default=self._id)
        elif "name" in configs:
            self._id = parse_id(configs["id"] if "id" in configs else configs["name"])
            self._name = configs["name"]
        else:
            raise ConfigurationException("Invalid configuration, missing specified component ID")

        from loris.data.context import DataContext
        from loris.components.context import ComponentContext
        if not isinstance(context, (Component, ComponentContext, DataContext)):
            raise ComponentException(f"Invalid component context type: {type(context)}")

        self._uuid = self._id if not isinstance(context, Component) else f"{context.uuid}.{self._id}"

        def _get_context(_type):
            _context = context
            while not isinstance(_context, _type):
                try:
                    _context = _context.context
                except AttributeError:
                    raise ComponentException(f"Invalid component context type: {type(context)}")
            return _context

        self.__data = DataAccess(self, _get_context(DataContext))
        self.__context = _get_context((ComponentContext, DataContext))

    # noinspection PyProtectedMember
    def _do_configure_members(self, configurators: Collection[Configurator]) -> None:
        super()._do_configure_members(configurators)
        self.__data._do_configure()

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def id(self) -> str:
        return self._id

    # @id.setter
    # def id(self, s: str) -> None:
    #     self._id = re.sub('[^A-Za-z0-9_]+', '', s.translate({ord(c): "_" for c in INVALID_CHARS}))

    @property
    def name(self) -> str:
        return self._name

    # @name.setter
    # def name(self, s: str) -> None:
    #     if s is None:
    #         self._name = s
    #     else:
    #         self._name = re.sub('[^0-9A-Za-zäöüÄÖÜß%&;:()\- ]+', '', s)

    @property
    def context(self):
        return self.__context

    @property
    def data(self):
        return self.__data

    @property
    @abstractmethod
    def type(self) -> str:
        pass

    # noinspection PyShadowingBuiltins
    def get(
        self,
        start: Optional[pd.Timestamp, dt.datetime, str] = None,
        end: Optional[pd.Timestamp, dt.datetime, str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        data = self.__data.to_frame()
        if start is not None:
            start = to_date(start, **kwargs)
            data = data[data.index >= start]
        if start is not None:
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
