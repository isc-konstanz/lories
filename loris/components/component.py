# -*- coding: utf-8 -*-
"""
loris.components.component
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import os
from abc import abstractmethod
from shutil import copytree, ignore_patterns
from typing import Collection, Optional

import pandas as pd
from loris import LocalResourceException, LocalResourceUnavailableException, Settings
from loris.components import Activator
from loris.configs import ConfigurationException, Configurations, Configurator
from loris.data import DataAccess
from loris.util import parse_id, to_date


class Component(Activator):
    _uuid: str
    _id: str
    _name: str

    __data: DataAccess

    # noinspection PyShadowingBuiltins
    @classmethod
    def copy(cls, settings: Settings) -> bool:
        configs = Configurations(f"{cls.__name__.lower()}.conf", **settings.dirs.encode())
        if "id" in configs:
            id = parse_id(configs["id"])
        elif "name" in configs:
            id = parse_id(configs["name"])
            configs["id"] = id
        else:
            raise ConfigurationException("Invalid configuration, missing specified system name")

        configs.dirs._data = os.path.join(configs.dirs.data, id)

        if os.path.isdir(configs.dirs.data):
            return False
        os.makedirs(configs.dirs.data, exist_ok=True)

        copytree(
            settings.dirs.conf,
            configs.dirs.conf,
            ignore=ignore_patterns(
                "*.default.conf",
                "evaluations*",
                "results*",
                "settings*",
                "logging*"
            ),
        )
        return True

    # noinspection PyProtectedMember
    def __init__(self, context, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        if "id" in configs:
            self._id = parse_id(configs.get("id"))
            self._name = configs.get("name", default=configs.get("id"))
        elif "name" in configs:
            self._id = parse_id(configs["id"] if "id" in configs else configs["name"])
            self._name = configs["name"]
        else:
            raise ConfigurationException("Invalid configuration, missing specified component ID")

        self._uuid = self._id if not isinstance(context, Component) else f"{context.uuid}.{configs['id']}"

        def _get_data_context():
            from loris.data.context import DataContext

            _data_context = context
            while not isinstance(_data_context, DataContext):
                try:
                    _data_context = _data_context.context
                except AttributeError:
                    return ComponentException(f"Invalid component context type: {type(context)}")
            return _data_context

        self.__context = context
        self.__data = DataAccess(self, _get_data_context(), configs.get_section(DataAccess.SECTION, defaults={}))

    def _do_configure_members(self, configurators: Collection[Configurator]) -> None:
        super()._do_configure_members(configurators)
        # Update DataAccess configurations before configuring it manually, as private members are not configured
        # This ensures up-to-date channel configurations when data is configured.
        self.__data.configs.update(self.configs.get_section(DataAccess.SECTION, defaults={}), replace=False)
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
