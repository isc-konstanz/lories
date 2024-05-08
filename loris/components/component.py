# -*- coding: utf-8 -*-
"""
    loris.components.component
    ~~~~~~~~~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional

import os
import pandas as pd
import datetime as dt
import logging

from shutil import copytree, ignore_patterns
from loris import Settings, Configurable, Configurations, ConfigurationException
from loris.data import DataAccess
from loris.util import parse_id, to_date

logger = logging.getLogger(__name__)


class Component(ABC, Configurable):

    _uuid: str

    _id: str
    _name: str

    _active: bool = False

    # noinspection PyShadowingBuiltins
    @classmethod
    def copy(cls, settings: Settings) -> bool:
        configs = Configurations(f"{cls.__name__.lower()}.conf", **settings.dirs.encode())
        if 'id' in configs:
            id = parse_id(configs['id'])
        elif 'name' in configs:
            id = parse_id(configs['name'])
            configs['id'] = id
        else:
            raise ValueError("Invalid configuration, missing specified system name")

        configs.dirs._data = os.path.join(configs.dirs.data, id)

        if os.path.isdir(configs.dirs.data):
            return False
        os.makedirs(configs.dirs.data, exist_ok=True)

        copytree(settings.dirs.conf,
                 configs.dirs.conf,
                 ignore=ignore_patterns('*.default.conf',
                                        'evaluations*',
                                        'results*',
                                        'settings*',
                                        'logging*'))
        return True

    # noinspection PyProtectedMember
    def __init__(self, context, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        if 'id' in configs:
            self._id = parse_id(configs.get('id'))
            self._name = configs.get('name', default=configs.get('id'))
        elif 'name' in configs:
            self._id = parse_id(configs['id'] if 'id' in configs else configs['name'])
            self._name = configs['name']
        else:
            raise ConfigurationException('Invalid configuration, missing specified component ID')

        self._uuid = self._id if not isinstance(context, Component) else f"{context._uuid}.{configs['id']}"
        self._context = context

        self.data = DataAccess(self, context._context, configs.get_section(DataAccess.SECTION, default={}))

    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)
        self.data.configure()

    def __activate__(self) -> None:
        pass

    def __deactivate__(self) -> None:
        pass

    def __enter__(self) -> Component:
        self.activate()
        return self

    # noinspection PyShadowingBuiltins
    def __exit__(self, type, value, traceback):
        self.deactivate()

    def __repr__(self) -> str:
        return super().__repr__() + f"\tactive = {self.is_active()}\n"

    @property
    def id(self) -> str:
        return self._id

    # @id.setter
    # def id(self, s: str) -> None:
    #     self._id = re.sub('[^A-Za-z0-9_]+', '', s.translate({ord(c): "_" for c in INVALID_CHARS}))

    @property
    def name(self):
        return self._name

    # @name.setter
    # def name(self, s: str) -> None:
    #     if s is None:
    #         self._name = s
    #     else:
    #         self._name = re.sub('[^0-9A-Za-zäöüÄÖÜß%&;:()\- ]+', '', s)

    @abstractmethod
    def get_type(self) -> str:
        pass

    # noinspection PyShadowingBuiltins
    def get(self,
            start:  Optional[pd.Timestamp, dt.datetime, str] = None,
            end:    Optional[pd.Timestamp, dt.datetime, str] = None,
            **kwargs) -> pd.DataFrame:

        data = self.data.to_frame()
        if start is not None:
            start = to_date(start, **kwargs)
            data = data[data.index >= start]
        if start is not None:
            end = to_date(end, **kwargs)
            data = data[data.index <= end]
        return data

    def is_active(self) -> bool:
        return self._active

    def activate(self) -> None:
        logger.info(f"Activating {type(self).__name__}: {self.name}")
        # for component in self._class_objects(Component):
        #     component.activate()
        self.__activate__()
        self._active = True

    def deactivate(self) -> None:
        logger.info(f"Deactivating {type(self).__name__}: {self.name}")
        # for component in self._class_objects(Component):
        #     component.deactivate()
        self.__deactivate__()
        self._active = False
