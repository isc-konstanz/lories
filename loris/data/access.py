# -*- coding: utf-8 -*-
"""
loris.data.access
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from typing import Any, Optional

import pandas as pd
from loris.core import Configurations, Configurator
from loris.data import Channel, DataContext
from loris.util import get_context


# noinspection PyProtectedMember
class DataAccess(DataContext, Configurator):
    _created: bool = False

    def __init__(self, component, **channels: Channel) -> None:
        from loris import Component, ComponentException
        from loris.data.context import DataContext

        super().__init__(channels, get_context(component.context, DataContext))
        if component is None or not isinstance(component, Component):
            raise ComponentException(f"Invalid component: {None if component is None else type(component)}")
        self.__component = component

    # noinspection PyArgumentList
    def __getattr__(self, attr):
        channels = DataContext.__getattribute__(self, "_channels")
        if attr in channels.keys():
            return channels[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no channel '{attr}'")

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        if not configs.has_section("channels"):
            configs._add_section("channels", {})

    def create(self) -> None:
        defaults = self._build_defaults(self.configs)
        if self.configs.has_section("channels"):
            self._load_sections(self.__component, self.configs.get_section("channels"), defaults)
        self._load_from_file(self.__component, self.configs.dirs, defaults=defaults)
        self._created = True

    def is_created(self) -> bool:
        return self._created

    # noinspection PyUnresolvedReferences
    def add(self, key: str, **configs: Any) -> None:
        configs = self._build_configs(configs)
        if not self.configs["channels"].has_section(key):
            self.configs["channels"]._add_section(key, configs)
        else:
            channel_configs = self._build_configs(self.configs["channels"][key])
            channel_configs = self._update_configs(channel_configs, configs, replace=False)
            self.configs["channels"][key] = channel_configs

        if self.is_created():
            channel_configs = self._build_configs(self._build_defaults(self.configs["channels"]))
            # Be wary of the order. First, update the channel core with the default core
            # of the configuration file, then update the function arguments. Last, override
            # everything with the channel specific configurations of the file.
            channel_configs = self._update_configs(channel_configs, configs)
            channel_configs = self._update_configs(channel_configs, self.configs["channels"][key])
            channel_id = f"{self.__component.id}.{key}"
            self._update(id=channel_id, key=key, **channel_configs)

    def _add(self, channel: Channel) -> None:
        self.context._add(channel)
        self._set(channel.key, channel)

    # noinspection PyShadowingBuiltins
    def _new(self, id: str, key: str, **configs: Any) -> Channel:
        return self.context._new(id=id, key=key, **configs)

    # noinspection PyShadowingBuiltins
    def _update(self, id: str, key: str, **configs: Any) -> Channel:
        if key in self:
            # TODO: Take popped configs into account
            channel = self._get(key)
            channel.configs.update(configs)
        else:
            channel = self._new(id=id, key=key, **configs)
        self._add(channel)
        return channel

    def _remove(self, key: str) -> None:
        channel = self._get(key)
        self.context.remove(channel)
        del self._channels[key]

    # noinspection PyShadowingBuiltins
    def read(
        self,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> pd.DataFrame:
        return self.context.read(self.channels, start, end)

    # noinspection PyShadowingBuiltins
    def write(self, data: pd.DataFrame) -> None:
        self.context.write(data, self.channels)
