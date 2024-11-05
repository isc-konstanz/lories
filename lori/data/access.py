# -*- coding: utf-8 -*-
"""
lori.data.access
~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from typing import Any, Optional

import pandas as pd
from lori.core import Configurations, Configurator
from lori.data import Channel, DataContext
from lori.util import get_context, update_recursive


# noinspection PyProtectedMember
class DataAccess(DataContext, Configurator):
    _created: bool = False

    def __init__(self, component, **channels: Channel) -> None:
        from lori import Component, ComponentException
        from lori.data.context import DataContext

        context = get_context(component.context, DataContext)
        configs = component.configs.get_section(self.SECTION, ensure_exists=True)
        if not configs.has_section("channels"):
            configs._add_section("channels", {})
        super().__init__(channels, context, configs)
        if component is None or not isinstance(component, Component):
            raise ComponentException(f"Invalid component: {None if component is None else type(component)}")
        self.__component = component

    # noinspection PyArgumentList
    def __getattr__(self, attr):
        channels = DataContext.__getattribute__(self, "_channels")
        channels_by_key = {c.key: c for c in channels.values()}
        if attr in channels_by_key:
            return channels_by_key[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no channel '{attr}'")

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        if not configs.has_section("channels"):
            configs._add_section("channels", {})

    def create(self) -> None:
        defaults = Channel._build_defaults(self.configs)
        if self.configs.has_section("channels"):
            self._load_sections(self.__component, self.configs.get_section("channels"), defaults)
        self._load_from_file(self.__component, self.configs.dirs, defaults=defaults)
        self._created = True

    def is_created(self) -> bool:
        return self._created

    # noinspection PyUnresolvedReferences
    def add(self, key: str, **configs: Any) -> None:
        configs = Channel._build_configs(configs)
        if not self.configs["channels"].has_section(key):
            self.configs["channels"]._add_section(key, configs)
        else:
            channel_configs = Channel._build_configs(self.configs["channels"][key])
            channel_configs = update_recursive(channel_configs, configs, replace=False)
            self.configs["channels"][key] = channel_configs

        if self.is_created():
            channel_defaults = Channel._build_defaults(self.configs["channels"])
            channel_configs = Channel._build_configs(channel_defaults)
            # Be wary of the order. First, update the channel core with the default core
            # of the configuration file, then update the function arguments. Last, override
            # everything with the channel specific configurations of the file.
            channel_configs = update_recursive(channel_configs, configs)
            channel_configs = update_recursive(channel_configs, self.configs["channels"][key])
            channel_id = f"{self.__component.id}.{key}"
            self._update(id=channel_id, key=key, **channel_configs)

    def _add(self, channel: Channel) -> None:
        self.context._add(channel)
        super()._add(channel)

    # noinspection PyShadowingBuiltins
    def _new(self, id: str, key: str, **configs: Any) -> Channel:
        return self.context._new(id=id, key=key, **configs)

    # noinspection PyShadowingBuiltins
    def _get(self, id: str) -> Channel:
        if not len(id.split(".")) > 1:
            id = f"{self.__component.id}.{id}"
        return super()._get(id)

    # noinspection PyShadowingBuiltins
    def _contains(self, id: str) -> bool:
        if not len(id.split(".")) > 1:
            id = f"{self.__component.id}.{id}"
        return id in self._channels.keys()

    # noinspection PyShadowingBuiltins
    def _remove(self, id: str) -> None:
        if not len(id.split(".")) > 1:
            id = f"{self.__component.id}.{id}"
        channel = self._channels.get(id)
        self.context.remove(channel)
        del self._channels[id]

    # noinspection PyShadowingBuiltins
    def read(
        self,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> pd.DataFrame:
        return self.context.read(self.channels, start, end)

    # noinspection PyShadowingBuiltins
    def write(self, data: pd.DataFrame) -> None:
        self.context.write(data, self.channels)
