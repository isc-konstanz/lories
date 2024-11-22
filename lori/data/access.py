# -*- coding: utf-8 -*-
"""
lori.data.access
~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from typing import Any, Callable, Literal, Optional

import pandas as pd
from lori.core import Configurations, Configurator, Context, ResourceException
from lori.data import Channel, DataContext
from lori.util import get_context, update_recursive


# noinspection PyProtectedMember
class DataAccess(DataContext, Configurator):
    _created: bool = False

    def __init__(self, component) -> None:
        self.__component = self._assert_component(component)
        self.__context = self._assert_context(component.context)
        super().__init__(component.configs.get_section(self.SECTION, ensure_exists=True))

    @classmethod
    def _assert_component(cls, component):
        from lori.components import Component

        if component is None or not isinstance(component, (Component, Context)):
            raise ResourceException(f"Invalid '{cls.__name__}' component: {type(component)}")
        return component

    @classmethod
    def _assert_context(cls, context: Context) -> Context:
        from lori.components import Component
        from lori.data.manager import DataManager

        if context is None or not isinstance(context, (Component, Context)):
            raise ResourceException(f"Invalid '{cls.__name__}' context: {type(context)}")
        return get_context(context, DataManager)

    @classmethod
    def _assert_configs(cls, configs: Configurations) -> Configurations:
        configs = super()._assert_configs(configs)
        if configs is None:
            raise ResourceException(f"Invalid '{cls.__name__}' NoneType configurations")
        if not configs.has_section("channels"):
            configs._add_section("channels", {})
        return configs

    # noinspection PyArgumentList
    def __contains__(self, __channel: str | Channel) -> bool:
        channels = Context.__getattribute__(self, f"_{Context.__name__}__map")
        if isinstance(__channel, str):
            if not len(__channel.split(".")) > 1:
                __channel = f"{self.__component.id}.{__channel}"
            return __channel in channels.keys()
        if isinstance(__channel, Channel):
            return __channel in channels.values()
        return False

    # noinspection PyArgumentList
    def __getattr__(self, attr):
        channels = Context.__getattribute__(self, f"_{Context.__name__}__map")
        channels_by_key = {c.key: c for c in channels.values()}
        if attr in channels_by_key:
            return channels_by_key[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no channel '{attr}'")

    def __delitem__(self, __uid: str) -> None:
        if not len(__uid.split(".")) > 1:
            __uid = f"{self.__component.id}.{__uid}"
        del self.__context[__uid]
        del self[__uid]

    @property
    def context(self) -> DataContext:
        return self.__context

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

    # noinspection PyShadowingBuiltins
    def _set(self, id: str, channel: Channel) -> None:
        self.__context._set(id, channel)
        super()._set(id, channel)

    # noinspection PyShadowingBuiltins
    def _new(self, id: str, key: str, **configs: Any) -> Channel:
        return self.__context._new(id=id, key=key, **configs)

    # noinspection PyShadowingBuiltins
    def _get(self, id: str) -> Channel:
        if not len(id.split(".")) > 1:
            id = f"{self.__component.id}.{id}"
        return super()._get(id)

    def register(
        self,
        function: Callable[[pd.DataFrame], None],
        *channels: Channel | str,
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> None:
        _channels = []
        for channel in channels:
            if isinstance(channel, str):
                if channel in self:
                    channel = self[channel]
            elif not isinstance(channel, Channel):
                raise ResourceException(f"Unable to register to '{type(channel)}' channel: {channel}")
            _channels.append(channel)
        self.__context.register(function, *_channels, how=how, unique=unique)

    # noinspection PyShadowingBuiltins
    def read(
        self,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> pd.DataFrame:
        return self.__context.read(self.channels, start, end)

    # noinspection PyShadowingBuiltins
    def write(self, data: pd.DataFrame) -> None:
        self.__context.write(data, self.channels)
