# -*- coding: utf-8 -*-
"""
loris.data.access
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any

from loris.core import Configurations, Configurator
from loris.data import Channel, DataContext
from loris.util import get_context


# noinspection PyProtectedMember
class DataAccess(DataContext, Configurator):
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

    @property
    def configs(self) -> Configurations:
        return self.__component.configs.get_section(self.SECTION, defaults={})

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        defaults = self._parse_defaults(configs)
        if configs.has_section("channels"):
            self._load_sections(self.__component, configs.get_section("channels"), defaults)
        self._load_from_file(self.__component, configs.dirs, defaults=defaults)

    # noinspection PyUnresolvedReferences
    def add(self, key: str, **configs: Any) -> None:
        data_configs = self.configs
        if not data_configs.has_section("channels"):
            data_configs._add_section("channels", {})
        if not data_configs["channels"].has_section(key):
            data_configs["channels"]._add_section(key, configs)
        else:
            data_configs["channels"][key].update(configs, replace=False)

        if self.is_configured():
            channel_configs = self._parse_defaults(data_configs["channels"])
            # Be wary of the order. First, update the channel core with the default core
            # of the configuration file, then update the function arguments. Last, override
            # everything with the channel specific configurations of the file.
            channel_configs.update(configs)
            channel_configs.update(data_configs["channels"][key])
            channel_id = f"{self.__component.id}.{key}"
            self._update(id=channel_id, key=key, **channel_configs)

    def _add(self, channel: Channel) -> None:
        self.context._add(channel)
        self._set(channel.key, channel)

    # noinspection PyShadowingBuiltins
    def _new(self, id: str, key: str, **configs: Any) -> Channel:
        return self.context._new(id=id, key=key, **configs)
