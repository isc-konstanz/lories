# -*- coding: utf-8 -*-
"""
loris.data.access
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Mapping

from loris.core import Configurations, Configurator, Context
from loris.data import Channel, DataMapping
from loris.util import get_context


# noinspection PyProtectedMember
class DataAccess(DataMapping, Context[Channel], Configurator):
    SECTION: str = "data"

    def __init__(self, component, **channels: Channel) -> None:
        from loris import Component, ComponentException
        from loris.data.context import DataContext
        super().__init__(channels, get_context(component.context, DataContext))

        if component is None or not isinstance(component, Component):
            raise ComponentException(f"Invalid component: {None if component is None else type(component)}")
        self.__component = component
        if not self.__component.configs.has_section(self.SECTION):
            self.__component.configs._add_section(self.SECTION, {})

    @property
    def configs(self) -> Configurations:
        return self.__component.configs.get_section(self.SECTION)

    def configure(self, configs: Configurations) -> None:
        if configs.has_section("channels"):
            self._load_sections(configs.get_section("channels"))

    def _load_sections(self, configs: Configurations) -> None:
        channel_defaults = self._parse_defaults(configs)
        for channel_id in [i for i in configs.keys() if i not in channel_defaults]:
            channel_configs = configs.get_section(channel_id)
            channel_configs.update(channel_defaults, replace=False)

            channel_id = channel_configs.pop("id", channel_id)
            channel_uuid = f"{self.__component.uuid}.{channel_id}"
            self._update(uuid=channel_uuid, id=channel_id, **channel_configs)

    # noinspection PyUnresolvedReferences, PyShadowingBuiltins
    def add(self, id: str, **configs: Any) -> None:
        data_configs = self.configs
        if not data_configs.has_section("channels"):
            data_configs._add_section("channels", {})
        if not data_configs["channels"].has_section(id):
            data_configs["channels"]._add_section(id, configs)
        else:
            data_configs["channels"][id].update(configs, replace=False)

        if self.is_configured():
            channel_configs = self._parse_defaults(data_configs["channels"])
            # Be wary of the order. First, update the channel core with the default core
            # of the configuration file, then update the function arguments. Last, override
            # everything with the channel specific configurations of the file.
            channel_configs.update(configs)
            channel_configs.update(data_configs["channels"][id])
            channel_uuid = f"{self.__component.uuid}.{id}"
            self._update(uuid=channel_uuid, id=id, **channel_configs)

    def _add(self, channel: Channel) -> None:
        self.context._add(channel)
        self._set(channel.id, channel)

    def _set(self, uid: str, channel: Channel) -> None:
        self._channels[uid] = channel

    # noinspection PyShadowingBuiltins
    def _new(self, uuid: str, id: str, **configs: Any) -> Channel:
        return self.context._new(uuid=uuid, id=id, **configs)

    # noinspection PyShadowingBuiltins
    def _update(self, uuid: str, id: str, **configs: Any) -> None:
        channel = self._new(uuid=uuid, id=id, **configs)

        # TODO: Implement connector config update
        # if channel.id in self:
        #     self._get(channel.id).configs.update(configs)
        # else:
        #     self._add(channel)
        self._add(channel)

    @staticmethod
    def _parse_defaults(configs: Configurations) -> Mapping[str, Any]:
        return {k: v for k, v in configs.items() if not isinstance(v, Mapping) or k in ["logger", "connector"]}
