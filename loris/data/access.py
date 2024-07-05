# -*- coding: utf-8 -*-
"""
loris.data.access
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any

from loris import Channel, Configurations, Configurator
from loris.data import DataMapping


class DataAccess(Configurator, DataMapping):
    SECTION: str = "data"

    def __init__(self, component, context, configs: Configurations, **channels: Channel) -> None:
        super().__init__(configs, **channels)
        self.__component = component
        self.__context = context

    # noinspection PyProtectedMember
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        if configs.has_section("connectors"):
            self.__context.connectors._do_load_sections(configs.get_section("connectors"), self.__component.uuid)

        if configs.has_section("channels"):
            self.__context._do_load_sections(configs.get_section("channels"), self.__component.uuid)

    # noinspection PyProtectedMember, PyShadowingBuiltins
    def add(self, id: str, **configs: Any) -> None:
        channel = Channel(uuid=f"{self.__component.uuid}.{id}", id=id, **configs)
        self.__context._add(channel)
        self._add(channel)

    def _add(self, channel: Channel) -> None:
        self._channels[channel.id] = channel
