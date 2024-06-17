# -*- coding: utf-8 -*-
"""
loris.data.access
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any

from loris import Channel, Configurable, Configurations, ConfigurationUnavailableException
from loris.data import DataMapping


class DataAccess(Configurable, DataMapping):
    SECTION: str = "data"

    def __init__(self, component, context, configs: Configurations, **channels: Channel) -> None:
        super().__init__(configs, **channels)
        self._component = component
        self._context = context

    # noinspection PyProtectedMember
    def __configure__(self, configs: Configurations) -> None:
        try:
            self._context.connectors._load(configs.get_section("connectors"), self._component.uuid)

        except ConfigurationUnavailableException:
            self._logger.debug(f"No connectors configured for configuration: {configs.name}")

        try:
            self._context._load(configs.get_section("channels"), self._component.uuid)

        except ConfigurationUnavailableException:
            self._logger.debug(f"No channels configured for configuration: {configs.name}")

    # noinspection PyProtectedMember, PyShadowingBuiltins
    def add(self, id: str, **configs: Any) -> None:
        channel = Channel(uuid=f"{self._component.uuid}.{id}", id=id, **configs)
        self._context._add(channel)
        self._add(channel)

    def _add(self, channel: Channel) -> None:
        self._channels[channel.id] = channel
