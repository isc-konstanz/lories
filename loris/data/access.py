# -*- coding: utf-8 -*-
"""
    loris.data.access
    ~~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations

import logging

from loris import Channel, Configurable, Configurations, ConfigurationUnavailableException
from loris.data import DataMapping

logger = logging.getLogger(__name__)


class DataAccess(Configurable, DataMapping):

    SECTION: str = 'data'

    def __init__(self, component, context, configs: Configurations, **channels: Channel) -> None:
        super().__init__(configs, **channels)
        self._component = component
        self._context = context

    # noinspection PyProtectedMember
    def __configure__(self, configs: Configurations) -> None:
        try:
            self._context.connectors._load(configs.get_section('connectors'), self._component._uuid)

        except ConfigurationUnavailableException:
            logger.debug(f"No connectors configured for configuration: {configs.name}")

        try:
            self._context._load(configs.get_section('channels'), self._component._uuid)

        except ConfigurationUnavailableException:
            logger.debug(f"No channels configured for configuration: {configs.name}")

    # noinspection PyProtectedMember
    def add(self, channel: Channel | Configurations) -> None:
        channel_id = f"{self._component._uuid}.{channel._configs['id']}"
        self._context._add(channel_id, channel)
        self._add(self._context._add.get(channel_id))

    def _add(self, channel: Channel) -> None:
        self._channels[channel.id] = channel
