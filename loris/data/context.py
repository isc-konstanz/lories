# -*- coding: utf-8 -*-
"""
loris.data.context
~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import os
from collections.abc import Mapping

from loris import Channel, Channels, ConfigurationException, Configurations, Configurator, LocalResourceException
from loris.components import ComponentContext
from loris.connectors import ConnectorContext
from loris.data import DataMapping


class DataContext(Configurator, DataMapping):
    _components: ComponentContext
    _connectors: ConnectorContext

    def __init__(self, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        self._components = ComponentContext(configs)
        self._connectors = ConnectorContext(configs)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._do_load_from_file(configs.dirs.conf)

    # noinspection PyTypeChecker, PyProtectedMember, PyUnresolvedReferences
    def _do_load_from_file(
        self,
        configs_dir: str,
        configs_file: str = "channels.conf",
        prefix_uuid: str = None
    ) -> None:
        configs_path = os.path.join(configs_dir, configs_file)
        if os.path.isfile(configs_path):
            configs_dirs = self.configs.dirs.encode()
            configs_dirs["conf_dir"] = configs_dir
            configs = Configurations.load(configs_file, **configs_dirs)
            self._do_load_sections(configs, prefix_uuid)

    def _do_load_sections(
        self,
        configs: Configurations,
        prefix_uuid: str = None
    ) -> None:
        channel_ids = [
            i for i in configs.keys() if (isinstance(configs[i], Mapping) and i not in ["logger", "connector"])
        ]
        channels = {i: configs.pop(i) for i in channel_ids}
        for channel_id, channel_section in channels.items():
            channel_uuid = channel_id if prefix_uuid is None else f"{prefix_uuid}.{channel_id}"
            channel_configs = configs.copy()
            channel_configs.update(channel_section)
            channel_configs.set("uuid", channel_uuid)
            channel_configs.set("id", channel_id)

            for connector_type in ["logger", "connector"]:
                connector = channel_configs.get(connector_type, None)
                if not connector:
                    continue
                if isinstance(connector, str):
                    channel_configs[connector_type] = connector = {"connector": connector}
                elif not isinstance(connector, Mapping):
                    raise ConfigurationException(f"Invalid channel {connector_type} type: " + str(connector))
                if "connector" in connector:
                    connector_uuid = connector["connector"]
                    if prefix_uuid is not None:
                        connector_uuid = f"{prefix_uuid}.{connector['connector']}"
                    if connector_uuid in self.connectors.keys():
                        channel_configs[connector_type]["connector"] = connector_uuid

            channel = self._new(channel_configs)

            # TODO: Implement channel config update
            # if channel.uuid in self:
            #     self.__get(channel.uuid).update(channel_configs)
            # else:
            #     self._add(channel)
            self._add(channel)

    def __get(self, uuid: str) -> Channel:
        return self._channels.get(uuid)

    # noinspection PyMethodMayBeStatic
    def _new(self, configs: Configurations) -> Channel:
        return Channel(**configs)

    def _add(self, channel: Channel) -> None:
        if not isinstance(channel, Channel):
            raise LocalResourceException(f"Invalid channel type: {type(channel)}")

        if channel.uuid in self._channels.keys():
            raise ConfigurationException(f'Channel with UUID "{channel.uuid}" already exists')

        # TODO: connector sanity check
        self._channels[channel.uuid] = channel

    def _remove(self, uuid: str) -> None:
        del self._channels[uuid]

    @property
    def components(self) -> ComponentContext:
        return self._components

    @property
    def connectors(self) -> ConnectorContext:
        return self._connectors

    @property
    def channels(self) -> Channels:
        return self.values()
