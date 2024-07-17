# -*- coding: utf-8 -*-
"""
loris.data.context
~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any, Collection

from loris import Channel, Channels, ConfigurationException, Configurations, Configurator, LocalResourceException
from loris.components import ComponentContext
from loris.connectors import ConnectorContext
from loris.data import DataMapping


class DataContext(Configurator, DataMapping):
    _connectors: ConnectorContext
    _components: ComponentContext

    def __init__(self, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        self._connectors = ConnectorContext(configs)
        self._components = ComponentContext(configs)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._do_load_from_file(configs.dirs.conf)

    def _do_configure_members(self, configurators: Collection[Configurator]) -> None:
        # Ensure components always being configured first
        configurators = list(configurators)
        configurators.remove(self._connectors)
        configurators.remove(self._components)
        super()._do_configure_members(configurators + [self._components, self._connectors])

    # noinspection PyTypeChecker, PyProtectedMember, PyUnresolvedReferences
    def _do_load_from_file(self, configs_dir: str, configs_file: str = "channels.conf") -> None:
        configs_path = os.path.join(configs_dir, configs_file)
        if os.path.isfile(configs_path):
            configs_dirs = self.configs.dirs.encode()
            configs_dirs["conf_dir"] = configs_dir
            configs = Configurations.load(configs_file, **configs_dirs)

            if "data" not in self.configs:
                self.configs.add_section("data", configs)
            else:
                self.configs.get_section("data").update(configs, replace=False)

            self._do_load_sections(self.configs.get_section("data"))

    def _do_load_sections(self, configs: Configurations) -> None:
        channel_ids = [
            i for i in configs.keys() if (isinstance(configs[i], Mapping) and i not in ["logger", "connector"])
        ]
        channel_defaults = {k: v for k, v in configs.items() if k not in channel_ids}
        for channel_id in channel_ids:
            channel_configs = configs.get_section(channel_id)
            channel_configs.update(channel_defaults, replace=False)

            channel_id = channel_configs.pop("id", channel_id)
            channel_uuid = f"{self.__component.uuid}.{channel_id}"
            channel = self._new(uuid=channel_uuid, id=channel_id, **channel_configs)

            # TODO: Implement channel config update
            # if channel.uuid in self:
            #     self.__get(channel.uuid).update(channel_configs)
            # else:
            #     self._add(channel)
            self._add(channel)

    def __get(self, uuid: str) -> Channel:
        return self._channels.get(uuid)

    # noinspection PyShadowingBuiltins
    def _new(self, id: str, uuid: str = None, **configs: Any) -> Channel:
        for connector_type in ["logger", "connector"]:
            connector = configs.get(connector_type, None)
            if not connector:
                continue
            if isinstance(connector, str):
                configs[connector_type] = connector = {"connector": connector}
            elif not isinstance(connector, Mapping):
                raise ConfigurationException(f"Invalid channel {connector_type} type: " + str(connector))
            if "connector" in connector:
                connector_uuid = connector["connector"] if id == uuid else uuid.replace(id, connector["connector"])
                if connector_uuid in self.connectors.keys():
                    configs[connector_type]["connector"] = connector_uuid

        return Channel(uuid, id, **configs)

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
