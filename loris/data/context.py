# -*- coding: utf-8 -*-
"""
loris.data.mapping
~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from copy import deepcopy
from typing import Any, Collection, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd
from loris.core import Configurations, ConfigurationException, Context, Directories, Registrator, ResourceException
from loris.data.channels import Channel, Channels


class DataContext(Context[Channel]):
    SECTION: str = "data"

    _channels: OrderedDict[str, Channel]

    def __init__(self, channels=(), *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._channels = OrderedDict(channels)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({[c.id for c in self._channels.values()]})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join([f"{i} = {repr(c)}" for i, c in self._channels.items()])

    def __getitem__(self, uid: str) -> Channel:
        return self._get(uid)

    def __contains__(self, channel: str | Channel) -> bool:
        if isinstance(channel, str):
            return channel in self._channels.keys()
        if isinstance(channel, Channel):
            return channel in self._channels.values()
        return False

    def __len__(self) -> int:
        return len(self._channels)

    def __iter__(self) -> Iterator[str]:
        return iter(self._channels)

    def _load(
         self,
         context: Registrator,
         configs: Configurations
    ) -> None:
        defaults = {}
        configs = configs.copy()
        if configs.has_section(self.SECTION):
            data = configs.get_section(self.SECTION)
            defaults.update(self._parse_defaults(data))
            if data.has_section("channels"):
                self._load_sections(context, data.get_section("channels"), defaults)
        self._load_from_file(context, configs.dirs, defaults=defaults)

    def _load_sections(
        self,
        context: Registrator,
        configs: Configurations,
        defaults: Optional[Mapping[str, Any]] = None
    ) -> Collection[Channel]:
        channels = []
        if defaults is None:
            defaults = {}
        defaults.update(self._parse_defaults(configs))
        for channel_id in [i for i in configs.keys() if i not in defaults]:
            channel_configs = deepcopy(defaults)
            channel_configs.update(configs.get_section(channel_id))

            channel_id = channel_configs.pop("id", channel_id)
            channel_uuid = f"{context.uuid}.{channel_id}"
            channels.append(self._update(uuid=channel_uuid, id=channel_id, **channel_configs))
        return channels

    # noinspection PyProtectedMember
    def _load_from_file(
        self,
        context: Registrator,
        configs_dirs: Directories,
        configs_file: str = "channels.conf",
        defaults: Mapping[str, Any] = None,
    ) -> Collection[Channel]:
        channels = []
        if configs_dirs.conf.joinpath(configs_file).is_file():
            configs = Configurations(configs_file, deepcopy(configs_dirs))
            configs._load()
            channels.extend(self._load_sections(context, configs, defaults))
        return channels

    @staticmethod
    def _parse_defaults(configs: Configurations) -> Mapping[str, Any]:
        return {k: v for k, v in configs.items() if not isinstance(v, Mapping) or k in ["logger", "connector"]}

    def _get(self, uuid: str) -> Channel:
        return self._channels.get(uuid)

    def _set(self, uuid: str, channel: Channel) -> None:
        self._channels[uuid] = channel

    def _add(self, channel: Channel) -> None:
        if not isinstance(channel, Channel):
            raise ResourceException(f"Invalid channel type: {type(channel)}")

        if channel.uuid in self._channels.keys():
            raise ConfigurationException(f'Channel with UUID "{channel.uuid}" already exists')

        # TODO: connector sanity check
        self._set(channel.uuid, channel)

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
                connector_uuid = connector["connector"]
                if not connector_uuid.startswith(self.uuid):
                    connector_uuid = uuid.replace(id, connector["connector"])
                    if connector_uuid not in self.connectors.keys():
                        connector_uuid = f"{self.uuid}.{connector['connector']}"
                configs[connector_type]["connector"] = connector_uuid

        return Channel(uuid, id, **configs)

    # noinspection PyShadowingBuiltins
    def _update(self, uuid: str, id: str, **configs: Any) -> Channel:
        channel = self._new(uuid=uuid, id=id, **configs)

        # TODO: Implement connector config update
        # if channel.id in self:
        #     self._get(channel.id).configs.update(configs)
        # else:
        #     self._add(channel)
        self._add(channel)
        return channel

    def _remove(self, uuid: str) -> None:
        del self._channels[uuid]

    def values(self) -> Channels:
        return Channels(self._channels.values())

    # noinspection PyShadowingBuiltins
    def filter(self, filter: callable) -> Channels:
        return Channels([c for c in self._channels.values() if filter(c)])

    # noinspection SpellCheckingInspection
    def groupby(self, by: str) -> List[Tuple[Any, Channels]]:
        groups = []
        for group_by in np.unique([getattr(c, by) for c in self._channels.values()]):
            groups.append((group_by, self.filter(lambda c: getattr(c, by) == group_by)))
        return groups

    def to_frame(self, **kwargs) -> pd.DataFrame:
        return self.values().to_frame(**kwargs)
