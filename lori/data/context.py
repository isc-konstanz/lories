# -*- coding: utf-8 -*-
"""
lori.data.mapping
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import abstractmethod
from collections import OrderedDict
from collections.abc import Mapping
from copy import deepcopy
from typing import Any, Callable, Collection, Iterator, List, Optional, Tuple, Type

import numpy as np
import pandas as pd
from lori.core import ConfigurationException, Configurations, Context, Directories, Registrator, ResourceException
from lori.data.channels import Channel, Channels
from lori.util import parse_key, update_recursive


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

    def __getitem__(self, key: str) -> Channel:
        return self._get(key)

    def __contains__(self, channel: str | Channel) -> bool:
        if isinstance(channel, str):
            return self._contains(channel)
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
        configs: Configurations,
    ) -> None:
        defaults = {}
        configs = configs.copy()
        if configs.has_section(self.SECTION):
            data = configs.get_section(self.SECTION)
            self._update_configs(defaults, self._build_defaults(configs))
            if data.has_section("channels"):
                self._load_sections(context, data.get_section("channels"), defaults)
        self._load_from_file(context, configs.dirs, defaults=defaults)

    # noinspection PyProtectedMember
    def _load_sections(
        self,
        context: Registrator,
        configs: Configurations,
        defaults: Optional[Mapping[str, Any]] = None,
    ) -> Collection[Channel]:
        channels = []
        if defaults is None:
            defaults = {}
        update_recursive(defaults, Channel._build_defaults(configs))

        for channel_key in [i for i in configs.keys() if i not in defaults]:
            channel_configs = update_recursive(deepcopy(defaults), configs.get_section(channel_key))

            channel_key = parse_key(channel_configs.pop("key", channel_key))
            channel_id = f"{context.id}.{channel_key}"
            channels.append(self._update(id=channel_id, key=channel_key, **channel_configs))
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

    # noinspection PyShadowingBuiltins, PyProtectedMember
    def _update(self, id: str, key: str, **configs: Any) -> Channel:
        if id in self:
            channel = self._get(id)
            channel._update(**configs)
        else:
            channel = self._new(id=id, key=key, **configs)
        self._add(channel)
        return channel

    # noinspection PyShadowingBuiltins
    def _new(self, id: str, key: str, type: Type, **configs: Any) -> Channel:
        return Channel(id=id, key=key, type=type, **configs)

    def _add(self, channel: Channel) -> None:
        if not isinstance(channel, Channel):
            raise ResourceException(f"Invalid channel type: {type(channel)}")

        if channel.id in self._channels.keys():
            raise ConfigurationException(f'Channel with ID "{channel.id}" already exists')

        # TODO: connector sanity check
        self._set(channel.id, channel)

    # noinspection PyShadowingBuiltins
    def _set(self, id: str, channel: Channel) -> None:
        self._channels[id] = channel

    # noinspection PyShadowingBuiltins
    def _get(self, id: str) -> Channel:
        return self._channels.get(id)

    # noinspection PyShadowingBuiltins
    def _contains(self, id: str) -> bool:
        return id in self._channels.keys()

    # noinspection PyShadowingBuiltins
    def _remove(self, id: str) -> None:
        del self._channels[id]

    @property
    def channels(self) -> Channels:
        return Channels(self._channels.values())

    # noinspection PyShadowingBuiltins
    def filter(self, filter: Callable[[Channel], bool]) -> Channels:
        return Channels([c for c in self._channels.values() if filter(c)])

    # noinspection SpellCheckingInspection
    def groupby(self, by: str) -> List[Tuple[Any, Channels]]:
        groups = []
        for group_by in np.unique([getattr(c, by) for c in self._channels.values()]):
            groups.append((group_by, self.filter(lambda c: getattr(c, by) == group_by)))
        return groups

    @abstractmethod
    def read(
        self,
        channels: Optional[Channels] = None,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def write(self, data: pd.DataFrame, channels: Optional[Channels] = None) -> None:
        pass

    def to_frame(self, **kwargs) -> pd.DataFrame:
        return self.channels.to_frame(**kwargs)
