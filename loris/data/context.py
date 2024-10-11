# -*- coding: utf-8 -*-
"""
loris.data.mapping
~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import abstractmethod
from collections import OrderedDict
from collections.abc import Mapping
from copy import deepcopy
from typing import Any, Callable, Collection, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd
from loris.core import Configurations, Context, Directories, Registrator
from loris.data.channels import Channel, Channels
from loris.util import parse_key


class DataContext(Context[Channel]):
    SECTION: str = "data"

    _channels: OrderedDict[str, Channel]

    def __init__(self, channels=(), *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._channels = OrderedDict(channels)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({[c.key for c in self._channels.values()]})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join([f"{i} = {repr(c)}" for i, c in self._channels.items()])

    def __getitem__(self, key: str) -> Channel:
        return self._get(key)

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
        configs: Configurations,
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
        defaults: Optional[Mapping[str, Any]] = None,
    ) -> Collection[Channel]:
        channels = []
        if defaults is None:
            defaults = {}
        defaults.update(self._parse_defaults(configs))
        for channel_key in [i for i in configs.keys() if i not in defaults]:
            channel_configs = deepcopy(defaults)
            channel_configs.update(configs.get_section(channel_key))

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

    @staticmethod
    def _parse_defaults(configs: Configurations) -> Mapping[str, Any]:
        return {k: v for k, v in configs.items() if not isinstance(v, Mapping) or k in ["logger", "connector"]}

    def _get(self, key: str) -> Channel:
        return self._channels.get(key)

    def _set(self, key: str, channel: Channel) -> None:
        self._channels[key] = channel

    @abstractmethod
    def _add(self, channel: Channel) -> None:
        pass

    @abstractmethod
    def _new(self, id: str, key: str, **configs: Any) -> Channel:
        pass

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def _update(self, id: str, key: str, **configs: Any) -> Channel:
        pass

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
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def write(self, data: pd.DataFrame, channels: Optional[Channels] = None) -> None:
        pass

    def to_frame(self, **kwargs) -> pd.DataFrame:
        return self.channels.to_frame(**kwargs)
