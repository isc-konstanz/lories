# -*- coding: utf-8 -*-
"""
lori.data.context
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Mapping
from copy import deepcopy
from typing import Any, Callable, Collection, Iterable, List, Optional, Tuple, Type

import numpy as np
import pandas as pd
from lori.core import Configurations, Context, Directories, Registrator, ResourceException
from lori.data.channels import Channel, Channels
from lori.typing import ChannelsType
from lori.util import update_recursive, validate_key

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


class DataContext(Context[Channel]):
    SECTION: str = "data"

    # noinspection PyProtectedMember
    def _load(
        self,
        context: Context | Registrator,
        configs: Configurations,
    ) -> None:
        defaults = {}
        configs = configs.copy()
        if configs.has_section(self.SECTION):
            data = configs.get_section(self.SECTION)
            update_recursive(defaults, Channel._build_defaults(configs))
            if data.has_section("channels"):
                self._load_from_sections(context, data.get_section("channels"), defaults)
        self._load_from_file(context, configs.dirs, "channels.conf", defaults=defaults)

    # noinspection PyProtectedMember, PyShadowingBuiltins
    def _load_from_configs(
        self,
        context: Context | Registrator,
        key: str,
        **configs: Any,
    ) -> Channel:
        id = Channel._build_id(key=key, context=context)
        if self._contains(id):
            self._update(id=id, key=key, **configs)
            return self._get(id)

        channel = self._create(id=id, key=key, **configs)
        self._add(channel)
        return channel

    # noinspection PyProtectedMember
    def _load_from_sections(
        self,
        context: Context | Registrator,
        configs: Configurations,
        defaults: Optional[Mapping[str, Any]] = None,
    ) -> Collection[Channel]:
        channels = []
        if defaults is None:
            defaults = {}
        update_recursive(defaults, Channel._build_defaults(configs))

        for channel_key in [i for i in configs.keys() if i not in defaults]:
            channel_configs = update_recursive(deepcopy(defaults), configs.get_section(channel_key))
            channel_key = validate_key(channel_configs.pop("key", channel_key))
            channels.append(self._load_from_configs(context, channel_key, **channel_configs))
        return channels

    def _load_from_file(
        self,
        context: Context | Registrator,
        configs_dirs: Directories,
        configs_file: str,
        defaults: Optional[Mapping[str, Any]] = None,
    ) -> Collection[Channel]:
        channels = []
        if configs_dirs.conf.joinpath(configs_file).is_file():
            configs = Configurations.load(configs_file, **configs_dirs.to_dict())
            channels.extend(self._load_from_sections(context, configs, defaults))
        return channels

    # noinspection PyShadowingBuiltins
    def _set(self, id: str, channel: Channel) -> None:
        if not isinstance(channel, Channel):
            raise ResourceException(f"Invalid channel type: {type(channel)}")

        # TODO: connector sanity check
        super()._set(id, channel)

    # noinspection PyShadowingBuiltins, PyProtectedMember, PyArgumentList
    def _update(self, id: str, key: str, type: Type, **configs: Any) -> None:
        channel = self._get(id)
        channel._update(type=type, **configs)

    @property
    def channels(self) -> Channels:
        return Channels(self.values())

    def _filter_by_args(self, channels: Optional[ChannelsType]) -> Channels:
        if channels is None:
            return self.channels
        _channels = []

        def append(_channel: Channel | str) -> None:
            if isinstance(_channel, str):
                if _channel in self:
                    _channels.append(self[_channel])
            elif isinstance(_channel, Channel):
                _channels.append(_channel)
            else:
                raise ResourceException(f"Invalid '{type(_channel)}' channel: {_channel}")

        if isinstance(channels, str) or isinstance(channels, Channel):
            append(channels)
        elif isinstance(channels, Iterable):
            for channel in channels:
                append(channel)
        else:
            raise ResourceException(f"Invalid '{type(channels)}' channels: {channels}")

        return Channels(_channels)

    # noinspection PyShadowingBuiltins
    def filter(self, filter: Callable[[Channel], bool]) -> Channels:
        return Channels(super().filter(filter))

    # noinspection SpellCheckingInspection
    def groupby(self, by: str) -> List[Tuple[Any, Channels]]:
        groups = []
        for group_by in np.unique([getattr(c, by) for c in self.values()]):
            groups.append((group_by, self.filter(lambda c: getattr(c, by) == group_by)))
        return groups

    @abstractmethod
    def register(
        self,
        function: Callable[[pd.DataFrame], None],
        channels: Optional[ChannelsType] = None,
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> None:
        pass

    @abstractmethod
    def read(
        self,
        channels: Optional[ChannelsType] = None,
        **kwargs,
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def write(self, data: pd.DataFrame, channels: Optional[ChannelsType] = None) -> None:
        pass

    def to_frame(self, **kwargs) -> pd.DataFrame:
        return self.channels.to_frame(**kwargs)
