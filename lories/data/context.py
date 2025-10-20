# -*- coding: utf-8 -*-
"""
lories.data.context
~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any, Callable, Collection, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
from lories._core import _DataContext  # noqa
from lories.core.configs import ConfigurationError, Configurations, Directories
from lories.core.errors import ResourceError
from lories.core.typing import ChannelsArgument, ContextArgument
from lories.data.channels import Channel, Channels
from lories.util import update_recursive, validate_key


# noinspection PyAbstractClass, PyProtectedMember
class DataContext(_DataContext):
    def _load(
        self,
        context: ContextArgument,
        configs: Configurations,
        sort: bool = True,
    ) -> Collection[Channel]:
        channels = []

        defaults = {}
        configs = configs.copy()
        if configs.has_member(self.TYPE):
            data = configs.get_member(self.TYPE)
            update_recursive(defaults, Channel._build_defaults(configs))
            if data.has_member(Channels.TYPE):
                channels.extend(self._load_from_members(context, data.get_member(Channels.TYPE), defaults))
        channels.extend(self._load_from_file(context, configs.dirs, f"{Channels.TYPE}.conf", defaults=defaults))

        if sort:
            self.sort()
        return channels

    # noinspection PyShadowingBuiltins
    def _load_from_configs(
        self,
        context: ContextArgument,
        key: str,
        **configs: Any,
    ) -> Channel:
        id = Channel._build_id(key=key, context=context)
        if "type" not in configs:
            raise ConfigurationError("Missing 'type' for channel: " + id)

        if self._contains(id):
            self._update(id=id, key=key, **configs)
            return self._get(id)

        channel = self._create(id=id, key=key, **configs)
        self._add(channel)
        return channel

    def _load_from_members(
        self,
        context: ContextArgument,
        configs: Configurations,
        defaults: Optional[Mapping[str, Any]] = None,
    ) -> Collection[Channel]:
        channels = []
        if defaults is None:
            defaults = {}
        update_recursive(defaults, Channel._build_defaults(configs))

        for channel_key in [i for i in configs.keys() if i not in defaults]:
            channel_configs = update_recursive(deepcopy(defaults), configs.get_member(channel_key))
            channel_key = validate_key(channel_configs.pop("key", channel_key))
            channels.append(self._load_from_configs(context, channel_key, **channel_configs))
        return channels

    def _load_from_file(
        self,
        context: ContextArgument,
        configs_dirs: Directories,
        configs_file: str,
        defaults: Optional[Mapping[str, Any]] = None,
    ) -> Collection[Channel]:
        channels = []
        if configs_dirs.conf.joinpath(configs_file).is_file():
            configs = Configurations.load(configs_file, **configs_dirs.to_dict())
            channels.extend(self._load_from_members(context, configs, defaults))
        return channels

    # noinspection PyShadowingBuiltins
    def _set(self, id: str, channel: Channel) -> None:
        if not isinstance(channel, Channel):
            raise ResourceError(f"Invalid channel type: {type(channel)}")

        # TODO: connector sanity check
        super()._set(id, channel)

    # noinspection PyShadowingBuiltins, PyProtectedMember, PyArgumentList
    def _update(self, id: str, key: str, **configs: Any) -> None:
        channel = self._get(id)
        channel._update(**configs)

    @property
    def channels(self) -> Channels:
        return Channels(self.values())

    def _filter_by_args(self, channels: Optional[ChannelsArgument]) -> Channels:
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
                raise ResourceError(f"Invalid '{type(_channel)}' channel: {_channel}")

        if isinstance(channels, str) or isinstance(channels, Channel):
            append(channels)
        elif isinstance(channels, Iterable):
            for channel in channels:
                append(channel)
        else:
            raise ResourceError(f"Invalid '{type(channels)}' channels: {channels}")

        return Channels(_channels)

    # noinspection PyShadowingBuiltins
    def filter(self, *filters: Optional[Callable[[Channel], bool]]) -> Channels:
        return Channels(super().filter(*filters))

    # noinspection SpellCheckingInspection
    def groupby(self, by: str) -> List[Tuple[Any, Channels]]:
        groups = []
        for group_by in np.unique([getattr(c, by) for c in self.values()]):
            groups.append((group_by, self.filter(lambda c: getattr(c, by) == group_by)))
        return groups

    def to_frame(self, **kwargs) -> pd.DataFrame:
        return self.channels.to_frame(**kwargs)
