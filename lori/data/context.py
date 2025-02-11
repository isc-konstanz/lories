# -*- coding: utf-8 -*-
"""
lori.data.context
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import abstractmethod
from collections.abc import Mapping
from copy import deepcopy
from typing import Any, Callable, Collection, List, Optional, Tuple, Type

import numpy as np
import pandas as pd
from lori.core import ConfigurationException, Configurations, Context, Directories, Identifier, ResourceException
from lori.data.channels import Channel, Channels
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
        context: Identifier,
        configs: Configurations,
    ) -> None:
        defaults = {}
        configs = configs.copy()
        if configs.has_section(self.SECTION):
            data = configs.get_section(self.SECTION)
            update_recursive(defaults, Channel._build_defaults(configs))
            if data.has_section("channels"):
                self._load_sections(context, data.get_section("channels"), defaults)
        self._load_from_file(context, configs.dirs, defaults=defaults)

    # noinspection PyProtectedMember
    def _load_sections(
        self,
        context: Identifier,
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
            channel_id = f"{context.id}.{channel_key}"
            channels.append(self._update(id=channel_id, key=channel_key, **channel_configs))
        return channels

    # noinspection PyProtectedMember
    def _load_from_file(
        self,
        context: Identifier,
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
    def _update(self, id: str, key: str, type: Type, **configs: Any) -> Channel:
        if id in self:
            channel = self._get(id)
            channel._update(type=type, **configs)
        else:
            channel = self._new(id=id, key=key, type=type, **configs)
        self._add(channel)
        return channel

    # noinspection PyShadowingBuiltins
    def _new(self, id: str, key: str, type: Type, **configs: Any) -> Channel:
        return Channel(id=id, key=key, type=type, context=self, **configs)

    # noinspection PyShadowingBuiltins, PyProtectedMember
    def _set(self, id: str, channel: Channel) -> None:
        if not isinstance(channel, Channel):
            raise ResourceException(f"Invalid channel type: {type(channel)}")

        if id in self.keys():
            raise ConfigurationException(f'Channel with ID "{id}" already exists')

        # TODO: connector sanity check
        super()._set(id, channel)

    @property
    def channels(self) -> Channels:
        return Channels(self.values())

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
        *channels: Channel | str,
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> None:
        pass

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
