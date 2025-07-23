# -*- coding: utf-8 -*-
"""
lori.data.access
~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Callable, Collection, Optional, Type, overload

import pandas as pd
from lori.core import Configurator, Constant, Context, Registrator, ResourceException
from lori.data import Channel, Channels, DataContext
from lori.data.typing import ChannelsType
from lori.typing import TimestampType
from lori.util import get_context, update_recursive

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


# noinspection PyProtectedMember, PyShadowingBuiltins
class DataAccess(DataContext, Configurator):
    __registrator: Registrator
    __context: Context

    def __init__(self, registrar: Registrator, **kwargs: Any) -> None:
        super().__init__(logger=registrar._logger, **kwargs)
        self.__registrator = self._assert_registrar(registrar)
        self.__context = self._assert_context(get_context(registrar, DataContext))

    @classmethod
    def _assert_registrar(cls, registrar: Registrator):
        if registrar is None or not isinstance(registrar, Registrator):
            raise ResourceException(f"Invalid '{cls.__name__}' registrator: {type(registrar)}")
        return registrar

    @classmethod
    def _assert_context(cls, context: DataContext):
        from lori.data.manager import DataManager

        if context is None or not isinstance(context, DataManager):
            raise ResourceException(f"Invalid '{cls.__name__}' context: {type(context)}")
        return context

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(str(c.key) for c in self.values())})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{v.key} = {repr(v)}" for v in self.values())

    # noinspection PyArgumentList
    def __getattr__(self, attr):
        channels = Context.__getattribute__(self, f"_{Context.__name__}__map")
        channels_by_key = {c.key: c for c in channels.values()}
        if attr in channels_by_key:
            return channels_by_key[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{attr}'")

    def __validate_id(self, id: str) -> str:
        if not len(id.split(".")) > 1:
            id = f"{self.__registrator.id}.{id}"
        return id

    def _contains(self, __channel: str | Channel) -> bool:
        channels = Context.__getattribute__(self, f"_{Context.__name__}__map")
        if isinstance(__channel, str):
            __channel = self.__validate_id(__channel)
            return __channel in channels.keys()
        if isinstance(__channel, Registrator):
            return __channel in channels.values()
        return False

    def _get(self, id: str) -> Channel:
        return super()._get(self.__validate_id(id))

    def _set(self, id: str, channel: Channel) -> None:
        id = self.__validate_id(id)

        self.context._set(id, channel)
        super()._set(id, channel)

    def _create(self, id: str, key: str, type: Type, **configs: Any) -> Channel:
        return self.context._create(id=id, key=key, type=type, **configs)

    def _remove(self, *__objects: str | Channel) -> None:
        for __object in __objects:
            if isinstance(__object, str):
                __object = self.__validate_id(__object)

            self.context._remove(__object)
            super()._remove(__object)

    # noinspection PyTypeChecker
    @property
    def empty(self) -> bool:
        return len(self) == 0 or self.to_frame(states=False).dropna(axis="index", how="all").empty

    # noinspection PyTypeChecker
    @property
    def context(self) -> DataContext:
        return self.__context

    def load(self, sort: bool = True) -> Collection[Channel]:
        channels = []
        defaults = {}
        if self.configs.has_section(Channels.SECTION):
            section = self.configs.get_section(Channels.SECTION)
            defaults = Channel._build_defaults(section)
            channels.extend(self._load_from_sections(self.__registrator, section))
        channels.extend(
            self._load_from_file(self.__registrator, self.configs.dirs, f"{Channels.SECTION}.conf", defaults=defaults)
        )

        if sort:
            self.sort()
        return channels

    # noinspection PyUnresolvedReferences
    def add(self, key: str | Constant, **configs: Any) -> None:
        if isinstance(key, Constant):
            configs = {
                **key.to_dict(),
                **configs,
            }
            key = configs.pop("key")
        configs = Channel._build_configs(configs)
        channels = self.configs.get_section(Channels.SECTION, ensure_exists=True)
        if not channels.has_section(key):
            channels._add_section(key, configs)
        else:
            channel_configs = Channel._build_configs(channels[key])
            channel_configs = update_recursive(channel_configs, configs, replace=False)
            channels[key] = channel_configs

        if self.__registrator.is_configured():
            channel_defaults = Channel._build_defaults(channels)
            channel_configs = Channel._build_configs(channel_defaults)
            # Be wary of the order. First, update the channel core with the default core
            # of the configuration file, then update the function arguments. Last, override
            # everything with the channel specific configurations of the file.
            channel_configs = update_recursive(channel_configs, configs)
            channel_configs = update_recursive(channel_configs, channels[key])
            channel_id = f"{self.__registrator.id}.{key}"
            if self._contains(channel_id):
                self._update(id=channel_id, key=key, **channel_configs)
            else:
                channel = self._create(id=channel_id, key=key, **channel_configs)
                self._add(channel)

    # noinspection PyUnresolvedReferences
    def register(
        self,
        function: Callable[[pd.DataFrame], None],
        channels: Optional[ChannelsType] = None,
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> None:
        channels = self._filter_by_args(channels)
        self.__context.register(function, channels, how=how, unique=unique)

    @overload
    def has_logged(
        self,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> bool: ...

    @overload
    def has_logged(
        self,
        channels: ChannelsType,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> bool: ...

    # noinspection PyUnresolvedReferences
    def has_logged(
        self,
        channels: Optional[ChannelsType] = None,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> bool:
        channels = self._filter_by_args(channels)
        return self.__context.has_logged(channels, start, end)

    @overload
    def from_logger(
        self,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
        unique: bool = False,
    ) -> pd.DataFrame: ...

    @overload
    def from_logger(
        self,
        channels: ChannelsType,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
        unique: bool = False,
    ) -> pd.DataFrame: ...

    def from_logger(
        self,
        channels: Optional[ChannelsType] = None,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
        unique: bool = False,
    ) -> pd.DataFrame:
        return self.read_logged(channels, start, end, unique=unique)

    # noinspection PyUnresolvedReferences
    def read_logged(
        self,
        channels: Optional[ChannelsType] = None,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
        unique: bool = False,
    ) -> pd.DataFrame:
        channels = self._filter_by_args(channels)
        data = self.__context.read_logged(channels, start, end)
        if not unique:
            data.rename(columns={c.id: c.key for c in channels}, inplace=True)
        return data

    # noinspection PyUnresolvedReferences
    def read(
        self,
        channels: Optional[ChannelsType] = None,
        unique: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        channels = self._filter_by_args(channels)
        data = self.__context.read(channels, **kwargs)
        if not unique:
            data.rename(columns={c.id: c.key for c in channels}, inplace=True)
        return data

    # noinspection PyUnresolvedReferences
    def write(
        self,
        data: pd.DataFrame,
        channels: Optional[ChannelsType] = None,
    ) -> None:
        if data is None:
            raise ResourceException(f"Invalid data to write '{self.id}': {data}")
        data.rename(columns={c.key: c.id for c in channels}, inplace=True)
        channels = self._filter_by_args(channels)
        self.__context.write(data, channels)
