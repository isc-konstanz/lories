# -*- coding: utf-8 -*-
"""
lories.data.access
~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Callable, Collection, Iterable, Optional, Type, overload

import pandas as pd
from lories._core import _Context, _Registrator  # noqa
from lories._core._data import DataContext, _DataContext, _DataManager  # noqa
from lories.core import Configurator, Constant, ResourceError
from lories.core.typing import ChannelsArgument, Registrator, Timestamp
from lories.data.channels import Channel, Channels
from lories.data.context import DataContext as _DataAccess
from lories.util import get_context, update_recursive

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


# noinspection PyProtectedMember, PyShadowingBuiltins
class DataAccess(_DataAccess, Configurator):
    __registrar: _Registrator
    __context: _DataContext

    def __init__(self, registrar: Registrator, **kwargs: Any) -> None:
        registrar = self._assert_registrar(registrar)
        super().__init__(logger=registrar._logger, **kwargs)
        self.__registrar = registrar
        self.__context = self._assert_context(get_context(registrar, _DataManager))

    @classmethod
    def _assert_registrar(cls, registrar: Registrator) -> Registrator:
        if registrar is None or not isinstance(registrar, _Registrator):
            raise TypeError(f"Invalid '{cls.__name__}' registrator: {type(registrar)}")
        return registrar

    @classmethod
    def _assert_context(cls, context: DataContext) -> DataContext:
        if context is None or not isinstance(context, _DataManager):
            raise TypeError(f"Invalid '{cls.__name__}' context: {type(context)}")
        return context

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(str(c.key) for c in self.values())})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{v.key} = {repr(v)}" for v in self.values())

    def __getitem__(self, id: Iterable[str] | str) -> Channel | Channels:
        if isinstance(id, str):
            return self._get(id)
        if isinstance(id, Iterable):
            return Channels([self._get(i) for i in id])
        raise KeyError(id)

    # noinspection PyArgumentList
    def __getattr__(self, attr):
        channels = _Context.__getattribute__(self, f"{_Context.__name__}__map")
        channels_by_key = {c.key: c for c in channels.values()}
        if attr in channels_by_key:
            return channels_by_key[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{attr}'")

    def __validate_id(self, id: str) -> str:
        if not len(id.split(".")) > 1:
            id = f"{self.__registrar.id}.{id}"
        return id

    # noinspection PyArgumentList
    def _contains(self, __channel: str | Channel) -> bool:
        channels = _Context.__getattribute__(self, f"{_Context.__name__}__map")
        if isinstance(__channel, str):
            __channel = self.__validate_id(__channel)
            return __channel in channels.keys()
        if isinstance(__channel, _Registrator):
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

    @property
    def empty(self) -> bool:
        return len(self.values()) == 0 or self.to_frame(states=False).dropna(axis="index", how="all").empty

    @property
    def context(self) -> DataContext:
        return self.__context

    def load(self, sort: bool = True) -> Collection[Channel]:
        channels = []
        defaults = {}
        if self.configs.has_member(Channels.TYPE):
            configs = self.configs.get_member(Channels.TYPE)
            defaults = Channel._build_defaults(configs)
            channels.extend(self._load_from_members(self.__registrar, configs))
        channels.extend(
            self._load_from_file(self.__registrar, self.configs.dirs, f"{Channels.TYPE}.conf", defaults=defaults)
        )

        if sort:
            self.sort()
        return channels

    def add(self, key: str | Constant, **configs: Any) -> None:
        if isinstance(key, Constant):
            configs = {
                **key.to_dict(),
                **configs,
            }
            key = configs.pop("key")
        configs = Channel._build_configs(configs)
        channels = self.configs.get_member(Channels.TYPE, ensure_exists=True)
        if not channels.has_member(key):
            channels._add_member(key, configs)
        else:
            channel_configs = Channel._build_configs(channels[key])
            channel_configs = update_recursive(channel_configs, configs, replace=False)
            channels[key] = channel_configs

        if self.__registrar.is_configured():
            channel_defaults = Channel._build_defaults(channels)
            channel_configs = Channel._build_configs(channel_defaults)
            # Be wary of the order. First, update the channel core with the default core
            # of the configuration file, then update the function arguments. Last, override
            # everything with the channel specific configurations of the file.
            channel_configs = update_recursive(channel_configs, configs)
            channel_configs = update_recursive(channel_configs, channels[key])
            channel_id = f"{self.__registrar.id}.{key}"
            if self._contains(channel_id):
                self._update(id=channel_id, key=key, **channel_configs)
            else:
                channel = self._create(id=channel_id, key=key, **channel_configs)
                self._add(channel)

    def register(
        self,
        function: Callable[[pd.DataFrame], None],
        channels: Optional[ChannelsArgument] = None,
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> None:
        channels = self._filter_by_args(channels)
        self.__context.register(function, channels=channels, how=how, unique=unique)

    def has_logged(
        self,
        channels: Optional[ChannelsArgument] = None,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
    ) -> bool:
        channels = self._filter_by_args(channels)
        return self.__context.has_logged(channels=channels, start=start, end=end, timeout=timeout)

    @overload
    def from_logger(
        self,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
        unique: bool = False,
    ) -> pd.DataFrame: ...

    @overload
    def from_logger(
        self,
        channels: ChannelsArgument,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
        unique: bool = False,
    ) -> pd.DataFrame: ...

    def from_logger(
        self,
        channels: Optional[ChannelsArgument] = None,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
        unique: bool = False,
    ) -> pd.DataFrame:
        return self.read_logged(channels=channels, start=start, end=end, timeout=timeout, unique=unique)

    def read_logged(
        self,
        channels: Optional[ChannelsArgument] = None,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
        unique: bool = False,
    ) -> pd.DataFrame:
        channels = self._filter_by_args(channels)
        data = self.__context.read_logged(channels=channels, start=start, end=end, timeout=timeout)
        if not unique:
            data.rename(columns={c.id: c.key for c in channels}, inplace=True)
        return data

    def read(
        self,
        channels: Optional[ChannelsArgument] = None,
        timeout: Optional[float] = None,
        unique: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        channels = self._filter_by_args(channels)
        data = self.__context.read(channels=channels, timeout=timeout, **kwargs)
        if not unique:
            data.rename(columns={c.id: c.key for c in channels}, inplace=True)
        return data

    def write(
        self,
        data: pd.DataFrame,
        channels: Optional[ChannelsArgument] = None,
        timeout: Optional[float] = None,
    ) -> None:
        if data is None:
            raise ResourceError(f"Invalid data to write '{self.id}': {data}")
        data.rename(columns={c.key: c.id for c in channels}, inplace=True)
        channels = self._filter_by_args(channels)
        self.__context.write(data, channels=channels, timeout=timeout)
