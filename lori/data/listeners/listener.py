# -*- coding: utf-8 -*-
"""
lori.data.listener
~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from collections.abc import Callable
from logging import Logger
from threading import Lock
from typing import Optional, TypeVar

import pandas as pd
import pytz as tz
from lori._core._channel import Channel  # noqa
from lori._core._channels import Channels  # noqa
from lori._core._listener import _Listener  # noqa
from lori.core import ResourceError

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


# noinspection PyShadowingBuiltins
class Listener(_Listener):
    __lock: Lock
    _logger: Logger

    _how: str
    _unique: bool

    _function: Callable[[pd.DataFrame], None]
    channels: Channels

    __start: pd.Timestamp = pd.NaT
    __complete: pd.Timestamp = pd.NaT

    def __init__(
        self,
        id: str,
        key: str,
        function: Callable[[pd.DataFrame], None],
        channels: Channels,
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> None:
        super().__init__(id=id, key=key)
        self.__lock = Lock()
        self._logger = logging.getLogger(self.__module__)

        self._how = how
        self._unique = unique
        self._function = function
        self.channels = channels

    def __call__(self, timestamp: pd.Timestamp) -> Listener:
        self.__start = pd.Timestamp.now(tz=tz.UTC)
        try:
            self.__lock.acquire()
            self.run()

        except Exception as e:
            raise ListenerError(self, str(e))
        finally:
            self.__complete = timestamp
            self.__lock.release()
            self._logger.debug(f"Listener '{self.id}' finished after {round(self.runtime, 3)} seconds")
            return self

    @property
    def timestamp(self) -> pd.Timestamp | pd.NaT:
        return self.__complete

    @property
    def runtime(self) -> Optional[float]:
        if pd.isna(self.__start):
            return None
        return (pd.Timestamp.now(tz=tz.UTC) - self.__start).total_seconds()

    def run(self) -> None:
        data = self.channels.to_frame(unique=self._unique)
        self._function(data)

    def locked(self) -> bool:
        return self.__lock.locked()

    def has_update(self) -> bool:
        def _has_update(channel: Channel) -> bool:
            return channel.is_valid() and (pd.isna(self.timestamp) or self.timestamp < channel.timestamp)

        if self._how == "any":
            return any(_has_update(c) for c in self.channels)
        elif self._how == "all":
            return all(_has_update(c) for c in self.channels)
        return False


class ListenerError(ResourceError):
    """
    Raise if an error occurred notifying the listener.

    """

    # noinspection PyArgumentList
    def __init__(self, listener: ListenerType, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.listener = listener


ListenerType = TypeVar("ListenerType", bound=Listener)
