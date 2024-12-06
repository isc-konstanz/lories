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
from typing import Literal

import pandas as pd
from lori.core import Identifier, ResourceException
from lori.data import Channel, Channels


# noinspection PyShadowingBuiltins
class Listener(Identifier):
    __lock: Lock
    _logger: Logger

    _how: str
    _unique: bool

    _function: Callable[[pd.DataFrame], None]
    channels: Channels

    timestamp: pd.Timestamp = pd.NaT

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
        try:
            self.__lock.acquire()
            self.run()

        except Exception as e:
            raise ListenerException(self, repr(e))
        finally:
            self.timestamp = timestamp
            self.__lock.release()
        return self

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


class ListenerException(ResourceException):
    """
    Raise if an error occurred notifying the listener.

    """

    # noinspection PyArgumentList
    def __init__(self, listener: Listener, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.listener = listener
