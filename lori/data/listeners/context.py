# -*- coding: utf-8 -*-
"""
lori.data.listeners.context
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from collections.abc import Callable
from threading import Lock
from time import sleep
from typing import Collection, Literal

import pandas as pd
from lori.core import Context, ResourceException
from lori.data import Channel, Channels
from lori.data.listeners import Listener


# noinspection PyShadowingBuiltins
class ListenerContext(Context[Listener]):
    __context: Context
    __lock: Lock

    def __init__(self, context: Context, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__context = self._assert_context(context)
        self._logger = logging.getLogger(self.__module__)

        self.__lock = Lock()

    def __enter__(self) -> Listener:
        self.__lock.acquire()
        return self

    def __exit__(self, type, value, traceback):
        self.__lock.release()

    @classmethod
    def _assert_context(cls, context: Context) -> Context:
        from lori.data.manager import DataManager

        if context is None or not isinstance(context, DataManager):
            raise ResourceException(f"Invalid '{cls.__name__}' context: {type(context)}")
        return context

    @property
    def context(self) -> Context:
        return self.__context

    # noinspection PyUnresolvedReferences, PyProtectedMember
    def register(
        self,
        function: Callable[[pd.DataFrame], None],
        channels: Channels,
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> None:
        key = function.__name__
        try:
            context = function.__self__.id
        except AttributeError:
            context = function.__module__
        id = f"{context}.{key}"
        if id in self:
            listener = self[id]
            if listener._how != how:
                raise ResourceException(
                    f"Trying to register '{how}' updated listener to existing '{listener._how}' instance"
                )
            if listener._unique != unique:
                raise ResourceException(
                    f"Trying to register '{unique}' processed listener to existing '{listener._unique}' instance"
                )
            listener.channels.extend(channels)
        else:
            self._add(Listener(id, key, function, channels, how=how, unique=unique))

    def notify(self, timestamp: pd.Timestamp, *channels: Channel) -> Collection[Listener]:
        listeners = []
        for id, listener in self.items():
            ids = listener.channels.ids
            if any(c.id in ids for c in channels) and listener.has_update():
                if listener.timestamp >= timestamp:
                    self._logger.warning(f"Listener '{listener.id}' already started at: {timestamp}")
                    continue
                if listener.locked():
                    self._logger.warning(
                        f"Listener '{listener.id}' not finished yet. Please verify your configurations"
                    )

                listener.timestamp = timestamp
                listeners.append(listener)
        return listeners

    def wait(self) -> None:
        # noinspection PyShadowingNames
        def has_locked() -> bool:
            locked = [listener.locked() for listener in self.values()]

            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.debug(f"Waiting for {len(locked)} listener{'s' if len(locked) > 0 else ''} to finish")
            return any(locked)

        while has_locked():
            sleep(0.1)
