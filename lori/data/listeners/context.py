# -*- coding: utf-8 -*-
"""
lori.data.listeners.context
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from threading import Lock
from typing import Collection, Optional

import pandas as pd
from lori.core import Context, ResourceException
from lori.data import Channel, Channels
from lori.data.listeners import Listener

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


# noinspection PyShadowingBuiltins
class ListenerContext(Context[Listener]):
    __context: Context
    __lock: Lock

    def __init__(self, context: Context, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__context = self._assert_context(context)
        self._logger = logging.getLogger(self.__module__)

        self.__lock = Lock()

    def __enter__(self) -> ListenerContext:
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

    def _create(
        self,
        id: str,
        key: str,
        function: Callable[[pd.DataFrame], None],
        channels: Channels,
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> Listener:
        return Listener(id, key, function, channels, how, unique)

    # noinspection PyShadowingBuiltins, PyProtectedMember
    def _update(
        self,
        id: str,
        channels: Channels,
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> None:
        listener = self._get(id)
        if listener._how != how:
            raise ResourceException(
                f"Trying to register '{how}' updated listener to existing '{listener._how}' instance"
            )
        if listener._unique != unique:
            raise ResourceException(
                f"Trying to register '{unique}' processed listener to existing '{listener._unique}' instance"
            )
        listener.channels.extend(channels)

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
        if self._contains(id):
            self._update(id, channels, how, unique)
        else:
            self._add(self._create(id, key, function, channels, how=how, unique=unique))

    def notify(self, *channels: Channel) -> Collection[Listener]:
        listeners = []
        for id, listener in self.items():
            ids = listener.channels.ids
            if any(c.id in ids for c in channels) and listener.has_update():
                if listener.locked():
                    self._logger.warning(
                        f"Listener '{listener.id}' not finished after {round(listener.runtime, 3)} seconds. "
                        f"Please verify your configurations"
                    )
                    listener.cancel()
                listeners.append(listener)
        return listeners

    def wait(self, timeout: Optional[float] = None, sleep: Callable = time.sleep) -> None:
        start = time.time()

        def is_timeout() -> bool:
            if timeout is None:
                return False
            return time.time() - start >= timeout

        # noinspection PyShadowingNames
        def has_locked() -> bool:
            locked = [listener.locked() for listener in self.values()]

            if self._logger.getEffectiveLevel() <= logging.DEBUG:
                self._logger.debug(f"Waiting for {len(locked)} listener{'s' if len(locked) > 0 else ''} to finish")
            return any(locked)

        while has_locked() and not is_timeout():
            sleep(0.01)
        if is_timeout():
            pass
