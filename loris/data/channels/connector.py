# -*- coding: utf-8 -*-
"""
loris.data.channels.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any, Dict

import pandas as pd
from loris.util import parse_key


class ChannelConnector:
    _id: str
    _key: str

    __configs: OrderedDict[str, Any]

    timestamp: pd.Timestamp = pd.NaT

    # noinspection PyShadowingBuiltins
    def __init__(self, connector: str = None, **configs: Any) -> None:
        self._id = connector
        self._key = parse_key(connector.split(".")[-1]) if connector is not None else None
        self.__configs = OrderedDict(configs)

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        vars = OrderedDict(id=self.id, **self.__configs)
        vars["timestamp"] = self.timestamp
        return vars

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.key})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{k}={v}" for k, v in self._get_vars().items())

    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        configs = ChannelConnector.__getattribute__(self, f"_{ChannelConnector.__name__}__configs")
        if attr in configs.keys():
            return configs[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no configuration '{attr}'")

    @property
    def id(self) -> str:
        return self._id

    @property
    def key(self) -> str:
        return self._key

    def copy(self) -> ChannelConnector:
        return type(self)(**self._get_vars())
