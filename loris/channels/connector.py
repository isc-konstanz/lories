# -*- coding: utf-8 -*-
"""
    loris.core.channels.channel
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations

from collections import OrderedDict
from typing import Any

import pandas as pd
from loris.util import parse_id


class ChannelConnector:
    _uuid: str
    _id: str

    _configs: OrderedDict[str, Any]

    timestamp: pd.Timestamp = pd.NaT

    # noinspection PyShadowingBuiltins
    def __init__(self, connector: str = None, **configs: Any) -> None:
        self._uuid = connector
        self._id = parse_id(connector.split(".")[-1]) if connector is not None else None
        self._configs = OrderedDict(configs)

    def __repr__(self) -> str:
        return (
            "ChannelConnector:\n\t" +
            f"\n\tid: {self.id}" +
            "\n\t".join(f"{key}: {str(val)}" for key, val in self._configs.items())
        )

    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        configs = ChannelConnector.__getattribute__(self, "_configs")
        if attr in configs.keys():
            return configs[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no configuration '{attr}'")

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def id(self) -> str:
        return self._id

    def copy(self) -> ChannelConnector:
        return ChannelConnector(self._uuid, **self._configs)
