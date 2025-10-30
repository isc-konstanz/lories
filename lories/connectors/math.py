# -*- coding: utf-8 -*-
"""
lories.connectors.math
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Dict, Optional, Any, Callable

import sympy

import pandas as pd
import pytz as tz
from lories._core import ChannelState  # noqa
from lories.connectors import Connector, register_connector_type
from lories.core import ConfigurationError
from lories.data import Channel
from lories.typing import Configurations, Resources


@register_connector_type("math")
class MathConnector(Connector):

    _connected: bool

    _channels: Dict[str, Channel]
    _expr: sympy.Expr

    _values: Dict[str, Any]


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._connected = False

        self._channels = configs.get("channels", {})
        math = configs.get("math", "")
        if not self._channels or math == "":
            raise ConfigurationError("MathConnector requires 'channels' and 'math' configurations.")

        try:
            expr = sympy.sympify(math)
            expr = sympy.simplify(expr)
            self._expr = expr
        except Exception as e:
            raise ConfigurationError(f"Error parsing math expression: {e}")

        symbols = sorted(expr.free_symbols, key=lambda s: s.name)
        symbol_names = [s.name for s in symbols]
        channel_aliases = list(self._channels.keys())
        if sorted(symbol_names) != sorted(channel_aliases):
            raise ConfigurationError(
                f"Symbols in math expression {symbol_names} do not match channel aliases {channel_aliases}."
            )

    def is_connected(self) -> bool:
        return self._connected

    def connect(self, resources: Resources) -> None:
        # Todo: is filtering necessary?
        channels = resources.filter(lambda r: isinstance(r, Channel))
        channel_ids = [ch.id for ch in channels]
        conf_channel_ids = [list(self._channels.values())]

        if sorted(channel_ids) != sorted(conf_channel_ids):
            raise ConfigurationError(
                f"Resources provided {channel_ids} do not match configured channels {conf_channel_ids}."
            )

        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def read(self, resources: Resources) -> pd.DataFrame:
        pass

    def write(self, data: pd.DataFrame) -> None:
        pass

    def _update_values(self, data: pd.DataFrame) -> None:
        pass

    def _eval(self):
        pass
