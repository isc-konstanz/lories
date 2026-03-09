# -*- coding: utf-8 -*-
"""
lories.connectors.math
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, List, Optional

import sympy

import pandas as pd
import pytz as tz
from lories.connectors import Connector, register_connector_type
from lories.core import ConfigurationError
from lories.data import Channel
from lories.typing import Configurations, Resources


@register_connector_type("math")
class MathConnector(Connector):
    _symbols: List[InputSymbol]
    _expr: sympy.Expr

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._symbols = []

        math = configs.get("math")
        try:
            expr = sympy.sympify(math)
            expr = sympy.simplify(expr)
            self._expr = expr

        except Exception as e:
            raise ConfigurationError(f"Error parsing math expression: {math}: {e}")

        mapping = configs.get("mapping", {})
        for symbol in expr.free_symbols:
            input_symbol = InputSymbol(symbol)
            if symbol.name in mapping:
                input_symbol.update_id(mapping[symbol.name])
                mapping.pop(symbol.name)
            self._symbols.append(input_symbol)

        for unmapped in mapping.items():
            raise ConfigurationError(f"Mapping for unknown symbol '{unmapped}'")

    def connect(self, resources: Resources) -> None:
        # TODO: Is filtering necessary?
        # channels = resources.filter(lambda r: isinstance(r, Channel))
        # resources is only the output resources (channels) assigned to this connector,
        # which receive the computed values

        for symbol in self._symbols:
            channel = self._find_channel(symbol.channel_id)
            if channel is None:
                raise ConfigurationError(f"Channel '{symbol.channel_id}' for symbol '{symbol.name}' not found.")
            symbol.update_channel(channel)
            symbol.update_value()
            self.context.context.data.register(symbol, channel, how="any", unique=False)
            # Todo: call _evaluate when symbol channel is updated

        self._evaluate()

    def _find_channel(self, channel_id: str) -> Optional[Channel]:
        if "." in channel_id:
            # TODO: Find globally by id
            raise NotImplementedError("Finding channels by global id is not implemented yet.")
        else:
            # TODO: Not tested yet
            component = self.context.context
            return component.data.get(channel_id)

    def _evaluate(self) -> None:
        timestamp = pd.Timestamp.now(tz=tz.UTC).floor(freq="s")
        local_dict = {symbol.name: symbol.value for symbol in self._symbols}
        if any(value is None for value in local_dict.values()):
            return

        result = self._expr.evalf(subs=local_dict)
        for channel in self.resources.filter(lambda r: isinstance(r, Channel)):
            channel.set(timestamp, result)

    def disconnect(self) -> None:
        for symbol in self._symbols:
            self.context.context.data.unregister(symbol)
            del symbol.channel

    def read(self, resources: Resources) -> pd.DataFrame:
        # TODO: Implement reading data non-reactingly (pull mode)
        pass

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("Math connector does not support writing data")


class InputSymbol:
    name: str
    channel_id: str
    channel: Channel
    value: Any

    def __init__(self, symbol: sympy.Symbol) -> None:
        self.name = symbol.name
        self.channel_id = self.name
        self.channel = None
        self.value = None

    def __call__(self, data: pd.DataFrame) -> None:
        self.value = data.at[data.index[-1], self.channel.id]

    def update_id(self, channel_id: str) -> None:
        self.channel_id = channel_id

    def update_channel(self, channel: Channel) -> None:
        self.channel = channel

    def update_value(self) -> None:
        if self.channel is not None:
            self.value = self.channel.value
