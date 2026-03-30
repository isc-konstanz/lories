# -*- coding: utf-8 -*-
"""
lories.connectors.math
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from typing import Any, Dict, List, Optional

import sympy

import pandas as pd
import pytz as tz
from lories.connectors import Connector, register_connector_type
from lories.core import ConfigurationError
from lories.core.configs.parameters import ParameterGroup, ResourceParameter
from lories.data import Channel, DataContext
from lories.typing import Resource, Resources
from lories.util import get_context, to_bool


# noinspection SpellCheckingInspection
@register_connector_type("math")
class MathConnector(Connector):
    """
    The Math connector evaluates symbolic mathematical expressions at runtime using the SymPy computer algebra
    library. It maps free symbols in an expression to live channel values, enabling computed or derived channels
    such as unit conversions, aggregations, or physical formulas. Expressions can be triggered reactively via
    listeners or evaluated on demand during read cycles. However, complex expressions with many symbols may
    introduce noticeable computation overhead.
    """

    _mapping = ParameterGroup(key="mapping", required=False, desc="Symbol-to-channel mapping")

    # Per-channel parameters
    expression = ResourceParameter(
        type=str, required=False, desc="Math expression (sympy syntax); also accepted as 'expr' or 'math'"
    )
    mapping = ResourceParameter(
        type=None, required=False, default={}, desc="Per-channel symbol-to-channel mapping overrides"
    )
    listen = ResourceParameter(type=str, required=False, desc="Channel ID whose updates trigger re-evaluation")
    listener = ResourceParameter(
        type=bool, required=False, desc="Register as listener (default: True when 'listen' is set)"
    )

    _exprs: Dict[str, ChannelExpr]

    def connect(self, resources: Resources) -> None:
        self._exprs = {}

        mapping = self.configs.get("mapping", default={})
        for resource in resources:
            resource_mappings = deepcopy(mapping)
            resource_mappings.update(resource.get("mapping", default={}))

            self._exprs[resource.id] = self._build_expr(resource, **resource_mappings)

    # noinspection PyTypeChecker, PyUnresolvedReferences
    def _build_expr(self, resource: Resource, **mappings: str) -> ChannelExpr:
        data = get_context(self.context, DataContext)

        expression = resource.get("expression", None)
        try:
            expr = sympy.sympify(expression)
            expr = sympy.simplify(expr)

            channels = []
            channel_symbols = []
            for symbol in expr.free_symbols:
                channel_id = mappings.get(symbol.name, symbol.name)
                if "." not in channel_id:
                    channel_id = ".".join((*resource.path[:-1], channel_id))

                channel = data.get(channel_id)
                if channel is None:
                    raise ConfigurationError(f"Channel '{channel_id}' for symbol '{symbol.name}' not found.")
                channels.append(channel)
                channel_symbols.append(ChannelSymbol(symbol, channel))
            channel_expr = ChannelExpr(expr, resource, channel_symbols)

            listen = resource.get("listen", default=None)
            if to_bool(resource.get("listener", default=listen is not None)):
                data.register(channel_expr, channels, how=listen, unique=True)

            return channel_expr

        except Exception as e:
            raise ConfigurationError(f"Error parsing math expression for channel '{resource.id}': {expression}: {e}")

    def disconnect(self) -> None:
        for expr in self._exprs.keys():
            self.context.context.unregister(self._exprs.pop(expr))

    def read(self, resources: Resources) -> pd.DataFrame:
        timestamp = pd.Timestamp.now(tz.UTC).floor(freq="s")
        columns = []
        data = []
        for resource in resources:
            columns.append(resource.id)
            data.append(self._exprs[resource.id].evaluate())

        return pd.DataFrame(index=[timestamp], data=data, columns=columns)

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("Math connector does not support writing data")


class ChannelExpr(Callable):
    _expr: sympy.Expr
    _channel: Channel

    symbols: List[ChannelSymbol]

    def __init__(self, expr: sympy.Expr, channel: Channel, symbols: List[ChannelSymbol]) -> None:
        self._expr = expr
        self._channel = channel
        self.symbols = symbols

    @property
    def __self__(self) -> Channel:
        return self._channel

    @property
    def __name__(self) -> str:
        return "expr"

    # noinspection PyTypeChecker
    def __call__(self, data: pd.DataFrame) -> None:
        timestamp = data.index[-1]
        result = self.evaluate()
        if result is None:
            return

        self._channel.set(timestamp, result)

    def evaluate(self) -> Optional[float]:
        if not all(symbol.is_valid() for symbol in self.symbols):
            return None

        return float(self._expr.evalf(subs={symbol.name: symbol.value for symbol in self.symbols}))


class ChannelSymbol:
    _channel: Channel
    _symbol: sympy.Symbol

    def __init__(self, symbol: sympy.Symbol, channel: Channel) -> None:
        self._channel = channel
        self._symbol = symbol

    @property
    def name(self) -> Any:
        return self._symbol.name

    @property
    def value(self) -> Any:
        return self._channel.value

    def is_valid(self) -> bool:
        return self._channel.is_valid()
