# -*- coding: utf-8 -*-
"""
lories.core.configs.parameters.parameter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``Parameter`` factory — returns the appropriate typed subclass based on the
*type* argument.  ``lories.util.parse_type`` is used so that string type names
(e.g. ``"int"``, ``"pandas.Timestamp"``) are accepted in addition to plain
Python types.
"""

from __future__ import annotations

import typing as _typing
from typing import Any, Callable, List, Optional, Type

import pandas as pd
from lories.core.configs.parameters.base import _UNSET, _TypedParameter  # noqa: F401
from lories.core.configs.parameters.bool import BoolParameter
from lories.core.configs.parameters.custom import CustomParameter
from lories.core.configs.parameters.date import DateParameter
from lories.core.configs.parameters.float import FloatParameter
from lories.core.configs.parameters.integer import IntParameter
from lories.core.configs.parameters.list import ListParameter  # noqa: F401
from lories.core.configs.parameters.numeric import _NumericParameter  # noqa: F401
from lories.core.configs.parameters.select import SelectParameter  # noqa: F401
from lories.core.configs.parameters.string import StringParameter
from lories.util import parse_type as _parse_type

_TYPE_MAP: dict = {
    str: StringParameter,
    int: IntParameter,
    float: FloatParameter,
    bool: BoolParameter,
    pd.Timestamp: DateParameter,
}

_COMMON = ("key", "default", "required", "desc", "choices", "validator")


def Parameter(
    key: str | None = None,
    type: Type | str | None = None,  # noqa: A002
    default: Any = _UNSET,
    required: bool | None = None,
    desc: str | None = None,
    choices: Optional[List[Any]] = None,
    validator: Optional[Callable[[Any], None]] = None,
    min: Optional[Any] = None,  # noqa: A002
    max: Optional[Any] = None,  # noqa: A002
    **kwargs,
) -> _TypedParameter:
    """Return a typed ``_TypedParameter`` subclass appropriate for *type*.

    Scalar types::

        host  = Parameter(type=str,  default="localhost", desc="Broker hostname")
        port  = Parameter(type=int,  default=1883, min=1, max=65535)
        ratio = Parameter(type=float, default=1.0, min=0.0, max=1.0)
        tls   = Parameter(type=bool, default=False, desc="Use TLS")
        since = Parameter(type=pd.Timestamp, desc="Start date")

    List types — pass a ``List[…]`` generic alias as *type*::

        ports = Parameter(type=List[int], default=[1883], desc="Broker ports")
        tags  = Parameter(type=List[str], desc="Resource tags")

    String type names accepted by ``lories.util.parse_type`` also work::

        port  = Parameter(type="int",              default=1883)
        since = Parameter(type="pandas.Timestamp", desc="Start date")

    An unknown (custom) type falls back to :class:`CustomParameter`::

        mode = Parameter(type=MyEnum, desc="Operating mode")

    Parameters
    ----------
    type:
        Expected Python type, a ``List[T]`` generic alias, or a dotted-string
        type name.  ``None`` / omitted defaults to :class:`StringParameter`.
    min:
        Inclusive lower bound.  Only used for ``int`` / ``float`` types.
    max:
        Inclusive upper bound.  Only used for ``int`` / ``float`` types.
    """
    _common: dict = dict(
        key=key,
        default=default,
        required=required,
        desc=desc,
        choices=choices,
        validator=validator,
    )

    if isinstance(type, str):
        type = _parse_type(type)  # noqa: A001

    origin = _typing.get_origin(type)
    if origin is list:
        args = _typing.get_args(type)
        item_type = args[0] if args else str
        return ListParameter(item_type=item_type, **_common, **kwargs)

    cls = _TYPE_MAP.get(type)

    if cls in (IntParameter, FloatParameter):
        return cls(**_common, min=min, max=max, **kwargs)

    if cls is not None:
        return cls(**_common, **kwargs)

    if type is None:
        return StringParameter(**_common, **kwargs)

    return CustomParameter(type=type, **_common, **kwargs)
