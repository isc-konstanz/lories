# -*- coding: utf-8 -*-
"""Seam 2 — descriptor edge cases: B2 (channel bool), B6 (ConnectType.get), B7 (bare group child) (issue 04)."""

from __future__ import annotations

import pytest

from lories._core._connector import ConnectType
from lories.core.configs.errors import ConfigurationError
from lories.core.configs.parameters import Parameter, ParameterGroup
from lories.core.configs.parameters.channel_parameter import ChannelParameter
from lories.util import to_bool

# ---------------------------------------------------------------- B2


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("true", True),
        ("yes", True),
        ("y", True),
        ("True", True),
        ("false", False),
        ("no", False),
        ("n", False),
        ("False", False),
    ],
)
def test_channel_parameter_bool_parity(raw, expected):
    cp = ChannelParameter(type=bool)
    assert cp._cast(raw) is expected
    assert cp._cast(raw) is to_bool(raw)  # parity with the shared helper


def test_channel_parameter_bool_passthrough():
    cp = ChannelParameter(type=bool)
    assert cp._cast(True) is True
    assert cp._cast(False) is False


def test_channel_parameter_rejects_non_bool_string():
    """'0' is no longer silently ``True`` (the ``any_str=True`` footgun)."""
    cp = ChannelParameter(type=bool)
    with pytest.raises(ConfigurationError):
        cp._cast("0")


# ---------------------------------------------------------------- B6


def test_connect_type_from_bool():
    assert ConnectType.get(True) is ConnectType.AUTO
    assert ConnectType.get(False) is ConnectType.NONE


@pytest.mark.parametrize("value", ["auto", "none", "true", "false", "1", "0"])
def test_connect_type_rejects_strings(value):
    """connect is bool-coercible only (BoolParameter/get_bool convert upstream); raw strings are rejected."""
    with pytest.raises(ValueError):
        ConnectType.get(value)


@pytest.mark.parametrize("value", [1, 0, None, 1.5])
def test_connect_type_rejects_non_bool(value):
    """Only a real ``bool`` is accepted — ints/None/floats are not silently coerced (catches a re-widened contract)."""
    with pytest.raises(ValueError):
        ConnectType.get(value)


# ---------------------------------------------------------------- B7


def test_group_child_with_key_ok():
    group = ParameterGroup(children=[Parameter(key="user", type=str)])
    assert "user" in group.children


def test_group_add_child_with_name_ok():
    group = ParameterGroup()
    group.add_child("bar", Parameter(type=str))
    assert "bar" in group.children


def test_group_rejects_bare_child():
    """A child with neither key nor name resolves to None and would collide; it is rejected."""
    with pytest.raises(ConfigurationError, match="neither"):
        ParameterGroup(children=[Parameter(type=str)])


def test_group_add_child_rejects_bare():
    """The bare-child guard fires on the ``add_child`` path too, not only at construction."""
    group = ParameterGroup()
    with pytest.raises(ConfigurationError, match="neither"):
        group.add_child(None, Parameter(type=str))
