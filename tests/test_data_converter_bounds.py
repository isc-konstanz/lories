# -*- coding: utf-8 -*-
"""
tests.test_data_converter_bounds
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Numeric channels (``int``, ``float`` and therefore ``linear``) take ``min``/``max`` plausibility
bounds in their converter table. Out of range is rejected by default: the value becomes NaN, a
read frame leaves the channel NOT_AVAILABLE and a pushed value sets that state. ``clamp = true``
clips to the bound instead. Bounds apply to the converted value in the channel's unit, i.e.
after a ``linear`` scale and offset.
"""

from __future__ import annotations

import math
import os
from textwrap import dedent

import pytest

import numpy as np
import pandas as pd
from lories.components import Component, register_component_type
from lories.core import ConfigurationError
from lories.data.channels import ChannelState

_SETTINGS_CONF = 'name = "boundstest"\naction = "run"\n\n[interface]\nenabled = false\n'
_SYSTEM_CONF = 'key = "sys"\nname = "Bounds Test System"\n'


@register_component_type("boundstest")
class BoundsTestDevice(Component):
    pass


def _load(tmp_path, channels: str, converters: dict[str, str] | None = None):
    import lories

    conf_dir = tmp_path / "conf"
    conf_dir.mkdir(exist_ok=True)
    (conf_dir / "settings.conf").write_text(_SETTINGS_CONF)
    (conf_dir / "system.conf").write_text(_SYSTEM_CONF)
    (conf_dir / "boundstest.conf").write_text('type = "boundstest"\nname = "Bounds Test"\n\n' + dedent(channels))
    if converters:
        converters_dir = conf_dir / "converters.d"
        converters_dir.mkdir(exist_ok=True)
        for key, body in converters.items():
            (converters_dir / f"{key}.conf").write_text(dedent(body))

    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        app = lories.load("boundstest")
    finally:
        os.chdir(cwd)
    (device,) = [c for c in app.components.values() if isinstance(c, BoundsTestDevice)]
    return device


def _read_frame(device, values: dict[str, float]) -> None:
    now = pd.Timestamp.now(tz="UTC")
    frame = pd.DataFrame({device.data[k].id: [v] for k, v in values.items()}, index=[now])
    device.data.channels.set_frame(frame)


_CHANNELS = """
[data.channels.temp]
type = "float"
converter = { min = -40.0, max = 80.0 }

[data.channels.temp_clamped]
type = "float"
converter = { min = -40.0, max = 80.0, clamp = true }

[data.channels.count]
type = "int"
converter = { min = 0, max = 100 }

[data.channels.count_clamped]
type = "int"
converter = { min = 0, max = 100, clamp = true }

[data.channels.scaled]
type = "float"
converter = { type = "linear", scale = 0.1, offset = -273.15, min = -40.0, max = 80.0 }
"""


def test_read_frame_rejects_out_of_range_and_keeps_in_range(tmp_path):
    device = _load(tmp_path, _CHANNELS)
    _read_frame(device, {"temp": 21.5, "count": 7, "scaled": 2981.5})
    assert device.data["temp"].value == pytest.approx(21.5)
    assert device.data["count"].value == 7
    assert device.data["scaled"].value == pytest.approx(25.0)

    _read_frame(device, {"temp": -999.0, "count": 65535, "scaled": 9999.0})
    assert device.data["temp"].state == ChannelState.NOT_AVAILABLE
    assert device.data["count"].state == ChannelState.NOT_AVAILABLE
    assert device.data["scaled"].state == ChannelState.NOT_AVAILABLE


def test_read_frame_clamps_when_flag_is_set(tmp_path):
    device = _load(tmp_path, _CHANNELS)
    _read_frame(device, {"temp_clamped": -999.0, "count_clamped": 65535})

    assert device.data["temp_clamped"].value == pytest.approx(-40.0)
    assert device.data["temp_clamped"].state == ChannelState.VALID
    assert device.data["count_clamped"].value == 100
    assert isinstance(device.data["count_clamped"].value, (int, np.integer))


def test_bounds_apply_after_linear_conversion(tmp_path):
    # 2981.5 raw -> 25.0 after scale/offset: inside; raw 25.0 would be -270.65: outside.
    device = _load(tmp_path, _CHANNELS)
    _read_frame(device, {"scaled": 25.0})
    assert device.data["scaled"].state == ChannelState.NOT_AVAILABLE


def test_pushed_value_out_of_range_sets_not_available(tmp_path):
    device = _load(tmp_path, _CHANNELS)
    now = pd.Timestamp.now(tz="UTC")

    device.data["temp"].set(now, 21.5)
    assert device.data["temp"].value == pytest.approx(21.5)
    assert device.data["temp"].state == ChannelState.VALID

    device.data["temp"].set(now, 500.0)
    assert device.data["temp"].state == ChannelState.NOT_AVAILABLE

    device.data["temp_clamped"].set(now, 500.0)
    assert device.data["temp_clamped"].value == pytest.approx(80.0)
    assert device.data["temp_clamped"].state == ChannelState.VALID


def test_pushed_series_marks_rejected_elements_nan(tmp_path):
    device = _load(tmp_path, _CHANNELS)
    index = pd.date_range("2026-01-01", periods=3, freq="1s", tz="UTC")
    device.data["temp"].set(index[-1], pd.Series([10.0, 500.0, 20.0], index=index))

    values = list(device.data["temp"].value)
    assert values[0] == pytest.approx(10.0)
    assert math.isnan(values[1])
    assert values[2] == pytest.approx(20.0)


def test_channel_defaults_and_named_instance_bounds(tmp_path):
    device = _load(
        tmp_path,
        """
        [data.channels]
        type = "float"
        converter = { min = 0.0 }

        [data.channels.inherited]
        name = "default lower bound"

        [data.channels.percent]
        converter = "percent"

        [data.channels.percent_rejecting]
        converter = { type = "percent", clamp = false }
        """,
        converters={"percent": 'type = "linear"\nscale = 100\nmin = 0\nmax = 100\nclamp = true\n'},
    )
    _read_frame(device, {"inherited": -1.0, "percent": 1.5, "percent_rejecting": 1.5})

    assert device.data["inherited"].state == ChannelState.NOT_AVAILABLE
    assert device.data["percent"].value == pytest.approx(100.0)
    assert device.data["percent_rejecting"].state == ChannelState.NOT_AVAILABLE


def test_string_channel_does_not_accept_bounds(tmp_path):
    with pytest.raises(ConfigurationError, match=r"Unknown converter argument\(s\) \['min'\]"):
        _load(
            tmp_path,
            """
            [data.channels.label]
            type = "str"
            converter = { min = 0 }
            """,
        )


def test_instance_min_above_max_fails_at_load(tmp_path):
    with pytest.raises(ConfigurationError, match="min 10.0 > max 5.0"):
        _load(
            tmp_path,
            """
            [data.channels.x]
            type = "float"
            converter = "bad"
            """,
            converters={"bad": 'type = "linear"\nmin = 10\nmax = 5\n'},
        )
