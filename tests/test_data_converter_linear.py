# -*- coding: utf-8 -*-
"""
tests.test_data_converter_linear
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``linear`` converter (``y = scale * x + offset``) is the one way to scale a channel's
values. Before it existed, ``converter = { type = "linear", scale = ... }`` was silently
accepted and ignored (the selector key was ``converter``, unknown keys were swallowed), so
84 deployed channels reported raw register values. These tests pin the converter, the
``type`` alias, the loud failures that replace the silence, and read/push agreement.
"""

from __future__ import annotations

import os
from textwrap import dedent

import pytest

import pandas as pd
from lories.components import Component, register_component_type
from lories.core import ConfigurationError

_SETTINGS_CONF = 'name = "lineartest"\naction = "run"\n\n[interface]\nenabled = false\n'
_SYSTEM_CONF = 'key = "sys"\nname = "Linear Test System"\n'


@register_component_type("lineartest")
class LinearTestDevice(Component):
    pass


def _load(tmp_path, channels: str, converters: dict[str, str] | None = None):
    import lories

    conf_dir = tmp_path / "conf"
    conf_dir.mkdir(exist_ok=True)
    (conf_dir / "settings.conf").write_text(_SETTINGS_CONF)
    (conf_dir / "system.conf").write_text(_SYSTEM_CONF)
    (conf_dir / "lineartest.conf").write_text('type = "lineartest"\nname = "Linear Test"\n\n' + dedent(channels))
    if converters:
        converters_dir = conf_dir / "converters.d"
        converters_dir.mkdir(exist_ok=True)
        for key, body in converters.items():
            (converters_dir / f"{key}.conf").write_text(dedent(body))

    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        app = lories.load("lineartest")
    finally:
        os.chdir(cwd)
    (device,) = [c for c in app.components.values() if isinstance(c, LinearTestDevice)]
    return device


def _read_frame(device, values: dict[str, float]) -> None:
    now = pd.Timestamp.now(tz="UTC")
    frame = pd.DataFrame({device.data[k].id: [v] for k, v in values.items()}, index=[now])
    device.data.channels.set_frame(frame)


def test_read_frame_applies_scale_and_offset(tmp_path):
    device = _load(
        tmp_path,
        """
        [data.channels.power]
        type = "float"
        converter = { type = "linear", scale = -1000.0 }

        [data.channels.temp]
        type = "float"
        converter = { type = "linear", scale = 0.1, offset = -273.15 }

        [data.channels.plain]
        type = "float"
        """,
    )
    _read_frame(device, {"power": 1.5, "temp": 2981.5, "plain": 1.5})

    assert device.data["power"].value == pytest.approx(-1500.0)
    assert device.data["temp"].value == pytest.approx(25.0)
    assert device.data["plain"].value == pytest.approx(1.5)


def test_pushed_value_and_series_agree_with_read(tmp_path):
    device = _load(
        tmp_path,
        """
        [data.channels.power]
        type = "float"
        converter = { type = "linear", scale = -1000.0, offset = 5.0 }
        """,
    )
    channel = device.data["power"]
    now = pd.Timestamp.now(tz="UTC")

    channel.set(now, 1.5)
    assert channel.value == pytest.approx(-1495.0)

    index = pd.date_range("2026-01-01", periods=2, freq="1s", tz="UTC")
    channel.set(index[-1], pd.Series([1.0, 2.0], index=index))
    assert list(channel.value) == pytest.approx([-995.0, -1995.0])

    _read_frame(device, {"power": 1.5})
    assert channel.value == pytest.approx(-1495.0)


def test_converter_key_and_type_alias_are_equivalent(tmp_path):
    device = _load(
        tmp_path,
        """
        [data.channels.by_type]
        type = "float"
        converter = { type = "linear", scale = 2.0 }

        [data.channels.by_converter]
        type = "float"
        converter = { converter = "linear", scale = 2.0 }
        """,
    )
    _read_frame(device, {"by_type": 3.0, "by_converter": 3.0})

    assert device.data["by_type"].value == pytest.approx(6.0)
    assert device.data["by_converter"].value == pytest.approx(6.0)
    # The alias is normalised away: a channel round-trips to the canonical key only.
    assert device.data["by_type"].converter.to_configs() == {"converter": "linear", "scale": 2.0, "enabled": True}


def test_channel_defaults_carry_the_converter(tmp_path):
    device = _load(
        tmp_path,
        """
        [data.channels]
        type = "float"
        converter = { type = "linear", scale = 0.1 }

        [data.channels.inherited]
        name = "uses the default"

        [data.channels.overridden]
        converter = { type = "linear", scale = 0.01 }
        """,
    )
    _read_frame(device, {"inherited": 100.0, "overridden": 100.0})

    assert device.data["inherited"].value == pytest.approx(10.0)
    assert device.data["overridden"].value == pytest.approx(1.0)


def test_int_channel_opts_out_of_linear_default_with_string_form(tmp_path):
    # A table override merges into the default table and would inherit `scale`; the string
    # form replaces the default wholesale, which is how an int channel escapes a float default.
    device = _load(
        tmp_path,
        """
        [data.channels]
        type = "float"
        converter = { type = "linear", scale = 0.1 }

        [data.channels.status]
        type = "int"
        converter = "int"
        """,
    )
    _read_frame(device, {"status": 3})

    assert device.data["status"].converter.to_configs() == {"converter": "int", "enabled": True}
    assert device.data["status"].value == 3


def test_named_instance_defaults_and_channel_override(tmp_path):
    device = _load(
        tmp_path,
        """
        [data.channels.kilo]
        type = "float"
        converter = "kilo"

        [data.channels.kilo_shifted]
        type = "float"
        converter = { type = "kilo", offset = 5.0 }
        """,
        converters={"kilo": 'type = "linear"\nscale = 1000\n'},
    )
    _read_frame(device, {"kilo": 1.5, "kilo_shifted": 1.5})

    assert device.data["kilo"].value == pytest.approx(1500.0)
    assert device.data["kilo_shifted"].value == pytest.approx(1505.0)


def test_float_channel_without_converter_is_not_routed_through_linear(tmp_path):
    device = _load(
        tmp_path,
        """
        [data.channels.plain]
        type = "float"
        converter = { decimals = 1 }
        """,
    )
    assert device.data["plain"].converter.key == "float"
    _read_frame(device, {"plain": 1.26})
    assert device.data["plain"].value == pytest.approx(1.3)


def test_unknown_converter_argument_fails_at_load(tmp_path):
    with pytest.raises(
        ConfigurationError,
        match=r"Unknown converter argument\(s\) \['scale'\].*'float' accepts: min, max, clamp, decimals",
    ):
        _load(
            tmp_path,
            """
            [data.channels.power]
            type = "float"
            converter = { type = "float", scale = -1000.0 }
            """,
        )


def test_unknown_converter_type_fails_at_load(tmp_path):
    with pytest.raises(ConfigurationError, match="Unknown converter 'quadratic'"):
        _load(
            tmp_path,
            """
            [data.channels.power]
            type = "float"
            converter = { type = "quadratic" }
            """,
        )


def test_conflicting_selector_keys_fail_at_load(tmp_path):
    with pytest.raises(ConfigurationError, match="Conflicting converter selection"):
        _load(
            tmp_path,
            """
            [data.channels.power]
            type = "float"
            converter = { type = "linear", converter = "float" }
            """,
        )


def test_linear_on_int_channel_fails_at_load(tmp_path):
    with pytest.raises(ConfigurationError, match="yields float values.*declared as type 'int'"):
        _load(
            tmp_path,
            """
            [data.channels.counter]
            type = "int"
            converter = { type = "linear", scale = 0.1 }
            """,
        )


def test_removed_channel_scale_key_fails_with_replacement(tmp_path):
    with pytest.raises(
        ConfigurationError, match=r"removed 'scale' key.*converter = \{ type = \"linear\", scale = 3.6 \}"
    ):
        _load(
            tmp_path,
            """
            [data.channels.wind_speed]
            type = "float"
            scale = 3.6
            """,
        )
