# -*- coding: utf-8 -*-
"""
tests.test_data_channel_scale
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A channel's ``scale`` must apply to a value a connector pushes through ``Channel.set``
exactly as it applies to a frame a polling connector reads (``from_series``); before this
was pinned, only the read path scaled and every push-based binding silently kept raw
units and signs.
"""

from __future__ import annotations

import os

import pytest

import pandas as pd
from lories.components import register_component_type
from lories.components.binding import BindableComponent
from lories.typing import Configurations

_SETTINGS_CONF = 'name = "scaletest"\naction = "run"\n\n[interface]\nenabled = false\n'
_SYSTEM_CONF = 'key = "sys"\nname = "Scale Test System"\n'


@register_component_type("scaletest")
class ScaleTestDevice(BindableComponent):
    def _add_channels(self, configs: Configurations) -> None:
        self._add_data("milli", type=float, scale=0.001)
        self._add_data("flipped", type=float, scale=-1)
        self._add_data("counter", type=int, scale=1000)
        self._add_data("plain", type=float)
        self._add_data("label", type=str)


@pytest.fixture
def device(tmp_path):
    import lories

    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    (conf_dir / "settings.conf").write_text(_SETTINGS_CONF)
    (conf_dir / "system.conf").write_text(_SYSTEM_CONF)
    (conf_dir / "scaletest.conf").write_text('type = "scaletest"\nname = "Scale Test"\n')

    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        app = lories.load("scaletest")
    finally:
        os.chdir(cwd)
    (device,) = [c for c in app.components.values() if isinstance(c, ScaleTestDevice)]
    return device


def test_pushed_value_is_scaled(device):
    now = pd.Timestamp.now(tz="UTC")
    device.data["milli"].set(now, 1500)
    device.data["flipped"].set(now, 769)
    device.data["counter"].set(now, 3)
    device.data["plain"].set(now, 1500)
    device.data["label"].set(now, "abc")

    assert device.data["milli"].value == pytest.approx(1.5)
    assert device.data["flipped"].value == -769
    assert device.data["counter"].value == 3000
    assert device.data["plain"].value == 1500
    assert device.data["label"].value == "abc"


def test_pushed_series_is_scaled(device):
    index = pd.date_range("2026-01-01", periods=2, freq="1s", tz="UTC")
    device.data["milli"].set(index[-1], pd.Series([1000.0, 2500.0], index=index))

    assert list(device.data["milli"].value) == pytest.approx([1.0, 2.5])


def test_push_and_read_paths_agree(device):
    channel = device.data["milli"]
    index = pd.date_range("2026-01-01", periods=3, freq="1s", tz="UTC")
    raw = pd.Series([1000.0, 1500.0, 2000.0], index=index, name=channel.id)

    read_path = channel.converter._converter.from_series(raw, channel)
    push_path = channel.converter(raw)

    assert list(push_path) == pytest.approx([1.0, 1.5, 2.0])
    assert list(read_path) == pytest.approx(list(push_path))
