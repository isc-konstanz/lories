# -*- coding: utf-8 -*-
"""Seam 3 — duration utilities: ms round-trip + B8 (get_duration rejects bare ints clearly) (issue 05)."""
from __future__ import annotations

import pytest

import pandas as pd
from lories.core.configs.configurator import Configurator
from lories.core.configs.errors import ConfigurationError
from lories.core.configs.parameters.duration import DurationParameter
from lories.util import parse_freq, to_timedelta


# ---------------------------------------------------------------- ms round-trip

@pytest.mark.parametrize(
    "text,expected",
    [
        ("500ms", pd.Timedelta(milliseconds=500)),
        ("10ms", pd.Timedelta(milliseconds=10)),
        ("1ms", pd.Timedelta(milliseconds=1)),
    ],
)
def test_ms_round_trip(text, expected):
    assert to_timedelta(text) == expected


def test_parse_freq_ms():
    assert parse_freq("500ms") == "500ms"
    assert parse_freq("10ms") == "10ms"
    assert parse_freq("1ms") == "ms"  # bare unit when value == 1


def test_get_duration_ms_via_config(write_conf):
    configs = write_conf('interval = "500ms"\n')
    assert configs.get_duration("interval") == pd.Timedelta(milliseconds=500)


# ---------------------------------------------------------------- B8

def test_get_duration_rejects_int(write_conf):
    configs = write_conf("interval = 10\n")  # bare int from TOML
    with pytest.raises(ConfigurationError, match="must be strings"):
        configs.get_duration("interval")


def test_get_duration_passes_through_timedelta(write_conf):
    configs = write_conf('label = "x"\n')
    delta = pd.Timedelta(seconds=5)
    configs.set("interval", delta)
    assert configs.get_duration("interval") is delta


class DurationHolder(Configurator):
    poll = DurationParameter(key="poll", min="1s")


def test_duration_parameter_int_raises_clear(write_conf):
    """A DurationParameter over a bare int raises a clear error, not the old misleading 'calendar vs fixed' one."""
    configs = write_conf("poll = 10\n")
    inst = DurationHolder()
    with pytest.raises(ConfigurationError, match="must be strings"):
        inst.configure(configs)


def test_duration_parameter_string_resolves(write_conf):
    configs = write_conf('poll = "2s"\n')  # >= the 1s minimum
    inst = DurationHolder()
    inst.configure(configs)
    assert inst.poll == pd.Timedelta(seconds=2)
