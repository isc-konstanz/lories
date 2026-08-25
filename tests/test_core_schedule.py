# -*- coding: utf-8 -*-
"""
tests.test_core_schedule
~~~~~~~~~~~~~~~~~~~~~~~~~

The wall-clock tick-schedule vocabulary: interval/offset parsing and
validation, intake-delay parsing, and the absolute slot alignment of
``slot_ceil``/``slot_floor`` (restart-stable, offset-shifted).
"""

from __future__ import annotations

import pytest

import pandas as pd
from lories.core import Configurations
from lories.core.configs.errors import ConfigurationError
from lories.core.schedule import (
    parse_intake_delay,
    parse_tick_schedule,
    slot_ceil,
    slot_floor,
    validate_intake_delay,
    validate_tick_schedule,
)

UTC = "UTC"
H1 = pd.Timedelta(hours=1)
M30 = pd.Timedelta(minutes=30)


@pytest.fixture
def write_conf(tmp_path):
    def _write(toml_text: str) -> Configurations:
        (tmp_path / "schedule.conf").write_text(toml_text)
        return Configurations.load("schedule.conf", data_dir=str(tmp_path), flat=True)

    return _write


def test_parse_defaults(write_conf):
    interval, offset = parse_tick_schedule(write_conf(""), default_interval=60, default_offset=30)
    assert (interval, offset) == (pd.Timedelta(hours=1), pd.Timedelta(minutes=30))


def test_parse_overrides(write_conf):
    interval, offset = parse_tick_schedule(
        write_conf("interval = 15\noffset = 5\n"), default_interval=60, default_offset=0
    )
    assert (interval, offset) == (pd.Timedelta(minutes=15), pd.Timedelta(minutes=5))


def test_parse_invalid_interval_raises(write_conf):
    with pytest.raises(ConfigurationError, match="interval"):
        parse_tick_schedule(write_conf("interval = 0\n"), default_interval=60)


def test_parse_offset_out_of_range_raises(write_conf):
    with pytest.raises(ConfigurationError, match="offset"):
        parse_tick_schedule(write_conf("interval = 60\noffset = 60\n"), default_interval=60)


def test_parse_intake_delay(write_conf):
    assert parse_intake_delay(write_conf("")) == pd.Timedelta(0)
    assert parse_intake_delay(write_conf('intake_delay = "15min"\n')) == pd.Timedelta(minutes=15)
    with pytest.raises(ConfigurationError, match="intake_delay"):
        parse_intake_delay(write_conf('intake_delay = "-5min"\n'))


def test_slot_ceil_hourly_offset30():
    now = pd.Timestamp("2026-08-25 10:29:59", tz=UTC)
    assert slot_ceil(now, UTC, H1, M30) == pd.Timestamp("2026-08-25 10:30", tz=UTC)
    now = pd.Timestamp("2026-08-25 10:30:00", tz=UTC)
    # strictly after: on the slot itself, the next slot is an hour later
    assert slot_ceil(now, UTC, H1, M30) == pd.Timestamp("2026-08-25 11:30", tz=UTC)


def test_slot_floor_hourly_offset30():
    now = pd.Timestamp("2026-08-25 10:29:59", tz=UTC)
    assert slot_floor(now, UTC, H1, M30) == pd.Timestamp("2026-08-25 09:30", tz=UTC)
    now = pd.Timestamp("2026-08-25 10:30:00", tz=UTC)
    # at or before: the slot itself counts
    assert slot_floor(now, UTC, H1, M30) == pd.Timestamp("2026-08-25 10:30", tz=UTC)


def test_slot_alignment_is_absolute():
    # two different "activation" times land on the same slot grid
    a = slot_ceil(pd.Timestamp("2026-08-25 10:07", tz=UTC), UTC, H1, M30)
    b = slot_ceil(pd.Timestamp("2026-08-25 10:23", tz=UTC), UTC, H1, M30)
    assert a == b == pd.Timestamp("2026-08-25 10:30", tz=UTC)


def test_slot_floor_ceil_relation():
    now = pd.Timestamp("2026-08-25 10:17", tz=UTC)
    lo = slot_floor(now, UTC, pd.Timedelta(minutes=15), pd.Timedelta(minutes=5))
    hi = slot_ceil(now, UTC, pd.Timedelta(minutes=15), pd.Timedelta(minutes=5))
    assert lo <= now < hi
    assert hi - lo == pd.Timedelta(minutes=15)


def test_validate_tick_schedule_values():
    assert validate_tick_schedule(H1, M30) == (H1, M30)
    with pytest.raises(ConfigurationError, match="interval"):
        validate_tick_schedule(pd.Timedelta(0), pd.Timedelta(0))
    with pytest.raises(ConfigurationError, match="offset"):
        validate_tick_schedule(H1, H1, section_name="apple_detection")
    with pytest.raises(ConfigurationError, match="calendar"):
        from dateutil.relativedelta import relativedelta

        validate_tick_schedule(relativedelta(months=1), pd.Timedelta(0))


def test_validate_intake_delay_values():
    assert validate_intake_delay(pd.Timedelta(minutes=15)) == pd.Timedelta(minutes=15)
    with pytest.raises(ConfigurationError, match="intake_delay"):
        validate_intake_delay(pd.Timedelta(minutes=-1))


def test_tick_schedule_mixin_declares_inherited_parameters(write_conf):
    from lories.core.configs.configurator import Configurator
    from lories.core.schedule import TickSchedule

    class _Scheduled(TickSchedule, Configurator):
        pass

    params = _Scheduled.__config_parameters__
    assert {"interval", "offset", "intake_delay"} <= set(params)

    inst = _Scheduled()
    inst.configure(write_conf('interval = "1h"\noffset = "30min"\nintake_delay = "15min"\n'))
    assert inst.interval == H1
    assert inst.offset == M30
    assert inst.intake_delay == pd.Timedelta(minutes=15)
    assert inst.next_slot(pd.Timestamp("2026-08-25 14:12", tz=UTC)) == pd.Timestamp("2026-08-25 14:30", tz=UTC)
    assert inst.intake_cutoff(pd.Timestamp("2026-08-25 14:30", tz=UTC)) == pd.Timestamp("2026-08-25 14:15", tz=UTC)


def test_tick_schedule_mixin_defaults(write_conf):
    from lories.core.configs.configurator import Configurator
    from lories.core.schedule import TickSchedule

    class _Scheduled(TickSchedule, Configurator):
        pass

    inst = _Scheduled()
    inst.configure(write_conf(""))
    assert inst.interval == H1
    assert inst.offset == pd.Timedelta(0)
    assert inst.intake_delay == pd.Timedelta(0)


def test_tick_schedule_mixin_validates_on_configure(write_conf):
    from lories.core.configs.configurator import Configurator
    from lories.core.schedule import TickSchedule

    class _Scheduled(TickSchedule, Configurator):
        pass

    inst = _Scheduled()
    with pytest.raises(ConfigurationError, match="offset"):
        inst.configure(write_conf('interval = "1h"\noffset = "2h"\n'))


def test_tick_schedule_mixin_validates_without_super_call(write_conf):
    # validation lives in _at_configure, so a consumer forgetting
    # super().configure() cannot skip it
    from lories.core.configs.configurator import Configurator
    from lories.core.schedule import TickSchedule

    class _Forgetful(TickSchedule, Configurator):
        def configure(self, configs):
            pass

    inst = _Forgetful()
    with pytest.raises(ConfigurationError, match="offset"):
        inst.configure(write_conf('interval = "1h"\noffset = "2h"\n'))


def test_tick_schedule_mixin_rejects_wrong_mro_order():
    from lories.core.configs.configurator import Configurator
    from lories.core.schedule import TickSchedule

    with pytest.raises(TypeError, match="BEFORE the Configurator base"):

        class _WrongOrder(Configurator, TickSchedule):
            pass
