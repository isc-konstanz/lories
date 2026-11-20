# -*- coding: utf-8 -*-
"""
tests.test_logger_interval
~~~~~~~~~~~~~~~~~~~~~~~~~~

Drives the application's log step with controlled timestamps against an in-memory database
and checks that channels with ``logger.freq`` log one aggregated row per aligned interval,
while channels without it keep logging their own samples.
"""

from __future__ import annotations

import os
import tempfile
from threading import Thread

import pytest

import pandas as pd

T0 = pd.Timestamp("2026-01-01 12:00:00", tz="UTC")


def _ts(seconds: int) -> pd.Timestamp:
    return T0 + pd.Timedelta(seconds=seconds)


@pytest.fixture
def app():
    from lories.application import Settings
    from lories.application.main import Application
    from lories.connectors import Database, register_connector_type

    @register_connector_type("memory_log", replace=True)
    class MemoryLogDatabase(Database):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.store = pd.DataFrame()

        def connect(self, resources) -> None:
            pass

        def disconnect(self) -> None:
            pass

        def is_connected(self) -> bool:
            return True

        def read(self, resources, start=None, end=None) -> pd.DataFrame:
            return self.store.copy()

        def read_first(self, resources):
            return None

        def read_last(self, resources):
            return None

        def write(self, data: pd.DataFrame) -> None:
            self.store = data.combine_first(self.store).sort_index()

        def delete(self, resources, start=None, end=None) -> None:
            pass

    tmp_dir = tempfile.mkdtemp(prefix="logger_interval_")
    conf_dir = os.path.join(tmp_dir, "conf")
    os.makedirs(conf_dir)
    os.makedirs(os.path.join(tmp_dir, "data"))
    with open(os.path.join(conf_dir, "settings.conf"), "w") as file:
        file.write(
            'name = "logger_interval"\n\n[interface]\nenabled = false\n\n[connectors.memdb]\ntype = "memory_log"\n'
        )

    cwd = os.getcwd()
    os.chdir(tmp_dir)
    try:
        settings = Settings("logger_interval")
        application = Application(settings)
        application.configure(settings)
    finally:
        os.chdir(cwd)
    return application


def _channel(app, id="power", type="float", logger=None, **configs):
    logger = {"connector": "memdb", **(logger or {})}
    channel = app._create(id=id, key=id, type=type, logger=logger, **configs)
    app._add(channel)
    database = app.connectors.get("memdb")
    database._connected = False
    database.connect(app.channels.filter(lambda c: c.has_logger("memdb")))
    channel.pop_log_rows(T0)
    return channel


def _log(app, now, **kwargs):
    app._Application__log(now=now, blocking=True, **kwargs)


def _stored(app, id="power") -> pd.Series:
    store = app.connectors.get("memdb").store
    return store[id].dropna() if id in store else pd.Series(dtype=float)


def _feed(channel, seconds, values):
    for second, value in zip(seconds, values):
        channel.set(_ts(second), value)


def test_one_row_per_minute_stamped_on_interval_end(app):
    channel = _channel(app, logger={"freq": "1min", "aggregate": "mean"})
    _feed(channel, range(15, 121, 15), [1, 2, 3, 4, 5, 6, 7, 8])

    _log(app, _ts(61))

    stored = _stored(app)
    assert list(stored.index) == [_ts(60)]
    assert stored.iloc[0] == pytest.approx(2.5)

    _log(app, _ts(121))

    stored = _stored(app)
    assert list(stored.index) == [_ts(60), _ts(120)]
    assert stored.iloc[1] == pytest.approx(6.5)
    assert channel.logger.timestamp == _ts(120)


def test_interval_is_written_only_after_one_application_interval(app):
    channel = _channel(app, logger={"freq": "1min", "aggregate": "mean"})
    _feed(channel, [15, 30, 45, 60], [1, 2, 3, 4])

    _log(app, _ts(60))
    assert _stored(app).empty

    _log(app, _ts(61))
    assert list(_stored(app).index) == [_ts(60)]


@pytest.mark.parametrize(
    "aggregate, expected",
    [("sum", 10), ("mean", 2.5), ("min", 1), ("max", 4), ("last", 2)],
)
def test_aggregate_methods(app, aggregate, expected):
    channel = _channel(app, logger={"freq": "1min", "aggregate": aggregate})
    _feed(channel, [15, 30, 45, 60], [3, 1, 4, 2])

    _log(app, _ts(61))

    assert _stored(app).iloc[0] == pytest.approx(expected)


def test_last_is_value_with_greatest_timestamp(app):
    channel = _channel(app, logger={"freq": "1min"})
    _feed(channel, [45, 15, 30], [9, 7, 8])

    _log(app, _ts(61))

    assert _stored(app).iloc[0] == 9


def test_default_aggregate_is_last(app):
    channel = _channel(app, logger={"freq": "1min"})
    _feed(channel, [15, 30, 45], [1, 2, 3])

    _log(app, _ts(61))

    assert channel.log_aggregate == "last"
    assert _stored(app).iloc[0] == 3


def test_channel_aggregate_is_used_when_logger_has_none(app):
    channel = _channel(app, logger={"freq": "1min"}, aggregate="Sum")
    _feed(channel, [15, 30, 45], [1, 2, 3])

    _log(app, _ts(61))

    assert channel.log_aggregate == "sum"
    assert _stored(app).iloc[0] == 6


def test_logger_aggregate_beats_channel_aggregate(app):
    channel = _channel(app, logger={"freq": "1min", "aggregate": "max"}, aggregate="sum")
    _feed(channel, [15, 30, 45], [1, 2, 3])

    _log(app, _ts(61))

    assert channel.log_aggregate == "max"
    assert _stored(app).iloc[0] == 3


def test_bool_mean_counts_true_as_one(app):
    channel = _channel(app, type="bool", logger={"freq": "1min", "aggregate": "mean"})
    _feed(channel, [15, 30, 45, 60], [True, False, True, True])

    _log(app, _ts(61))

    assert _stored(app).iloc[0] == pytest.approx(0.75)


def test_push_channel_loses_no_value_between_ticks(app):
    channel = _channel(app, logger={"freq": "1min", "aggregate": "sum"})
    for second in range(1, 60):
        channel.set(_ts(second), 1)
    channel.set(_ts(60), 1)

    _log(app, _ts(61))

    assert _stored(app).iloc[0] == 60


def test_concurrent_pushes_are_all_recorded(app):
    channel = _channel(app, logger={"freq": "1min", "aggregate": "sum"})

    def push(offset: int) -> None:
        for i in range(250):
            channel.set(_ts(1 + (offset * 250 + i) % 59), 1)

    threads = [Thread(target=push, args=(n,)) for n in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    _log(app, _ts(61))

    assert _stored(app).iloc[0] == 1000


def test_empty_interval_writes_nothing_and_stalled_intervals_come_oldest_first(app):
    channel = _channel(app, logger={"freq": "1min", "aggregate": "sum"})
    _feed(channel, [30, 210], [1, 5])
    database = app.connectors.get("memdb")
    original = database._run_write
    frames = []

    def spy(data, *args, **kwargs):
        frames.append(data.copy())
        return original(data, *args, **kwargs)

    database._run_write = spy

    _log(app, _ts(301))

    assert len(frames) == 1
    assert list(frames[0].index) == [_ts(60), _ts(240)]
    assert list(frames[0]["power"]) == [1, 5]
    assert list(_stored(app).index) == [_ts(60), _ts(240)]


def test_shutdown_writes_finished_intervals_and_drops_unfinished(app):
    channel = _channel(app, logger={"freq": "1min", "aggregate": "sum"})
    _feed(channel, [30, 60, 90], [1, 2, 4])

    _log(app, _ts(100), final=True)

    stored = _stored(app)
    assert list(stored.index) == [_ts(60)]
    assert stored.iloc[0] == 3

    channel.set(_ts(110), 8)
    _log(app, _ts(200))
    assert list(_stored(app).index) == [_ts(60), _ts(120)]
    assert _stored(app).iloc[1] == 8


def test_force_does_not_write_unfinished_interval(app):
    channel = _channel(app, logger={"freq": "1min", "aggregate": "sum"})
    _feed(channel, [30, 90], [1, 2])

    _log(app, _ts(100), force=True)

    assert list(_stored(app).index) == [_ts(60)]


def test_finished_intervals_are_dropped_while_logger_is_disconnected(app):
    channel = _channel(app, logger={"freq": "1min", "aggregate": "sum"})
    database = app.connectors.get("memdb")
    _feed(channel, [30, 90], [1, 2])

    database._connected = False
    _log(app, _ts(100))
    database._connected = True
    assert _stored(app).empty

    _log(app, _ts(200))
    assert list(_stored(app).index) == [_ts(120)]
    assert _stored(app).iloc[0] == 2


def test_channel_without_logger_freq_keeps_logging_its_samples(app):
    sampled = _channel(app, id="sampled", freq="15s")
    pushed = _channel(app, id="pushed")
    for second, value in [(15, 1.0), (30, 2.0), (45, 3.0)]:
        sampled.set(_ts(second), value)
        pushed.set(_ts(second + 1), value)
        _log(app, _ts(second + 2))
    _log(app, _ts(100))

    assert list(_stored(app, "sampled").index) == [_ts(15), _ts(30), _ts(45)]
    assert list(_stored(app, "sampled")) == [1.0, 2.0, 3.0]
    assert list(_stored(app, "pushed").index) == [_ts(16), _ts(31), _ts(46)]
    assert sampled.log_freq is None


def test_interval_and_plain_channels_share_one_write(app):
    interval = _channel(app, id="interval", logger={"freq": "1min", "aggregate": "sum"})
    plain = _channel(app, id="plain")
    _feed(interval, [15, 45], [1, 2])
    plain.set(_ts(30), 7.0)

    _log(app, _ts(61))

    assert list(_stored(app, "interval").index) == [_ts(60)]
    assert list(_stored(app, "plain").index) == [_ts(30)]


def test_log_freq_resolves_alias(app):
    channel = _channel(app, logger={"resolution": "5min"})

    assert channel.log_freq == "5min"
    assert channel.log_timedelta == pd.Timedelta(minutes=5)
    assert channel.freq is None


def test_copies_do_not_share_buffer_or_lock(app):
    channel = _channel(app, logger={"freq": "1min", "aggregate": "sum"})
    copy = channel.from_logger()
    duplicate = channel.copy()

    channel.set(_ts(10), 1)

    for other in (copy, duplicate):
        assert other._log_lock is not channel._log_lock
        assert other._log_buffer is None
    assert len(channel._log_buffer) == 1
    assert copy.log_freq == "min"
    assert copy.log_aggregate == "sum"


@pytest.mark.parametrize("aggregate", ["median", "avg"])
def test_unknown_aggregate_is_rejected(app, aggregate):
    from lories.core.configs import ConfigurationError

    with pytest.raises(ConfigurationError, match="bad_agg") as error:
        _channel(app, id="bad_agg", logger={"freq": "1min", "aggregate": aggregate})
    assert aggregate in str(error.value)


@pytest.mark.parametrize("aggregate", ["sum", "mean", "min", "max"])
def test_numeric_aggregate_on_string_channel_is_rejected(app, aggregate):
    from lories.core.configs import ConfigurationError

    with pytest.raises(ConfigurationError, match="text_channel"):
        _channel(app, id="text_channel", type="str", logger={"freq": "1min", "aggregate": aggregate})


def test_last_is_allowed_on_string_channel(app):
    channel = _channel(app, id="text_channel", type="str", logger={"freq": "1min"})

    assert channel.log_aggregate == "last"


def test_update_revalidates_the_aggregate(app):
    from lories.core.configs import ConfigurationError

    channel = _channel(app, id="text_channel", type="str", logger={"freq": "1min"})

    with pytest.raises(ConfigurationError, match="text_channel"):
        channel._update(logger={"aggregate": "mean"})


def test_aggregate_without_logger_freq_is_not_validated(app):
    channel = _channel(app, id="text_channel", type="str", aggregate="mean")

    assert channel.log_freq is None


@pytest.mark.parametrize("freq", ["1W", "1M", "1Y"])
def test_variable_length_log_freq_is_rejected(app, freq):
    from lories.core.configs import ConfigurationError

    with pytest.raises(ConfigurationError, match="bad_freq") as error:
        _channel(app, id="bad_freq", logger={"freq": freq})
    assert freq in str(error.value)


@pytest.mark.parametrize("freq", ["30s", "5min", "1h", "1D"])
def test_fixed_length_log_freq_is_accepted(app, freq):
    assert _channel(app, logger={"freq": freq}).log_freq is not None


def test_disabled_logger_has_no_log_freq_and_buffers_nothing(app):
    channel = app._create(
        id="quiet", key="quiet", type="float", logger={"connector": "memdb", "enabled": False, "freq": "1min"}
    )

    assert channel.log_freq is None
    channel.set(_ts(10), 1.0)
    channel.pop_log_rows(T0)
    channel.set(_ts(20), 1.0)
    assert channel._log_buffer == []


def test_disabled_logger_inherits_defaults_without_collecting(app):
    from lories._core._channel import _Channel  # noqa
    from lories.util import update_recursive

    defaults = _Channel._build_defaults({"logger": {"freq": "1min"}})
    configs = update_recursive(defaults, {"logger": {"connector": "memdb", "enabled": False}})
    channel = app._create(id="quiet", key="quiet", type="float", **configs)

    assert channel.log_freq is None
    channel.pop_log_rows(T0)
    channel.set(_ts(10), 1.0)
    assert channel._log_buffer == []


def test_collection_starts_with_first_pop(app):
    channel = app._create(
        id="fresh", key="fresh", type="float", logger={"connector": "memdb", "freq": "1min", "aggregate": "sum"}
    )

    channel.set(_ts(10), 1.0)
    assert channel._log_buffer is None

    assert channel.pop_log_rows(_ts(100)).empty
    channel.set(_ts(130), 2.0)
    assert channel._log_buffer == [(_ts(130), 2.0)]
