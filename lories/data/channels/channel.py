# -*- coding: utf-8 -*-
"""
lories.data.channels.channel
~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections import OrderedDict
from threading import Lock
from typing import Any, Collection, Dict, List, Optional, Tuple, Type

import numpy as np
import pandas as pd
import pytz as tz
from lories._core._channel import ChannelState, _Channel  # noqa
from lories._core._tasks import TaskContext, _TaskContext  # noqa
from lories._core.typing import Timestamp  # noqa
from lories.core import ConfigurationError, Resource, ResourceError
from lories.data.channels import ChannelConnector, ChannelConverter, ChannelProcessors, Channels
from lories.data.processors import Processor
from lories.util import floor_date, parse_freq, to_bool, to_timedelta

LOG_AGGREGATES = ("sum", "mean", "min", "max", "last")


class Channel(_Channel, Resource):
    _context: _TaskContext

    _timestamp: pd.Timestamp = pd.NaT
    _value: Optional[Any] = None
    _state: str | ChannelState = ChannelState.DISABLED

    logger: ChannelConnector
    connector: ChannelConnector
    converter: ChannelConverter
    processors: ChannelProcessors

    _log_freq: Optional[str] = None
    _log_aggregate: str = "last"
    _log_buffer: Optional[List[Tuple[pd.Timestamp, Any]]] = None
    _log_lock: Lock

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        context: TaskContext,
        id: str,
        key: str = None,
        name: str = None,
        type: str | Type = None,
        processors: Optional[Dict[str, Any] | str] = None,
        converter: Optional[Dict[str, Any] | str] = None,
        connector: Optional[Dict[str, Any] | str] = None,
        logger: Optional[Dict[str, Any] | str] = None,
        **configs: Any,
    ) -> None:
        if "scale" in configs:
            raise ConfigurationError(
                f"Channel '{id}' uses the removed 'scale' key. "
                f'Replace it with: converter = {{ type = "linear", scale = {configs["scale"]} }}'
            )
        super().__init__(id=id, key=key, name=name, type=type, **configs)
        self._context = self._assert_context(context)
        self.processors = ChannelProcessors.build(self, processors)
        self.converter = ChannelConverter.build(self, **Channel._build_member(converter, "converter"))
        self.connector = ChannelConnector.build(self, **Channel._build_member(connector, "connector"))
        self.logger = ChannelConnector.build(self, **Channel._build_member(logger, "connector"))
        self._log_lock = Lock()
        self._configure_log()

    def _configure_log(self) -> None:
        log_freq = None
        if self.logger.enabled:
            log_freq = self.logger.get(next((k for k in ["freq", "frequency", "resolution"] if k in self.logger), None))
        self._log_freq = parse_freq(log_freq) if log_freq is not None else None
        if self._log_freq is None:
            self._log_aggregate = "last"
            return
        if not self._log_freq.endswith(("D", "h", "min", "s")):
            raise ConfigurationError(
                f"Channel '{self.id}' has invalid logger freq '{log_freq}', expected a unit of: D, h, min, s"
            )

        aggregate = self.logger.get("aggregate")
        if aggregate is None:
            aggregate = self.get("aggregate", default="last")
        aggregate = str(aggregate).lower()
        if aggregate not in LOG_AGGREGATES:
            raise ConfigurationError(
                f"Channel '{self.id}' has invalid logger aggregate '{aggregate}', "
                f"expected one of: {', '.join(LOG_AGGREGATES)}"
            )
        if aggregate != "last" and not issubclass(self.type, (int, float, bool, np.number, np.bool_)):
            raise ConfigurationError(
                f"Channel '{self.id}' of type '{self.type.__name__}' does not support logger aggregate '{aggregate}'"
            )
        self._log_aggregate = aggregate

    @classmethod
    def _assert_context(cls, context: TaskContext) -> TaskContext:
        if context is None or not isinstance(context, _TaskContext):
            raise ResourceError(f"Invalid '{cls.__name__}' context: {type(context)}")
        return context

    def _get_attrs(self) -> List[str]:
        return [
            *super()._get_attrs(),
            "processors",
            "converter",
            "connector",
            "logger",
            "value",
            "state",
            "timestamp",
        ]

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        vars = super()._get_vars()
        vars["value"] = self.value
        vars["state"] = self.state
        vars["timestamp"] = self.timestamp
        return vars

    # noinspection PyShadowingBuiltins
    def __repr__(self) -> str:
        vars = OrderedDict(key=self.id)
        if self.is_valid():
            vars["value"] = str(self.value)
        else:
            vars["state"] = str(self.state)
        vars["timestamp"] = str(self.timestamp)
        return f"{type(self).__name__}({', '.join(f'{k}={v}' for k, v in vars.items())})"

    @property
    def freq(self) -> Optional[str]:
        freq = self.get(next((k for k in ["freq", "frequency", "resolution"] if k in self), None), default=None)
        if freq is not None:
            freq = parse_freq(freq)
        return freq

    @property
    def timedelta(self) -> Optional[pd.Timedelta]:
        return to_timedelta(self.freq)

    @property
    def log_freq(self) -> Optional[str]:
        return self._log_freq

    @property
    def log_timedelta(self) -> Optional[pd.Timedelta]:
        return to_timedelta(self._log_freq)

    @property
    def log_aggregate(self) -> str:
        return self._log_aggregate

    @property
    def timestamp(self) -> pd.Timestamp:
        return self._timestamp

    @property
    def value(self) -> Optional[Any]:
        return self._value

    @value.setter
    def value(self, value) -> None:
        self._set(pd.Timestamp.now(tz.UTC).floor(freq="s"), value, ChannelState.VALID)

    @property
    def state(self) -> ChannelState | str:
        return self._state

    @state.setter
    def state(self, state) -> None:
        self._set(pd.Timestamp.now(tz.UTC).floor(freq="s"), None, state)

    def is_valid(self) -> bool:
        return self._is_valid(self.value, self.state)

    @staticmethod
    def _is_valid(value: Any, state: ChannelState) -> bool:
        return state == ChannelState.VALID and not Channel._is_empty(value)

    @staticmethod
    def _is_empty(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, (str, bytes, bytearray)):
            return len(value) == 0
        if isinstance(value, Collection):
            try:
                return bool(np.asarray(pd.isna(value)).any())
            except (TypeError, ValueError):
                return False
        return bool(pd.isna(value))

    def is_listener(self) -> bool:
        listener = self.get(next((k for k in ["listener", "listening", "listen"] if k in self), None), default=None)
        if listener is None:
            listener = False
        return to_bool(listener, any_str=True)

    def set(
        self,
        timestamp: pd.Timestamp,
        value: Any,
        state: Optional[str | ChannelState] = ChannelState.VALID,
    ) -> None:
        value = self.converter(value)
        if state == ChannelState.VALID and self._is_empty(value):
            # The converter produced nothing (e.g. a value outside its bounds was rejected).
            state = ChannelState.NOT_AVAILABLE
        self._set(timestamp, value, state)

    # noinspection PyUnresolvedReferences
    def _set(
        self,
        timestamp: pd.Timestamp,
        value: Optional[Any],
        state: str | ChannelState,
    ) -> None:
        if not isinstance(timestamp, pd.Timestamp):
            raise ResourceError(f"Expected pandas Timestamp for '{self.id}', not: {type(value)}")

        if state == ChannelState.VALID:
            for processor in self.processors:
                value = processor(timestamp, value)
                if value is Processor.SKIP:
                    # Leave _value, _timestamp and _state untouched — listeners
                    # must not see a "fresh" update for a skipped frame.
                    return
            if self._is_empty(value):
                raise ResourceError(f"Invalid value for valid state '{self.id}': {value}")
        self._timestamp = timestamp
        self._value = value
        self._state = state
        if state == ChannelState.VALID and self._log_freq is not None:
            with self._log_lock:
                if self._log_buffer is not None:
                    self._log_buffer.append((timestamp, value))

    def _log_interval_end(self, timestamp: pd.Timestamp) -> pd.Timestamp:
        timestamp = timestamp.tz_localize(tz.UTC) if timestamp.tzinfo is None else timestamp.tz_convert(tz.UTC)
        start = floor_date(timestamp, freq=self._log_freq)
        return start if start == timestamp else start + self.log_timedelta

    def pop_log_rows(self, until: pd.Timestamp, drop_unfinished: bool = False) -> pd.Series:
        """
        Removes the buffered values of all log intervals ending at or before ``until``
        and returns one aggregated value per interval, indexed by the interval end.
        """
        with self._log_lock:
            samples = self._log_buffer or []
            self._log_buffer = []
            finished = {}
            unfinished = []
            for sample in samples:
                end = self._log_interval_end(sample[0])
                if end <= until:
                    finished.setdefault(end, []).append(sample)
                else:
                    unfinished.append(sample)
            if not drop_unfinished:
                self._log_buffer = unfinished

        ends = sorted(finished.keys())
        values = [self._aggregate_log(finished[end]) for end in ends]
        return pd.Series(values, index=pd.DatetimeIndex(ends, tz=tz.UTC), name=self.id)

    def _aggregate_log(self, samples: List[Tuple[pd.Timestamp, Any]]) -> Any:
        values = [s[1] for s in sorted(samples, key=lambda s: s[0])]
        if self._log_aggregate == "last":
            return values[-1]
        if self._log_aggregate == "sum":
            return sum(int(v) if isinstance(v, (bool, np.bool_)) else v for v in values)
        if self._log_aggregate == "mean":
            return sum(float(v) for v in values) / len(values)
        return min(values) if self._log_aggregate == "min" else max(values)

    # noinspection PyShadowingBuiltins, PyProtectedMember
    def _update(
        self,
        converter: Optional[Dict[str, Any] | str] = None,
        connector: Optional[Dict[str, Any] | str] = None,
        logger: Optional[Dict[str, Any] | str] = None,
        **configs: Any,
    ) -> None:
        if converter is not None:
            self.converter._update(**Channel._build_member(converter, "converter"))
        if connector is not None:
            self.connector._update(**Channel._build_member(connector, "connector"))
        if logger is not None:
            self.logger._update(**Channel._build_member(logger, "connector"))
        super()._update(**configs)
        self._configure_log()

    def _copy_args(self) -> Dict[str, Any]:
        return {
            "id": self._id,
            **self.to_configs(),
            "context": self._context,
            "processors": self.processors.to_configs(),
            "converter": self.converter.to_configs(),
            "connector": self.connector.to_configs(),
            "logger": self.logger.to_configs(),
        }

    def copy(self) -> Channel:
        channel = super().copy()
        channel._timestamp = self._timestamp
        channel._value = self._value
        channel._state = self._state
        return channel

    def to_list(self) -> Channels:
        return Channels([self])

    def to_configs(self) -> Dict[str, Any]:
        configs = super().to_configs()
        configs["processors"] = self.processors.to_configs()
        configs["converter"] = self.converter.to_configs()
        configs["connector"] = self.connector.to_configs()
        configs["logger"] = self.logger.to_configs()
        return configs

    def to_series(self, state: bool = False) -> pd.Series:
        if not self.is_valid() and state:
            return pd.Series(data=[self.state], index=[self.timestamp], name=self.key)

        return self.converter.to_series(self.value, self.timestamp, name=self.key)

    # noinspection PyProtectedMember
    def from_logger(self) -> Channel:
        channel = self.copy()
        channel._update(**self.logger._copy_configs())
        return channel

    # noinspection PyShadowingBuiltins
    def has_logger(self, *ids: Optional[str]) -> bool:
        return self.logger.enabled and (any(self.logger.id == id for id in ids) if len(ids) > 0 else True)

    # noinspection PyShadowingBuiltins
    def has_connector(self, id: Optional[str] = None) -> bool:
        return self.connector.enabled and (self.connector.id == id if id is not None else True)

    # noinspection PyUnresolvedReferences
    def read(
        self,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> pd.DataFrame:
        return self._context.read(self.to_list(), start=start, end=end)

    # noinspection PyUnresolvedReferences
    def write(self, data: pd.DataFrame | pd.Series | Any) -> None:
        if data is None:
            raise ResourceError(f"Invalid data to write '{self.id}': {data}")
        if isinstance(data, pd.Series):
            data.name = self.id
            data = data.to_frame()
        elif not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(index=[pd.Timestamp.now(tz.UTC).floor(freq="s")], data=[data], columns=[self.id])

        self._context.write(data, self.to_list())
