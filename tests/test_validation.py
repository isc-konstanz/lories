# -*- coding: utf-8 -*-
"""Unit tests for ``lories.data.validation.validate_index`` (pure function, no DB)."""

from __future__ import annotations

import warnings

import pytest

import pandas as pd
from lories.core.errors import ResourceError
from lories.data.validation import validate_index, validate_timezone

# Mixed UTC offsets across Europe/Berlin's 2024 spring-forward (CET +01:00 -> CEST +02:00).
# As they arrive from transport: object-dtype strings, not a DatetimeIndex.
DST_MIXED_OFFSETS = [
    "2024-03-31 00:30:00+01:00",
    "2024-03-31 01:30:00+01:00",
    "2024-03-31 03:30:00+02:00",
]


def _series(values):
    return pd.Series(range(len(values)), index=pd.Index(values, dtype=object))


def test_dst_mixed_offsets_yield_utc_datetimeindex():
    # The bug: a replication slice crossing a DST boundary carries mixed offsets. On pandas <3
    # validate_index silently returned an object-dtype index; on pandas >=3 it raised ResourceError.
    # Either way the result was not a usable DatetimeIndex. The fix coerces to UTC.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # silence pandas <3 mixed-offset FutureWarning
        out = validate_index(_series(DST_MIXED_OFFSETS))

    assert isinstance(out.index, pd.DatetimeIndex)
    assert str(out.index.tz) == "UTC"
    # Offsets resolve correctly: 00:30+01:00 and 03:30+02:00 are both 23:30/01:30 UTC apart as expected.
    assert list(out.index) == [
        pd.Timestamp("2024-03-30 23:30:00", tz="UTC"),
        pd.Timestamp("2024-03-31 00:30:00", tz="UTC"),
        pd.Timestamp("2024-03-31 01:30:00", tz="UTC"),
    ]


def test_garbage_index_still_raises():
    # Regression: a genuinely-invalid index fails under utc=True too, so it must still raise.
    with pytest.raises(ResourceError):
        validate_index(_series(["not a date", "still not a date"]))


def test_naive_single_offset_index_unchanged():
    # Happy path must not be UTC-localized: naive strings stay a naive DatetimeIndex.
    out = validate_index(_series(["2024-01-01 00:00:00", "2024-01-01 01:00:00"]))
    assert isinstance(out.index, pd.DatetimeIndex)
    assert out.index.tz is None


def test_existing_datetimeindex_passes_through():
    idx = pd.date_range("2024-01-01", periods=3, freq="h", tz="UTC")
    s = pd.Series(range(3), index=idx)
    out = validate_index(s)
    assert out.index.equals(idx)


def test_dataframe_caller_path_is_benign():
    # Mirrors a non-replication caller (database._validate / connector._validate / channels):
    # a multi-column DataFrame goes through validate_index then validate_timezone. The broadened
    # UTC coercion must produce a tz-aware DatetimeIndex that the downstream tz_convert accepts.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        data = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]}, index=pd.Index(DST_MIXED_OFFSETS, dtype=object))
        out = validate_index(data)

    assert isinstance(out.index, pd.DatetimeIndex)
    out.index.name = "timestamp"  # callers assign this immediately after; must not error
    converted = validate_timezone(out.index, "Europe/Berlin")
    assert str(converted.tz) == "Europe/Berlin"
    assert list(out["a"]) == [1, 2, 3]  # rows preserved, still aligned to their timestamps


def test_non_unique_index_raises():
    idx = pd.DatetimeIndex(["2024-01-01", "2024-01-01"])
    with pytest.raises(ResourceError):
        validate_index(pd.Series([1, 2], index=idx))
