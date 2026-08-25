# -*- coding: utf-8 -*-
"""
tests.test_data_access_write
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Regression test for issue 02: ``DataAccess.write(data)`` with the default
``channels=None`` must filter the access's own channels *before* renaming the
``DataFrame`` columns, not rename first while iterating the still-``None``
``channels`` argument (``TypeError: 'NoneType' object is not iterable``).
"""

from __future__ import annotations

import pandas as pd
from lories._core._context import _Context
from lories.data.access import DataAccess


class _FakeChannel:
    """The slice of a channel that ``write()`` reads: ``id`` and ``key``."""

    def __init__(self, id: str, key: str) -> None:
        self.id = id
        self.key = key


class _FakeContext:
    """Stand-in for the ``_TaskContext | _DataContext`` that ``write()``
    forwards to, recording what it was called with."""

    def __init__(self) -> None:
        self.written_data = None
        self.written_channels = None

    def write(self, data, channels=None, timeout=None, **kwargs) -> None:
        self.written_data = data
        self.written_channels = channels


def _build_data_access(channels):
    """Construct a bare ``DataAccess`` without the full Registrator/Configurator
    machinery ``DataAccess.__init__`` normally requires: set up only the
    entity map (via ``_Context.__init__``, the same bypass used in
    ``tests/test_listener_backpressure.py``) and stub the private
    ``__context`` attribute that ``write()`` forwards to.
    """
    access = object.__new__(DataAccess)
    _Context.__init__(access)
    for channel in channels:
        _Context._set(access, channel.id, channel)
    context = _FakeContext()
    access._DataAccess__context = context
    return access, context


def test_write_with_default_channels_filters_before_renaming():
    channel_a = _FakeChannel(id="sys.a", key="a")
    channel_b = _FakeChannel(id="sys.b", key="b")
    access, context = _build_data_access([channel_a, channel_b])

    data = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    # On unfixed code this raises TypeError: 'NoneType' object is not
    # iterable, because `channels` (default None) is renamed against before
    # `_filter_by_args` ever runs.
    access.write(data)

    assert context.written_data is data
    assert list(data.columns) == ["sys.a", "sys.b"]
    assert list(context.written_channels.ids) == ["sys.a", "sys.b"]
