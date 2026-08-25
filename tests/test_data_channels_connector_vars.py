# -*- coding: utf-8 -*-
"""
tests.test_data_channels_connector_vars
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Regression test for the ``ChannelConnector._get_vars`` name-mangling bug:
``**self.__configs`` inside ``ChannelConnector`` mangles to
``_ChannelConnector__configs``, an attribute that never exists (the real one,
set by ``_ChannelWrapper.__init__``, is ``_ChannelWrapper__configs``). That
sends every ``_get_vars()`` call through ``_ChannelWrapper.__getattr__``,
which doesn't find a matching config key either and raises AttributeError.
"""

from __future__ import annotations

from lories._core._connector import Connector
from lories.data.channels.connector import ChannelConnector


def _make_connector(**configs) -> ChannelConnector:
    return ChannelConnector(Connector, None, enabled=True, **configs)


def test_get_vars_does_not_raise():
    connector = _make_connector(host="localhost")
    assert connector._get_vars()["host"] == "localhost"


def test_get_returns_configured_value():
    connector = _make_connector(host="localhost")
    assert connector.get("host") == "localhost"
    assert connector.get("missing", "default") == "default"


def test_str_does_not_raise():
    connector = _make_connector(host="localhost")
    assert "host=localhost" in str(connector)
