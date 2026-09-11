# -*- coding: utf-8 -*-
"""
tests.test_connectors_context_reconnect
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`ConnectorContext._reconnect` must not queue a duplicate ConnectTask while a
previously submitted connect for the same connector is still in flight.

"""

import logging
from concurrent.futures import Future

from lories.connectors.context import ConnectorContext
from lories.connectors.errors import ConnectorError


class StubTaskContext:
    def __init__(self):
        self.submitted = []

    def _submit(self, task):
        future = Future()
        self.submitted.append((task, future))
        return future


class StubConnector:
    def __init__(self, id="test.camera"):
        self.id = id
        self.name = "Camera"
        self.channels = []
        self._connected = False

    def is_enabled(self):
        return True

    def _is_connected(self):
        return False


def make_context():
    # ConnectorContext.__init__ requires a real TaskContext; bypass it and
    # set the attributes _reconnect actually touches.
    context = object.__new__(ConnectorContext)
    context._logger = logging.getLogger("test.connectors.context")
    context._RegistratorContext__context = StubTaskContext()
    context._ConnectorContext__reconnect_futures = {}
    return context


def test_reconnect_skips_while_connect_in_flight():
    context = make_context()
    connector = StubConnector()

    context._reconnect(connector)
    assert len(context.context.submitted) == 1

    context._reconnect(connector)
    context._reconnect(connector)
    assert len(context.context.submitted) == 1


def test_reconnect_resubmits_after_connect_completes():
    context = make_context()
    connector = StubConnector()

    context._reconnect(connector)
    task, future = context.context.submitted[0]
    future.set_result(connector)

    context._reconnect(connector)
    assert len(context.context.submitted) == 2


def test_reconnect_resubmits_after_connect_fails():
    context = make_context()
    connector = StubConnector()

    context._reconnect(connector)
    task, future = context.context.submitted[0]
    future.set_exception(ConnectorError(connector, "boom"))

    context._reconnect(connector)
    assert len(context.context.submitted) == 2


def test_reconnect_tracks_connectors_independently():
    context = make_context()
    first = StubConnector("test.camera")
    second = StubConnector("test.meter")

    context._reconnect(first, second)
    assert len(context.context.submitted) == 2

    context._reconnect(first, second)
    assert len(context.context.submitted) == 2
