# -*- coding: utf-8 -*-
"""
lories.connectors.access
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections.abc import Callable
from typing import Optional

from lories._core._component import Component  # noqa
from lories._core._connector import Connector, _Connector, _ConnectorContext  # noqa
from lories._core._tasks import _TaskContext  # noqa
from lories.core import RegistratorAccess, ResourceError
from lories.core.typing import ChannelsArgument
from lories.util import get_context


# noinspection PyProtectedMember, PyShadowingBuiltins
class ConnectorAccess(_ConnectorContext, RegistratorAccess[Connector]):
    # noinspection PyUnresolvedReferences
    def __init__(self, registrar: Component, **kwargs) -> None:
        context = get_context(registrar, _TaskContext).connectors
        super().__init__(context, registrar, **kwargs)

    def _set(self, id: str, connector: Connector) -> None:
        if not isinstance(connector, _Connector):
            raise ResourceError(f"Invalid connector type: {type(connector)}")

        super()._set(id, connector)

    def connect(
        self,
        filter: Optional[Callable[[Connector], bool]] = None,
        channels: Optional[ChannelsArgument] = None,
        timeout: Optional[float] = None,
    ) -> None:
        _connectors = self.filter(filter)
        if len(_connectors) > 0:
            self.context._connect(*_connectors, channels=channels, timeout=timeout)

    def reconnect(
        self,
        filter: Optional[Callable[[Connector], bool]] = None,
    ) -> None:
        _connectors = self.filter(filter)
        if len(_connectors) > 0:
            self.context._reconnect(*_connectors)

    def disconnect(
        self,
        filter: Optional[Callable[[Connector], bool]] = None,
    ) -> None:
        _connectors = self.filter(filter)
        if len(_connectors) > 0:
            self.context._disconnect(*_connectors)
