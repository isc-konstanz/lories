# -*- coding: utf-8 -*-
"""
lories.connectors.access
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lories._core._component import Component  # noqa
from lories._core._connector import Connector, _Connector, _ConnectorContext  # noqa
from lories._core._data import _DataManager  # noqa
from lories.core import RegistratorAccess, ResourceError
from lories.util import get_context


class ConnectorAccess(_ConnectorContext, RegistratorAccess[Connector]):
    # noinspection PyUnresolvedReferences
    def __init__(self, registrar: Component, **kwargs) -> None:
        context = get_context(registrar, _DataManager).connectors
        super().__init__(context, registrar, **kwargs)

    # noinspection PyProtectedMember, PyShadowingBuiltins
    def _set(self, id: str, connector: Connector) -> None:
        if not isinstance(connector, _Connector):
            raise ResourceError(f"Invalid connector type: {type(connector)}")

        super()._set(id, connector)
