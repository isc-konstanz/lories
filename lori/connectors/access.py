# -*- coding: utf-8 -*-
"""
lori.connectors.access
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lori._core._component import Component  # noqa
from lori._core._connector import Connector, _Connector, _ConnectorContext  # noqa
from lori._core._data import _DataManager  # noqa
from lori.core import RegistratorAccess, ResourceError
from lori.util import get_context


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
