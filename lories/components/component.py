# -*- coding: utf-8 -*-
"""
lories.components.component
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd
from lories._core._component import _Component  # noqa
from lories._core._configurations import Configurations  # noqa
from lories._core._registrator import RegistratorContext  # noqa
from lories._core.typing import Timestamp  # noqa
from lories.components.access import ComponentAccess
from lories.connectors.access import ConnectorAccess
from lories.core.activator import Activator
from lories.core.register import Registrator
from lories.data.access import DataAccess
from lories.data.converters import ConverterAccess
from lories.util import to_date


class Component(_Component, Registrator, Activator):
    __converters: ConverterAccess
    __connectors: ConnectorAccess
    __components: ComponentAccess
    __data: DataAccess

    def __init__(
        self,
        context: RegistratorContext,
        configs: Optional[Configurations] = None,
        **kwargs,
    ) -> None:
        super().__init__(context=context, configs=configs, **kwargs)
        self.__converters = ConverterAccess(self)
        self.__connectors = ConnectorAccess(self)
        self.__components = ComponentAccess(self)
        self.__data = DataAccess(self)

    def _at_configure(self, configs: Configurations) -> None:
        self.__data.configure(configs.get_member(DataAccess.TYPE, ensure_exists=True))

    def _on_configure(self, configs: Configurations) -> None:
        self.__converters.load(configure=False, sort=False)
        self.__converters.sort()
        self.__converters.configure()

        self.__connectors.load(configure=False, sort=False)
        self.__connectors.sort()
        self.__connectors.configure()

        self.__components.load(configure=False, sort=False)
        self.__components.sort()
        self.__components.configure()

        self.__data.load()

    def _at_duplicate(self, **changes) -> None:
        self.__converters.duplicate(**changes)
        self.__connectors.duplicate(**changes)
        self.__components.duplicate(**changes)

    @property
    def components(self) -> ComponentAccess:
        return self.__components

    @property
    def converters(self) -> ConverterAccess:
        return self.__converters

    @property
    def connectors(self) -> ConnectorAccess:
        return self.__connectors

    @property
    def data(self) -> DataAccess:
        return self.__data

    def get(
        self,
        start: Optional[Timestamp | str] = None,
        end: Optional[Timestamp | str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        start = to_date(start)
        end = to_date(end)

        data = self.__data.to_frame(unique=False)
        if data.empty or (start is not None and start < data.index[0]) or (end is not None and end > data.index[-1]):
            logged = self.__data.from_logger(start=start, end=end, unique=False)
            if not logged.empty:
                data = logged if data.empty else data.combine_first(logged)
        return self._get_range(data, start, end, **kwargs)

    @staticmethod
    def _get_range(
        data: pd.DataFrame,
        start: Optional[Timestamp | str] = None,
        end: Optional[Timestamp | str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        if data.empty:
            return data
        if start is not None:
            start = to_date(start, **kwargs)
            data = data[data.index >= start]
        if end is not None:
            end = to_date(end, **kwargs)
            data = data[data.index <= end]
        return data

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        vars = super()._get_vars()
        vars.pop("type", None)
        return vars
