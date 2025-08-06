# -*- coding: utf-8 -*-
"""
lori.components.component
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd
from lori.components import ComponentAccess
from lori.components.core import _Component
from lori.connectors import ConnectorAccess
from lori.converters import ConverterAccess
from lori.core import Configurations, Context, Registrator
from lori.data import DataAccess
from lori.data.predictors import PredictorAccess
from lori.typing import TimestampType
from lori.util import to_date


# noinspection PyAbstractClass
class Component(_Component):
    __converters: ConverterAccess
    __connectors: ConnectorAccess
    __components: ComponentAccess
    __predictors: PredictorAccess
    __data: DataAccess

    def __init__(
        self,
        context: Context | Registrator,
        configs: Optional[Configurations] = None,
        **kwargs,
    ) -> None:
        super().__init__(context=context, configs=configs, **kwargs)
        self.__converters = ConverterAccess(self)
        self.__connectors = ConnectorAccess(self)
        self.__components = ComponentAccess(self)
        self.__predictors = PredictorAccess(self)
        self.__data = DataAccess(self)

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        vars = super()._get_vars()
        vars.pop("type", None)
        return vars

    def _at_configure(self, configs: Configurations) -> None:
        super()._at_configure(configs)
        self.__data.configure(configs.get_section(DataAccess.SECTION, ensure_exists=True))

    def _on_configure(self, configs: Configurations) -> None:
        super()._on_configure(configs)

        self.__converters.load(configure=False, sort=False)
        self.__converters.sort()
        self.__converters.configure()

        self.__connectors.load(configure=False, sort=False)
        self.__connectors.sort()
        self.__connectors.configure()

        self.__predictors.load(configure=False, sort=False)
        self.__predictors.sort()
        self.__predictors.configure()

        self.__components.load(configure=False, sort=False)
        self.__components.sort()
        self.__components.configure()

        self.__data.load()

    def _at_duplicate(self, **changes) -> None:
        super()._at_duplicate(**changes)
        self.converters.duplicate(**changes)
        self.connectors.duplicate(**changes)
        self.components.duplicate(**changes)

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
    def predictors(self) -> PredictorAccess:
        return self.__predictors

    @property
    def data(self):
        return self.__data

    def get(
        self,
        start: Optional[TimestampType | str] = None,
        end: Optional[TimestampType | str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        data = self.__data.to_frame(unique=False)
        if data.empty or start < data.index[0] or end > data.index[-1]:
            logged = self.__data.from_logger(start=start, end=end, unique=False)
            if not logged.empty:
                data = logged if data.empty else data.combine_first(logged)
        return self._get_range(data, start, end, **kwargs)

    @staticmethod
    def _get_range(
        data: pd.DataFrame,
        start: Optional[TimestampType | str] = None,
        end: Optional[TimestampType | str] = None,
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

    def predict(
        self,
        start: Optional[TimestampType | str] = None,
        end: Optional[TimestampType | str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        # TODO: Implement by accessing local predictors
        raise NotImplementedError()
