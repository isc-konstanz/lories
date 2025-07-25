# -*- coding: utf-8 -*-
"""
lori.data.predictors.core
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Collection, Dict, List, Optional, overload

import pandas as pd
from lori.core import Configurations, ResourceException, Resources, ResourceUnavailableException
from lori.core.activator import Activator
from lori.core.register import Registrator
from lori.data import Channel, Channels, DataAccess
from lori.typing import TimestampType


# noinspection PyShadowingBuiltins
class _Predictor(Registrator, Activator):
    SECTION: str = "predictor"
    INCLUDES: List[str] = [DataAccess.SECTION]

    # noinspection PyProtectedMember
    @classmethod
    def _build_defaults(
        cls,
        configs: Configurations,
        includes: Optional[Collection[str]] = (),
        strict: bool = False,
    ) -> Dict[str, Any]:
        defaults = super()._build_defaults(configs, includes)
        if strict and DataAccess.SECTION in defaults:
            defaults[DataAccess.SECTION][Channels.SECTION] = Channel._build_defaults(
                defaults[DataAccess.SECTION].get_section(Channels.SECTION, defaults={})
            )
        return defaults

    @property
    @abstractmethod
    def resources(self) -> Resources: ...

    @property
    @abstractmethod
    def channels(self) -> Channels: ...

    def activate(self, resources: Resources) -> None:
        pass

    def _at_activate(self, resources: Resources) -> None:
        pass

    def _on_activate(self, resources: Resources) -> None:
        pass

    @overload
    def predict(
        self,
        resources: Resources,
    ) -> pd.DataFrame: ...

    @overload
    def predict(
        self,
        resources: Resources,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def predict(
        self,
        resources: Resources,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> pd.DataFrame: ...


class PredictorException(ResourceException):
    """
    Raise if an error occurred with the predictor.

    """

    # noinspection PyArgumentList
    def __init__(self, predictor: _Predictor, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.predictor = predictor


class PredictorUnavailableException(ResourceUnavailableException, PredictorException):
    """
    Raise if an accessed predictor can not be found.

    """
