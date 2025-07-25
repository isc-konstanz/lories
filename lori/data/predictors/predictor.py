# -*- coding: utf-8 -*-
"""
lori.data.predictors.predictor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import abstractmethod
from functools import wraps
from typing import Optional

import pandas as pd
from lori.core import Configurations, Resources
from lori.core.activator import ActivatorMeta
from lori.data import Channel, Channels, DataContext
from lori.data.predictors.core import _Predictor
from lori.data.validation import validate_index
from lori.typing import TimestampType


class PredictorMeta(ActivatorMeta):
    # noinspection PyProtectedMember
    def __call__(cls, *args, **kwargs):
        prediction = super().__call__(*args, **kwargs)
        cls._wrap_method(prediction, "predict")

        return prediction


class Predictor(_Predictor, metaclass=PredictorMeta):
    __context: DataContext
    __resources: Resources

    def __init__(
        self,
        context: DataContext,
        configs: Optional[Configurations] = None,
        **kwargs,
    ) -> None:
        super().__init__(context=context, configs=configs, **kwargs)
        self.__resources = Resources()

    def __enter__(self) -> Predictor:
        self.activate(Resources())
        return self

    @property
    def resources(self) -> Resources:
        return self.__resources

    @property
    def channels(self) -> Channels:
        return Channels([resource for resource in self.__resources if isinstance(resource, Channel)])

    @abstractmethod
    def predict(
        self,
        resources: Resources,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> pd.DataFrame:
        pass

    # noinspection PyUnresolvedReferences, PyTypeChecker
    @wraps(predict, updated=())
    def _do_predict(self, resources: Resources, *args, **kwargs) -> pd.DataFrame:
        data = self._run_predict(resources, *args, **kwargs)
        data = self._validate(resources, data)
        return data

    # noinspection PyMethodMayBeStatic
    def _validate(self, resources: Resources, data: pd.DataFrame) -> pd.DataFrame:
        if not data.empty:
            data = validate_index(data)
            for resource in resources:
                if resource.id not in data:
                    continue
                if resource.type in [pd.Timestamp, dt.datetime]:
                    resource_data = data[resource.id]
                    if pd.api.types.is_string_dtype(resource_data.values):
                        data[resource.id] = pd.to_datetime(resource_data)
        return data
