# -*- coding: utf-8 -*-
"""
lories.data.predictors.predictor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from functools import wraps
from typing import Iterable, Optional

import pandas as pd
from lories._core._channels import ChannelsArgument  # noqa
from lories._core._configurations import Configurations  # noqa
from lories._core._context import _Context  # noqa
from lories._core._predictor import _Predictor  # noqa
from lories._core._registrator import RegistratorContext  # noqa
from lories.core import ResourceError, Resources
from lories.core.activator import Activator, ActivatorMeta
from lories.data import Channel, Channels
from lories.core.register.registrator import Registrator
from lories.data.access import DataAccess
from lories.data.validation import validate_index
from lories.typing import Timestamp


class PredictorMeta(ActivatorMeta):
    # noinspection PyProtectedMember
    def __call__(cls, *args, **kwargs):
        prediction = super().__call__(*args, **kwargs)
        cls._wrap_method(prediction, "predict")

        return prediction


# noinspection PyAbstractClass
class Predictor(_Predictor, Registrator, Activator, metaclass=PredictorMeta):
    __data: DataAccess

    def __init__(
        self,
        context: RegistratorContext,
        configs: Optional[Configurations] = None,
        **kwargs,
    ) -> None:
        super().__init__(context=context, configs=configs, **kwargs)
        self.__data = DataAccess(self)

    def __enter__(self) -> Predictor:
        self.activate(Resources())
        return self

    def _at_configure(self, configs: Configurations) -> None:
        self.__data.configure(configs.get_member(DataAccess.TYPE, ensure_exists=True))

    def _on_configure(self, configs: Configurations) -> None:
        self.__data.load()

    @property
    def data(self) -> DataAccess:
        return self.__data

    # noinspection PyArgumentList
    @wraps(_Predictor.activate, updated=())
    def _do_activate(self, resources: Resources, *args, **kwargs) -> None:
        super()._do_activate(resources, *args, **kwargs)

    # noinspection PyUnresolvedReferences, PyTypeChecker
    @wraps(_Predictor.predict, updated=())
    def _do_predict(
        self,
        resources: Optional[Resources] = None,
        start: Optional[Timestamp | str] = None,
        end: Optional[Timestamp | str] = None,
        unique: bool = False,
        *args, **kwargs
    ) -> pd.DataFrame:
        data = self._run_predict(resources, start=start, end=end, *args, **kwargs)

        # TODO IMPLEMENT ME

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

    def _filter_by_args(self, channels: Optional[ChannelsArgument]) -> Channels:
        if channels is None:
            return Channels(self.data.values())
        _channels = []

        def append(_channel: Channel | str) -> None:
            if isinstance(_channel, str):
                if _channel in self:
                    _channels.append(self[_channel])
            elif isinstance(_channel, Channel):
                _channels.append(_channel)
            else:
                raise ResourceError(f"Invalid '{type(_channel)}' channel: {_channel}")

        if isinstance(channels, str) or isinstance(channels, Channel):
            append(channels)
        elif isinstance(channels, Iterable):
            for channel in channels:
                append(channel)
        else:
            raise ResourceError(f"Invalid '{type(channels)}' channels: {channels}")

        return Channels(_channels)
