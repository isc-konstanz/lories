# -*- coding: utf-8 -*-
"""
lories.data.predictors.access
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Optional, Sequence

from lories._core._application import _Application  # noqa
from lories._core._channels import ChannelsArgument  # noqa
from lories._core._component import Component  # noqa
from lories._core._predictor import Predictor, _Predictor, _PredictorContext  # noqa
from lories.core import Configurations, RegistratorAccess, ResourceError
from lories.util import get_context


# noinspection PyProtectedMember, PyShadowingBuiltins
class PredictorAccess(_PredictorContext, RegistratorAccess[Predictor]):
    # noinspection PyUnresolvedReferences
    def __init__(self, registrar: Component, **kwargs) -> None:
        context = get_context(registrar, _Application).predictors
        super().__init__(context, registrar, **kwargs)

    def _set(self, id: str, predictor: Predictor) -> None:
        if not isinstance(predictor, _Predictor):
            raise ResourceError(f"Invalid predictor type: {type(predictor)}")

        super()._set(id, predictor)

    # noinspection PyUnresolvedReferences
    def activate(
        self,
        filter: Optional[Callable[[Predictor], bool]] = None,
        channels: Optional[ChannelsArgument] = None,
    ) -> None:
        _predictors = self.filter(filter)
        if len(_predictors) > 0:
            self.context._activate(*_predictors)

    # noinspection PyUnresolvedReferences
    def deactivate(
        self,
        filter: Optional[Callable[[Predictor], bool]] = None,
        channels: Optional[ChannelsArgument] = None,
    ) -> None:
        _predictors = self.filter(filter)
        if len(_predictors) > 0:
            self.context._deactivate(*_predictors)

    def load(
        self,
        configs: Optional[Configurations] = None,
        configs_file: Optional[str] = None,
        configs_dir: Optional[str] = None,
        configure: bool = False,
        **kwargs: Any,
    ) -> Sequence[Predictor]:
        return super().load(configs, configs_file, configs_dir, configure, strict=True)

    def _create(
        self,
        context: _PredictorContext | _Predictor,
        configs: Configurations,
        factory: Optional[Callable[..., Predictor]] = None,
        **kwargs: Any,
    ) -> Predictor:
        if factory is None:
            factory = super()._create
        return factory(context, configs, **kwargs)
