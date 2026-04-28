# -*- coding: utf-8 -*-
"""
lories.data.predictors.context
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from typing import Callable, Optional, Type

from lories._core._predictor import Predictor, _Predictor, _PredictorContext  # noqa
from lories.core.configs import Configurations
from lories.core.register import RegistratorContext, Registry
from lories.data.channels import Channels

registry = Registry[_Predictor]()


# noinspection PyShadowingBuiltins
def register_predictor_type(
    type: str,
    *alias: str,
    factory: Callable[[PredictorContext | Predictor, Optional[Configurations]], Predictor] = None,
    replace: bool = False,
) -> Callable[[Type[Predictor]], Type[Predictor]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[Predictor]) -> Type[Predictor]:
        registry.register(cls, type, *alias, factory=factory, replace=replace)
        return cls

    return _register


class PredictorContext(_PredictorContext, RegistratorContext[Predictor]):
    @property
    def _registry(self) -> Registry[Predictor]:
        return registry

    # noinspection PyShadowingBuiltins
    def activate(
        self,
        filter: Optional[Callable[[Predictor], bool]] = None,
        channels: Optional[Channels] = None,
    ) -> None:
        _activators = self.filter(filter)
        if len(_activators) > 0:
            self._activate(*_activators, channels=channels)

    def _activate(
        self,
        *predictor: Predictor,
        channels: Channels = None,
    ) -> None:
        for predictor in predictor:
            if not predictor.is_enabled():
                self._logger.debug(
                    f"Skipping to activate disabled {type(predictor).__name__} '{predictor.name}': {predictor.id}"
                )
                continue
            self.__activate(predictor, channels=channels)

    def __activate(
        self,
        predictor: Predictor,
        channels: Channels = None,
    ) -> None:
        self._logger.debug(f"Activating {type(predictor).__name__} '{predictor.name}': {predictor.id}")
        predictor.activate(channels)

        self._logger.info(f"Activated {type(predictor).__name__} '{predictor.name}': {predictor.id}")

    # noinspection PyShadowingBuiltins
    def deactivate(self, filter: Optional[Callable[[Predictor], bool]] = None) -> None:
        _predictors = self.filter(filter)
        if len(_predictors) > 0:
            self._deactivate(*_predictors)

    def _deactivate(self, *predictors: Predictor) -> None:
        for predictor in reversed(list(predictors)):
            if not predictor.is_active():
                self._logger.debug(
                    f"Skipping to deactivate already deactivated {type(predictor).__name__} '{predictor.name}': "
                    f"{predictor.id}"
                )
                return
            self.__deactivate(predictor)

    def __deactivate(self, predictor: Predictor) -> None:
        if not predictor.is_active():
            self._logger.debug(
                f"Skipping to deactivate already deactivated {type(predictor).__name__} '{predictor.name}': "
                f"{predictor.id}"
            )
            return
        try:
            self._logger.debug(f"Deactivating {type(predictor).__name__} '{predictor.name}': {predictor.id}")
            predictor.deactivate()

            self._logger.info(f"Deactivated {type(predictor).__name__} '{predictor.name}': {predictor.id}")

        except Exception as e:
            self._logger.warning(f"Failed deactivating predictor '{predictor.id}': {str(e)}")
            if self._logger.getEffectiveLevel() <= logging.DEBUG:
                self._logger.exception(e)
