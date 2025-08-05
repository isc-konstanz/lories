# -*- coding: utf-8 -*-
"""
lori.data.predictors.access
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Collection, Optional, TypeVar

from lori.core import Configurations, Directory, Registrator, RegistratorAccess, RegistratorContext, ResourceException
from lori.data import Channels
from lori.data.predictors.core import _Predictor
from lori.util import get_context

P = TypeVar("P", bound=_Predictor)


class PredictorAccess(RegistratorAccess[P]):
    __channels: Channels

    # noinspection PyUnresolvedReferences
    def __init__(self, registrar: Registrator, **kwargs) -> None:
        context = get_context(registrar, RegistratorContext).context.predictors
        super().__init__(context, registrar, "predictors", **kwargs)

    # noinspection PyShadowingBuiltins
    def _set(self, id: str, predictor: P) -> None:
        if not isinstance(predictor, _Predictor):
            raise ResourceException(f"Invalid connector type: {type(predictor)}")

        super()._set(id, predictor)

    def load(
        self,
        configs: Optional[Configurations] = None,
        configs_file: Optional[str] = None,
        configs_dir: Optional[str | Directory] = None,
        configure: bool = False,
        **kwargs: Any,
    ) -> Collection[P]:
        if configs is None:
            configs = self._get_registrator_section()
        if configs_file is None:
            configs_file = configs.name
        if configs_dir is None:
            configs_dir = configs.dirs.conf.joinpath(configs.name.replace(".conf", ".d"))
        return self._load(
            self._registrar,
            configs=configs,
            configs_file=configs_file,
            configs_dir=configs_dir,
            configure=configure,
            includes=_Predictor.INCLUDES,
            **kwargs,
        )
