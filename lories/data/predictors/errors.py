# -*- coding: utf-8 -*-
"""
lories.data.predictors.errors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lories._core._predictor import Predictor  # noqa
from lories.core.errors import ResourceError, ResourceUnavailableError


class PredictorError(ResourceError):
    """
    Raise if an error occurred accessing the predictor.

    """

    # noinspection PyArgumentList
    def __init__(self, predictor: Predictor, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.predictor = predictor


class PredictorUnavailableError(ResourceUnavailableError, PredictorError):
    """
    Raise if an accessed predictor can not be found.

    """
