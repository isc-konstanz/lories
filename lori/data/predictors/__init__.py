# -*- coding: utf-8 -*-
"""
lori.data.predictors
~~~~~~~~~~~~~~~~~~~~


"""

from .core import (  # noqa: F401
    _Predictor,
    PredictorException,
    PredictorUnavailableException,
)

from . import access  # noqa: F401
from .access import PredictorAccess  # noqa: F401

from . import context  # noqa: F401
from .context import (  # noqa: F401
    PredictorContext,
    register_predictor_type,
    registry,
)

from . import predictor  # noqa: F401
from .predictor import Predictor  # noqa: F401

import importlib

for import_predictor in ["dummy"]:
    try:
        importlib.import_module(f".{import_predictor}", "lori.data.predictors")

    except ModuleNotFoundError:
        # TODO: Implement meaningful logging here
        pass

del importlib
