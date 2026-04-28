# -*- coding: utf-8 -*-
"""
lories.data.predictors.virtual
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import pandas as pd
from lories import Resources
from lories.data.predictors import Predictor, PredictorException, register_predictor_type
from lories.typing import Timestamp


# noinspection PyShadowingBuiltins
@register_predictor_type("virtual")
class VirtualPredictor(Predictor):
    def predict(
        self,
        resources: Resources,
        start: Timestamp = None,
        end: Timestamp = None,
    ) -> pd.DataFrame:
        data = []

        for generator, generator_resources in resources.groupby(lambda r: r.get("generator", default="logged")):
            if generator == "logged":
                data.append(self._from_logged(resources))

            raise PredictorException(self, f"Trying to predict resources with dummy generator: {generator}")

        if len(data) == 0:
            return pd.DataFrame()
        data = sorted(data, key=lambda d: min(d.index))
        return pd.concat(data, axis="columns")

    def _from_logged(self, resources: Resources) -> pd.DataFrame:
        # TODO: check if resources are channel and group by channel.logger._connector
        return pd.DataFrame()
