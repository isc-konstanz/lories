# -*- coding: utf-8 -*-
"""
lori.connector.weather.dwd.brightsky
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from typing import Optional

import numpy as np
import pandas as pd
from lori import Configurations, Resources
from lori.components.tariff import Tariff

#from lori.components.tariff import TariffConnector
from lori.connectors.entsoe import EntsoeConnector


class EntsoeTariff(EntsoeConnector): # TariffConnector, EntsoeConnector):
    document_type: str = "A44"
    domain: str = "10YAT-APG------L"

    def __init__(self, context: Tariff, **kwargs) -> None:
        super().__init__(context, **kwargs)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

    def read(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> pd.DataFrame:

        parameters = {
            #[M] Tariff Document (Tariff Document)
            "documentType":  self.document_type,
            # [M] EIC code of a Bidding Zone
            "out_Domain": self.domain,
            # [M] EIC code of a Bidding Zone (must be same as out_Domain)
            "in_Domain": self.domain,
            # [O] A01 = Day-ahead ; A07 = Intraday
            # "contract_MarketAgreement.type": self,
            # [O] Integer? This is actual 1 = Day-ahead ; 2 = Intraday
            # "classificationSequence_AttributeInstanceComponent.position": 1,
            # [O] Integer (allows downloading more than 100 documents. The offset ∈ [0,4800] so that pagging is restricted to query for 4900 documents max., offset=n returns files in sequence between n+1 and n+100)\n
            #"offset": self,

        }
        #day_diff = (end - start).days

        results = []
        for r in resources:
            if r.key == Tariff.DAYAHEAD.key:
                parameters["classificationSequence_AttributeInstanceComponent.position"] = "1"

            elif r.key == Tariff.INTRADAY.key:
                parameters["classificationSequence_AttributeInstanceComponent.position"] = "2"

            json_response = self._request(parameters, start, end)
            result = self._json_timeseries_parse(json_response)
            result = result.rename(columns={"value": r.id})

            if len(result) > 0:
                results.append(result)
            else:
                results.append(pd.DataFrame(columns=[r.id]))



        if len(results) > 0:
            #results = sorted(results, key=lambda d: min(d.index))
            df = pd.concat(results, axis="columns")
            df.index.name = "timestamp"
            df = df.ffill()
            return df
        return pd.DataFrame(columns=[r.id for r in resources])

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("Entsoe connector does not support writing")
