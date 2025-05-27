# -*- coding: utf-8 -*-
"""
lori.connectors.entsoe.day_ahead
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Optional
import pandas as pd

from lori import Configurations, Resources, ConfigurationException
from lori.connectors import ConnectionException, Connector, ConnectorException, register_connector_type
from lori.connectors.entsoe import EntsoeConnector
from lori.typing import TimestampType


class EntsoeDayAheadConnector(EntsoeConnector):
    # https://github.com/EnergieID/entsoe-py?tab=readme-ov-file
    #       	2015	2016	2017	2018	2019	2020	2021
    #DE	        no	    no  	no  	no  	no  	no  	no
    #DE_AT_LU	yes	    yes	    yes	    yes	    no	    no	    no
    #DE_LU	    no	    no  	no  	yes 	yes 	yes 	yes
    #AT	        no	    no  	no  	yes 	yes 	yes 	yes

    country_code: str = 'DE_LU' # Germany-Luxembourg
    tariff_offset: float = 0

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        #TODO: implement DE as county code
        #TODO: validate country code against entsoe-py/entsoe/mappings.py
        self.country_code = configs.get("country_code", default=self.country_code)
        self.tariff_offset = configs.get("tariff_offset", default=self.tariff_offset)

    def read(
        self,
        resources: Resources,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> pd.DataFrame:

        #TODO: future import, is this needed?
        from lori.components.tariff.entsoe_day_ahead import EntsoeDayAhead

        #TODO: implement DE as county code handling
        results = []
        for r in resources:
            #TODO: better checking of resource type
            if r.key != EntsoeDayAhead.DAY_AHEAD and False:
                raise ConfigurationException(
                    f"Resource {r.key} is not a valid EntsoeDayAhead resource."
                )

            #TODO: Expected behavior if start==None or end==None

            #TODO: Exception handling
            try:
                ts = self._client.query_day_ahead_prices(self.country_code, start=start, end=end)
            except Exception as e:
                #ConnectionError(timeout, unauthorized, ...)
                raise ConnectionException(self, str(e))


            result = pd.DataFrame(ts, columns=[r.id])
            results.append(result)

        if len(results) == 0:
            return pd.DataFrame(columns=[r.id for r in resources])

        results = sorted(results, key=lambda d: min(d.index))
        df = pd.concat(results, axis="columns")

        return df
