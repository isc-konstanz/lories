# -*- coding: utf-8 -*-
"""
lori.connector.entsoe.tariff
~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from typing import Optional

import pandas as pd
from lori import Configurations, Resources, ConfigurationException
from lori.connectors.entsoe import EntsoeConnector
from lori.typing import TimestampType




class EntsoeDayaheadConnector(EntsoeConnector):
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

        self.country_code = configs.get("country_code", default=self.country_code)
        self.tariff_offset = configs.get("tariff_offset", default=self.tariff_offset)

    def read(
        self,
        resources: Resources,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> pd.DataFrame:

        from lori.components.tariff.entsoe_dayahead import EntsoeDayahead

        results = []
        for r in resources:
            #TODO: better checking of resource type
            if r.key != EntsoeDayahead.DAY_AHEAD and False:
                raise ConfigurationException(
                    f"Resource {r.key} is not a valid EntsoeDayahead resource."
                )

            #TODO: Exception: Please use a timezoned pandas object for start and end (not localized?)
            ts = self._client.query_day_ahead_prices(self.country_code, start=start, end=end)
            result = pd.DataFrame(ts, columns=[r.id])
            results.append(result)


        if len(results) == 0:
            return pd.DataFrame(columns=[r.id for r in resources])

        #TODO: sort? index name?
        #results = sorted(results, key=lambda d: min(d.index))
        df = pd.concat(results, axis="columns")

        path = "/Users/jonasbechler/Nextcloud/Master_Share/WS_Python/penguin/data/isc/tariff"
        csv_filename = f"intraday_{start.strftime('%Y')}.csv"
        df.to_csv(f"{path}/{csv_filename}", index_label="timestamp")
        return df

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("Entsoe connector does not support writing")
