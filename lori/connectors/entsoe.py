# -*- coding: utf-8 -*-
"""
lori.connectors.entsoe
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import Optional
import numpy as np
import pandas as pd

# TODO: add to requirements
from entsoe import EntsoePandasClient
from entsoe.mappings import Area as EntsoeArea
from urllib3.exceptions import MaxRetryError
from requests.exceptions import HTTPError

from lori.connectors import ConnectorException, ConnectionException, Connector
from lori.core import ConfigurationException, Configurations, Resources, Constant
from lori.typing import TimestampType


# noinspection PyShadowingBuiltins
class EntsoeConnector(Connector):
    DAY_AHEAD = Constant(float, "day_ahead", name="Day-Ahead", unit="ct/kWh")

    api_key: str
    country_code: str = 'DE_LU'  # Germany-Luxembourg

    _client: Optional[EntsoePandasClient] = None

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self.api_key = configs.get("api_key")
        if self.api_key is None:
            raise ConfigurationException("Missing security token")

        self.country_code = configs.get("country_code")
        self._validate_country_code()

    def _validate_country_code(self) -> None:
        """Validate the country code against the Entsoe mappings."""
        if self.country_code is None:
            raise ConfigurationException("Missing country code")
        elif not (EntsoeArea.has_code(self.country_code) or self.country_code == "DE"):
            raise ConfigurationException(f"Invalid country code: {self.country_code}.")

    def connect(self, resources: Resources) -> None:
        self._client = EntsoePandasClient(api_key=self.api_key)

    def disconnect(self) -> None:
        if self._client is not None:
            self._client = None

    def is_connected(self) -> bool:
        return self._client is not None

    def read(
            self,
            resources: Resources,
            start: Optional[TimestampType] = None,
            end: Optional[TimestampType] = None,
    ) -> pd.DataFrame:
        # TODO: Expected behavior if start==None and end==None or just one of them is None

        results = []
        country_code = self._get_country_code(self.country_code, start, end)
        for key, keyed_resources in resources.groupby(lambda x: x.key):
            if key == EntsoeConnector.DAY_AHEAD:

                # TODO: Exception handling
                try:
                    ts = self._client.query_day_ahead_prices(country_code, start=start, end=end)
                except (MaxRetryError, HTTPError) as e:
                    if isinstance(e, MaxRetryError):
                        raise ConnectionException(self, str(e))
                    elif isinstance(e, HTTPError) and "Unauthorized" in str(e):
                        raise ConnectorException(self, "Unauthorized access. Check your API key.")
                    raise ConnectorException(self, str(e))

                result = pd.DataFrame()
                for r in keyed_resources:
                    # append the resource id to the series
                    result[r.id] = ts

                results.append(result)

        if len(results) == 0:
            return pd.DataFrame(columns=[r.id for r in resources])

        results = sorted(results, key=lambda d: min(d.index))
        df = pd.concat(results, axis="columns")
        return df

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("EntsoeConnector does not support writing data")

    def _get_country_code(self, county_code: str, start: TimestampType, end: TimestampType) -> str:
        """
        Returns the country code for Germany based on the start and end dates.
        """
        # https://github.com/EnergieID/entsoe-py?tab=readme-ov-file
        #       	2015	2016	2017	2018	2019	2020	2021
        # DE	    no	    no  	no  	no  	no  	no  	no
        # DE_AT_LU	yes	    yes	    yes	    yes	    no	    no	    no
        # DE_LU	    no	    no  	no  	yes 	yes 	yes 	yes
        # AT	    no	    no  	no  	yes 	yes 	yes 	yes
        
        if county_code in ["DE", "AT", "LU"]:
            if end.year < 2019:
                return "DE_AT_LU"
            elif start.year > 2018:
                if county_code in ["DE", "LU"]:
                    return "DE_LU"
                else:
                    return "AT"
            elif start.year < 2018 and 2018 < end.year:
                raise ConnectorException(self, "Cannot determine country code for Germany over the given time range.")

            if county_code in ["DE", "LU"]:
                return "DE_LU"
            else:
                return "AT"

        return county_code



