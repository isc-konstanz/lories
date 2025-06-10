# -*- coding: utf-8 -*-
"""
lori.connectors.entsoe
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Optional

# TODO: add to requirements
from entsoe import EntsoePandasClient
from entsoe.mappings import Area as EntsoeArea
from requests.exceptions import HTTPError
from urllib3.exceptions import MaxRetryError

import pandas as pd
from lori.connectors import ConnectionException, Connector, ConnectorException
from lori.core import ConfigurationException, Configurations, Resources
from lori.typing import TimestampType


# noinspection SpellCheckingInspection
class EntsoeConnector(Connector):
    DAY_AHEAD: str = "day_ahead"

    country_code: str = "DE_LU"  # Germany-Luxembourg
    _api_key: str

    _client: Optional[EntsoePandasClient] = None

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.country_code = self._validate_country_code(
            configs.get("country_code", default=EntsoeConnector.country_code)
        )
        self._api_key = configs.get("_api_key")
        if self._api_key is None:
            raise ConfigurationException("Missing security token")

    def _validate_country_code(self, country_code) -> str:
        """Validate the country code against the Entsoe mappings."""
        if country_code is None:
            raise ConfigurationException("Missing country code")
        elif not (EntsoeArea.has_code(self.country_code) or self.country_code == "DE"):
            raise ConfigurationException(f"Invalid country code: {self.country_code}.")
        return country_code

    def connect(self, resources: Resources) -> None:
        self._client = EntsoePandasClient(api_key=self._api_key)

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
        for method, method_resources in resources.groupby("method"):
            if method.lower() == EntsoeConnector.DAY_AHEAD:
                # TODO: Exception handling
                try:
                    prices = self._client.query_day_ahead_prices(country_code, start=start, end=end)

                except (MaxRetryError, HTTPError) as e:
                    if isinstance(e, MaxRetryError):
                        raise ConnectionException(self, str(e))
                    elif isinstance(e, HTTPError) and "Unauthorized" in str(e):
                        raise ConnectorException(self, "Unauthorized access. Check your API key.")
                    raise ConnectorException(self, str(e))

                result = pd.DataFrame()
                for resource in method_resources:
                    # Append the resource id to the series
                    result[resource.id] = prices
                results.append(result)

        if len(results) == 0:
            return pd.DataFrame()

        results = sorted(results, key=lambda d: min(d.index))
        data = pd.concat(results, axis="columns")
        return data

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
