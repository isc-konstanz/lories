# -*- coding: utf-8 -*-
"""
lories.connectors.entsoe
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Optional

# TODO: Add to requirements
from entsoe import EntsoePandasClient
from entsoe.mappings import Area as EntsoeArea
from requests.exceptions import HTTPError
from urllib3.exceptions import MaxRetryError

import pandas as pd
import pytz as tz
from lories.connectors import ConnectionError, Connector, ConnectorError
from lories.core.configs import ConfigurationError
from lories.typing import Configurations, Resources, Timestamp
from lories.util import parse_freq

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


# noinspection SpellCheckingInspection
class EntsoeConnector(Connector):
    DAY_AHEAD: str = "day_ahead"
    METHODS: list = [DAY_AHEAD]

    resolution: Literal["60min", "30min", "15min"] = "60min"

    country_code: str
    _api_key: str

    _client: Optional[EntsoePandasClient] = None

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.resolution = self._validate_resolution(configs.get("resolution", default=EntsoeConnector.resolution))
        self.country_code = self._validate_country_code(configs.get("country_code").upper())
        self._api_key = configs.get("api_key")
        if self._api_key is None:
            raise ConfigurationError("Missing security token")

    # noinspection PyTypeChecker
    def _validate_resolution(self, resolution: str) -> Literal["60min", "30min", "15min"]:
        resolution = parse_freq(resolution)
        if resolution not in ["60min", "30min", "15min"]:
            raise ConnectorError(self, f"Invalid resolution: {resolution}.")
        return resolution

    def _validate_country_code(self, country_code) -> str:
        """Validate the country code against the Entsoe mappings."""
        if country_code is None:
            raise ConfigurationError(self, "Missing country code")
        elif not (EntsoeArea.has_code(country_code) or country_code == "DE"):
            raise ConfigurationError(self, f"Invalid country code: {country_code}.")
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
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> pd.DataFrame:
        if start is None:
            start = pd.Timestamp.now(tz=tz.UTC).floor(self.resolution)
        if end is None:
            end = start + pd.Timedelta(days=1) - pd.Timedelta(self.resolution)

        results = []
        country_code = self._get_country_code(self.country_code, start, end)
        for method, method_resources in resources.groupby("method"):
            if method not in self.METHODS:
                raise ConnectorError(self, f"Unsupported method: {method}. Supported methods: {self.METHODS}")

            if method.lower() == EntsoeConnector.DAY_AHEAD:
                # TODO: Exception handling
                try:
                    prices = self._client.query_day_ahead_prices(country_code, start=start, end=end)

                except (MaxRetryError, HTTPError) as e:
                    if isinstance(e, MaxRetryError):
                        raise ConnectionError(self, str(e))
                    elif isinstance(e, HTTPError) and "Unauthorized" in str(e):
                        raise ConnectorError(self, "Unauthorized access. Check your API key.")
                    raise ConnectorError(self, str(e))

                result = pd.DataFrame()
                for resource in method_resources:
                    result[resource.id] = prices
                results.append(result)

        if len(results) == 0:
            return pd.DataFrame()

        results = sorted(results, key=lambda d: min(d.index))
        data = pd.concat(results, axis="columns")
        return data

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("EntsoeConnector does not support writing data")

    def _get_country_code(self, county_code: str, start: Timestamp, end: Timestamp) -> str:
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
                raise ConnectorError(self, "Cannot determine country code for Germany over the given time range.")

            if county_code in ["DE", "LU"]:
                return "DE_LU"
            else:
                return "AT"

        return county_code
