# -*- coding: utf-8 -*-
"""
loris.components.weather.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, Optional

from loris import ConfigurationException, Configurations, Connector, Location
from loris.connectors import ConnectorContext


class WeatherConnector(Connector):
    location: Location

    # noinspection PyShadowingBuiltins, SpellCheckingInspection
    @staticmethod
    def load(
        type: str,
        context: ConnectorContext,
        configs: Configurations,
        location: Location
    ) -> Optional[WeatherConnector]:
        if type in ["brightsky", "dwd"]:
            from .dwd import Brightsky

            return Brightsky(context, configs, location)

        # elif type in ["meteoblue", "nmm"]:
        #     from .meteoblue import Meteoblue
        #
        #     return Meteoblue(context, configs, location)

        raise ConfigurationException(f"Unknown weather type: {type}")

    def __init__(self, context: ConnectorContext, configs: Configurations, location: Location, *args, **kwargs) -> None:
        super().__init__(context, configs, *args, **kwargs)
        self.location = location

    # noinspection PyMethodMayBeStatic
    def _get_config_defaults(self) -> Dict[str, Any]:
        return {}

    @abstractmethod
    def has_forecast(self) -> bool:
        pass
