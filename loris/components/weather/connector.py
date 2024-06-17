# -*- coding: utf-8 -*-
"""
loris.components.weather.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict

from loris import Configurations, Connector, Location


class WeatherConnector(Connector, ABC):
    location: Location

    def __init__(self, context, configs: Configurations, location, *args, **kwargs) -> None:
        super().__init__(context, configs, *args, **kwargs)
        self.location = location

    # noinspection PyMethodMayBeStatic
    def _get_config_defaults(self) -> Dict[str, Any]:
        return {}

    @abstractmethod
    def has_forecast(self) -> bool:
        pass
