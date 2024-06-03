# -*- coding: utf-8 -*-
"""
    loris.components.weather.weather
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    This module provides the :class:`loris.components.weather.weather.Weather`, used as reference to
    calculate e.g. photovoltaic installations generated power. The provided environmental data
    contains temperatures and horizontal solar irradiation, which can be used, to calculate the
    effective irradiance on defined, tilted photovoltaic systems.

"""
from __future__ import annotations
from loris import Configurations, Location
from loris.components.weather import WeatherBase, WeatherForecast, WeatherException


# noinspection SpellCheckingInspection
class Weather(WeatherBase):

    GHI = 'ghi'
    DNI = 'dni'
    DHI = 'dhi'
    TEMP_AIR = 'temp_air'
    TEMP_FELT = 'temp_felt'
    TEMP_DEW_POINT = 'dew_point'
    HUMIDITY_REL = 'relative_humidity'
    PRESSURE_SEA = 'pressure_sea'
    WIND_SPEED = 'wind_speed'
    WIND_SPEED_GUST = 'wind_speed_gust'
    WIND_DIRECTION = 'wind_direction'
    WIND_DIRECTION_GUST = 'wind_direction_gust'
    CLOUD_COVER = 'cloud_cover'
    CLOUDS_LOW = 'clouds_low'
    CLOUDS_MID = 'clouds_mid'
    CLOUDS_HIGH = 'clouds_high'
    SUNSHINE = 'sunshine'
    VISIBILITY = 'visibility'
    PRECIPITATION = 'precipitation'
    PRECIPITATION_CONV = 'precipitation_convective'
    PRECIPITATION_PROB = 'precipitation_probability'
    PRECIPITABLE_WATER = 'precipitable_water'
    SNOW_FRACTION = 'snow_fraction'

    location: Location

    _forecast: WeatherForecast = None

    # noinspection PyProtectedMember
    def __configure__(self, configs: Configurations) -> None:
        # Do not call super __configure__ to avoid data channels to be configured, before the
        # local connector object is instanced
        if hasattr(self._context, 'location'):
            location = getattr(self._context, 'location')
            if not isinstance(location, Location):
                raise WeatherException(f"Invalid location type for weather \"{self._uuid}\": {type(location)}")
            self.location = location

        elif configs.has_section(Location.SECTION):
            self.location = self.__localize__(configs.get_section(Location.SECTION))
        else:
            raise WeatherException(f"Unable to find valid location for weather configuration: {self.configs.name}")

        connector_configs = configs.copy()
        connector_configs['id'] = connector_configs.get('type').lower()
        connector_configs['uuid'] = f"{self._uuid}.{connector_configs['id']}"
        connector_configs.pop('data', None)
        connector_context = self._context._context.connectors
        connector = self._load_connector(connector_context, connector_configs, self.location)
        connector_context._add(connector)
        self._connector = connector
        self._connector.configure()
        self._configs.update(self._connector._get_config_defaults(), replace=False)

        if connector.has_forecast() and self._configs.has_section(WeatherForecast.SECTION):
            self._configs[WeatherForecast.SECTION]['id'] = f'{self.id}.forecast'
            self._forecast = WeatherForecast(self._context, connector, configs.get_section(WeatherForecast.SECTION))
            self._forecast.configure()

        self.data.configs.update(self.configs.get_section(self.data.SECTION, default={}), replace=False)
        self.data.configure()

    # noinspection PyMethodMayBeStatic
    def __localize__(self, configs: Configurations) -> Location:
        return Location(configs.get_float('latitude'),
                        configs.get_float('longitude'),
                        timezone=configs.get('timezone', default='UTC'),
                        altitude=configs.get_float('altitude', default=None),
                        country=configs.get('country', default=None),
                        state=configs.get('state', default=None))

    def __activate__(self) -> None:
        super().__activate__()
        if self.has_forecast():
            self._forecast.activate()

    def has_forecast(self) -> bool:
        return self._forecast is not None

    @property
    def forecast(self) -> WeatherForecast:
        if not self._forecast:
            raise WeatherException(f"Weather \"{self.name}\" has no forecast configured")
        return self._forecast
