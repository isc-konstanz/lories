# -*- coding: utf-8 -*-
"""
    corsys.weather.nmm
    ~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations
from typing import Dict

import json
import pandas as pd
import requests

from io import StringIO
from ..configs import Configurations
from ..system import System
from .fcst import ScheduledForecast


class NMM(ScheduledForecast):
    """
    Subclass of the Forecast class representing the Meteoblue
    NMM (Nonhydrostatic Meso-Scale Modelling) weather forecast model.
    
    Model data corresponds to 4km resolution forecasts.
    """

    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)

        # TODO: Add sanity check
        self.name = configs.get('Meteoblue', 'name')
        self.address = configs.get('Meteoblue', 'address')
        self.apikey = configs.get('Meteoblue', 'apikey')

        self._variables = {
            'sunshine':                     'sunshinetime',
            'daylight':                     'isdaylight',
            'temp_air':                     'temperature',
            'temp_felt':                    'felttemperature',
            'wind_speed':                   'windspeed',
            'wind_direction':               'winddirection',
            'relative_humidity':            'relativehumidity',
            'pressure_sea':                 'sealevelpressure',
            'precipitation_convective':     'convective_precipitation',
            'precipitation_probability':    'precipitation_probability',
            'snow_fraction':                'snowfraction',
            'clouds_low':                   'lowclouds',
            'clouds_mid':                   'midclouds',
            'clouds_high':                  'highclouds',
            'clouds_total':                 'totalcloudcover',
            'uv_index':                     'uvindex',
            'gni':                          'gni_backwards',
            'dni':                          'dni_backwards',
            'ghi':                          'ghi_backwards',
            'dhi':                          'dif_backwards',
            'dhi_instant':                  'dif_instant',
            'etr':                          'extraterrestrialradiation_backwards',
            'etr_instant':                  'extraterrestrialradiation_instant'
        }

        self._variables_output = [
            'sunshine',                     # Sonnenscheindauer [min]
            'daylight',                     # Tageslicht Indikator
            'temp_air',                     # Temperatur [°C]
            'temp_felt',                    # Gefühlte Temperatur [°C]
            'wind_speed',                   # Windgeschwindigkeit [m/s]
            'wind_direction',               # Windrichtung [°]
            'gni',                          # Average Global Normal Irradiance of the last interval [W/m^2]
            'dni',                          # Average Direct Normal Irradiance of the last interval [W/m^2]
            'ghi',                          # Average Global Horizontal Irradiance of the last interval [W/m^2]
            'dhi',                          # Average Diffuse Horizontal Irradiance of the last interval [W/m^2]
            'etr',                          # Average Extraterrestrial Solar Radiation of the last interval [W/m^2]
            'dni_instant',                  # Global Normal Irradiance at the exact time [W/m^2]
            'gni_instant',                  # Global Normal Irradiance at the exact time [W/m^2]
            'ghi_instant',                  # Global Horizontal Irradiance at the exact time [W/m^2]
            'dhi_instant',                  # Diffuse Horizontal Irradiance at the exact time [W/m^2]
            'etr_instant',                  # Extraterrestrial Solar Radiation at the exact time [W/m^2]
            'clouds_total',                 # Gesamtbedeckungsgrad mit Wolken [%]
            'clouds_low',                   # Bedeckungsgrad mit niedrigen Wolken [%]
            'clouds_mid',                   # Bedeckungsgrad mit mittleren Wolken [%]
            'clouds_high',                  # Bedeckungsgrad mit hohen Wolken [%]
            'visibility',                   # Sichtweite [km]
            'uv_index',                     # UV index numbers from 1 to 16
            'relative_humidity',            # Relative Luftfeuchtigkeit [%]
            'pressure_sea',                 # Luftdruck auf Hoehe des Meerespiegels [hPa]
            'precipitation',                # Niederschlagsmenge [mm]
            'precipitation_convective',     # Niederschlag als Schauer [mm]
            'precipitation_probability',    # Niederschlagswahrscheinlichkeit [%]
            'snow_fraction'                 # Schneefall [0.0 - 1.0]
        ]

    def __activate__(self, system: System, configs: Configurations) -> None:
        super().__activate__(system, configs)
        self.location = system.location

    # noinspection PyPackageRequirements
    def get_meta(self) -> Dict[str, str]:
        parameters = {
            'name': self.name,
            'tz': self.location.timezone.zone,
            'lat': self.location.latitude,
            'lon': self.location.longitude,
            'asl': self.location.altitude,
            'timeformat': 'iso8601',
            'format': 'json',
            'apikey': self.apikey
        }
        response = requests.get(self.address + 'packages/basic-1h_clouds-1h_solar-1h', params=parameters)

        if response.status_code != 200:
            raise requests.HTTPError("Response returned with error " + str(response.status_code) + ": " +
                                     response.reason)

        data = json.loads(response.text)
        return data.get('metadata')

    def predict(self, *_) -> pd.DataFrame:
        data = self._rename(self._request())
        return data[self._variables_output]

    # noinspection PyPackageRequirements
    def _request(self) -> pd.DataFrame:
        """
        Submits a query to the meteoblue servers and
        converts the CSV response to a pandas DataFrame.
        
        Returns
        -------
        data : DataFrame
            column names are the weather model's variable names.
        """
        parameters = {
            'name': self.name,
            'tz': self.location.timezone.zone,
            'lat': self.location.latitude,
            'lon': self.location.longitude,
            'asl': self.location.altitude,
            'temperature': 'C',
            'windspeed': 'ms-1',
            'winddirection': 'degree',
            'precipitationamount': 'mm',
            'timeformat': 'iso8601',
            'format': 'csv',
            'apikey': self.apikey
        }
        response = requests.get(self.address + 'packages/basic-1h_clouds-1h_solar-1h', params=parameters)

        if response.status_code != 200:
            raise requests.HTTPError("Response returned with error " + str(response.status_code) + ": " +
                                     response.reason)

        data = pd.read_csv(StringIO(response.text), sep=',')
        data['time'] = pd.to_datetime(data['time'])
        data = data.set_index('time')
        data = data.tz_convert(self.location.timezone)
        data = data.replace(-999, 0)

        return data
