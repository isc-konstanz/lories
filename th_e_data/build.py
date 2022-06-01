# -*- coding: utf-8 -*-
"""
    th-e-data.build
    ~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from typing import Optional

import logging
import numpy as np
import pandas as pd
import datetime as dt
from pvlib.location import Location
from th_e_core.io.csv import Database
from configparser import ConfigParser as Configurations

logger = logging.getLogger(__name__)


# noinspection PyShadowingBuiltins
def build(configs: Configurations,
          database: Database,
          start: pd.Timestamp | dt.datetime = None,
          end:   pd.Timestamp | dt.datetime = None, **kwargs) -> Optional[pd.DataFrame]:
    if database is None:
        return

    if not configs.has_section('Data'):
        return

    buildargs = dict(configs.items('Data'))
    buildargs['start'] = start
    buildargs['end'] = end
    buildargs.update(kwargs)

    if database.exists(**buildargs):
        return

    type = configs.get('Data', 'type', fallback=None).strip()
    # types = configs.get('Data', 'type', fallback='default')
    # for type in types.split(','):
    #     type = type.strip().lower()

    if start is not None and end is not None:
        logger.info('Building %s data from %s to %s', type,
                    start.strftime('%d.%m.%Y %H:%M'),
                    end.strftime('%d.%m.%Y %H:%M'))
    else:
        logger.info('Building %s data', type)

    if type.lower() in 'lpg':
        data = build_lpg(**buildargs)
    elif type.lower() == 'opsd':
        data = build_opsd(**buildargs)
    elif type.lower() == 'meteoblue':
        data = build_meteoblue(**buildargs)
    else:
        raise ValueError('Invalid data build type: {}'.format(type))

    if database.enabled and data is not None and not data.empty:
        database.write(data, split_data=True, **kwargs)

    return data


def build_lpg(key: str = None, weather: pd.DataFrame = None, **kwargs) -> pd.DataFrame:
    from th_e_core.system import System
    from th_e_data.process import process_lpg

    data = pd.DataFrame()
    if key is None:
        raise ValueError('The LPG key needs to be configured')

    for k in key.split(','):
        d = process_lpg(key=k.strip(), **kwargs)
        if d is not None and not d.empty:
            data = data.combine_first(d)

    # TODO: iterate full years and calculate annual heating demand
    data_time = pd.DataFrame(index=data.index, data=data.index)
    data_time.columns = ['date']
    data_time['hours'] = ((data_time['date'] - data_time['date'].shift(1)) / np.timedelta64(1, 'h')).bfill()

    data_th_ht = _build_oemof(data.loc[data.index[-1], System.ENERGY_TH_HT], weather, **kwargs)

    data[System.POWER_TH_HT] = (data_th_ht/data_time['hours']*1000).interpolate(method='akima')

    data[System.POWER_TH] = data[System.POWER_TH_HT] + data[System.POWER_TH_DOM]
    data[System.ENERGY_TH] = (data[System.POWER_TH]/1000*data_time['hours']).cumsum()
    data[System.ENERGY_TH_HT] = data_th_ht.cumsum()

    return data


def build_opsd(**kwargs) -> pd.DataFrame:
    from th_e_data.process import process_opsd
    return process_opsd(**kwargs)


def build_meteoblue(location: Location, **kwargs) -> pd.DataFrame:
    if 'latitude' not in kwargs:
        kwargs['latitude'] = location.latitude
    if 'longitude' not in kwargs:
        kwargs['longitude'] = location.longitude

    from th_e_data.process import process_meteoblue
    return process_meteoblue(**kwargs)


# noinspection PyShadowingBuiltins, PyPackageRequirements, SpellCheckingInspection
def _build_oemof(annual_demand: float,
                 weather: pd.DataFrame,
                 country: str = 'DE/BW',
                 building_type: str = "EFH",
                 building_class: int = 1,
                 wind_class: int = 1, **_) -> pd.Series:
    import demandlib.bdew as bdew
    import holidays as hl

    # TODO: sanitize country parsing
    country = country.split('/')
    years = list(dict.fromkeys(weather.index.year))
    holidays = hl.country_holidays(country[0], subdiv=country[1], years=years)

    data = bdew.HeatBuilding(weather.index,
                             holidays=holidays,
                             temperature=weather['temp_air'],
                             annual_heat_demand=annual_demand,
                             shlp_type=building_type,
                             building_class=int(building_class),
                             wind_class=int(wind_class),
                             ww_incl=False).get_bdew_profile()

    data_res = int(3600/(data.index[1] - data.index[0]).seconds)
    data = data.rolling(window=data_res, win_type="gaussian", center=True).mean(std=20).fillna(data)

    return data
