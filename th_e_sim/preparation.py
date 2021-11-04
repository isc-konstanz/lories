# -*- coding: utf-8 -*-
"""
    th-e-sim.preparation
    ~~~~~
    
    
"""
import logging
logger = logging.getLogger(__name__)

import os
import pytz as tz
import numpy as np
import pandas as pd
import datetime as dt
import dateutil.relativedelta as rd


def process_weather(system, meteoblue_libs='Meteoblue'):
    lat = system._configs.getfloat('Location', 'latitude')
    lon = system._configs.getfloat('Location', 'longitude')
    loc = '{0:06.2f}'.format(lat).replace('.', '') + '_' \
          + '{0:06.2f}'.format(lon).replace('.', '')

    weather = system.forecast._weather._configs
    weather_dir = os.path.join(weather.get('Database', 'dir'))
    weather_libs = os.path.join(weather.get('General', 'lib_dir'), 'weather')
    weather_lib = os.path.join(weather_libs, loc)

    if os.path.isdir(weather_dir):
        return

    os.makedirs(weather_dir, exist_ok=True)

    if not os.path.exists(weather_lib) and \
            os.path.exists(weather_libs):
        os.mkdir(weather_lib)

    logger.info('Preparing weather files for {0} in: {1}'.format(system.name, weather_dir))

    meteoblue_lib = os.path.join(meteoblue_libs, 'Locations', loc)
    if not os.path.isdir(meteoblue_libs):
        raise Exception("Unable to access meteoblue directory: {0}".format(meteoblue_libs))

    infos = []
    files = []

    for entry in os.scandir(meteoblue_libs):
        if entry.is_file() and entry.path.endswith('.csv'):
            info = pd.read_csv(entry.path, skipinitialspace=True, sep=';', header=None, index_col=[0],
                               low_memory=False).iloc[:18, :]
            info.columns = info.iloc[3]

            infos.append(info.loc[:, ~info.columns.duplicated()].dropna(axis=1, how='all'))
            files.append(pd.read_csv(entry.path, skipinitialspace=True, sep=';', header=[18], index_col=[0, 1, 2, 3, 4]))

    points = pd.concat(infos, axis=0).drop_duplicates()
    histories = pd.concat(files, axis=1)
    for point in points.columns.values:
        if abs(lat - float(points.loc['LAT', point]) > 0.001) or \
           abs(lon - float(points.loc['LON', point]) > 0.001):
            continue

        columns = [column for column in histories.columns.values if column.startswith(point + ' ')]
        history = histories[columns]
        history.columns = [c.replace(c.split(' ')[0], '').replace(c.split('[')[1], '').replace('  [', '') for c in columns]
        history['time'] = [dt.datetime(y, m, d, h, n) for y, m, d, h, n in history.index]
        history.set_index('time', inplace=True)
        history.index = history.index.tz_localize(tz.utc)
        # history.index = history.index.tz_convert('Europe/Berlin')
        history.rename(columns={' Temperature': 'temp_air', 
                                ' Wind Speed': 'wind_speed', 
                                ' Wind Direction': 'wind_direction', 
                                ' Wind Gust': 'wind_gust', 
                                ' Relative Humidity': 'humidity_rel', 
                                ' Mean Sea Level Pressure': 'pressure_sea', 
                                ' Shortwave Radiation': 'ghi', 
                                ' DNI - backwards': 'dni', 
                                ' DIF - backwards': 'dhi', 
                                ' Total Cloud Cover': 'total_clouds', 
                                ' Low Cloud Cover': 'low_clouds', 
                                ' Medium Cloud Cover': 'mid_clouds', 
                                ' High Cloud Cover': 'high_clouds', 
                                ' Total Precipitation': 'precipitation', 
                                ' Snow Fraction': 'snow_fraction'}, inplace=True)

        if os.path.isdir(meteoblue_lib):
            fill_start = history.index[-1]
            fill_end = None

            # Delete unavailable column of continuous forecasts
            del history['wind_gust']

            for file in sorted(os.listdir(meteoblue_lib)):
                path = os.path.join(meteoblue_lib, file)
                if os.path.isfile(path) and file.endswith('.csv'):
                    forecast = pd.read_csv(path, index_col='time', parse_dates=['time'])
                    forecast.columns = ['temp_air', 'wind_speed', 'wind_direction', 'humidity_rel', 'pressure_sea', 
                                        'ghi', 'dni', 'dhi', 'total_clouds', 'low_clouds', 'mid_clouds', 'high_clouds', 
                                        'precipitation', 'precipitation_convective', 'precipitation_probability', 
                                        'snow_fraction']

                    time = forecast.index[0]
                    if time.hour == 0 or time.hour == 1:
                        if fill_end is None or time < fill_end:
                            fill_end = time

                        history = forecast.loc[time:time+dt.timedelta(hours=23), history.columns].combine_first(history)

                    if os.path.exists(weather_lib):
                        forecast_file = os.path.join(weather_lib, file)
                        if not os.path.exists(forecast_file):
                            forecast = forecast.resample('1Min').interpolate(method='akima')
                            forecast[forecast[['ghi', 'dni', 'dhi']] < 0] = 0
                            forecast.to_csv(forecast_file, sep=',', encoding='utf-8') #, date_format='%Y-%m-%d %H:%M:%S')

            fill_offset = rd.relativedelta(years=10)
            fill_data = history[fill_start-fill_offset:fill_end-fill_offset]
            fill_leap = (fill_data.index.month == 2) & (fill_data.index.day == 29)
            if fill_leap.any():
                fill_data = fill_data.drop(fill_data[fill_leap].index)
            fill_data.index = fill_data.index.map(lambda dt: dt.replace(year=dt.year+10))

            history = history.combine_first(fill_data)

        # Upsample forecast to a resolution of 1 minute. Use the advanced Akima interpolator for best results
        history = history.resample('1Min').interpolate(method='akima')
        history[history[['ghi', 'dni', 'dhi']] < 0] = 0

        time = history.index[0]
        while time <= history.index[-1]:
            day = history[time:time+dt.timedelta(hours=23, minutes=59)]
            day.to_csv(os.path.join(weather_dir, time.strftime('%Y%m%d_%H%M%S') + '.csv'), sep=',', encoding='utf-8')

            if os.path.exists(weather_lib):
                loc_file = os.path.join(weather_lib, time.strftime('%Y%m%d_%H%M%S') + '.csv');
                if not os.path.exists(loc_file):
                    day.to_csv(loc_file, sep=',', encoding='utf-8')

            time = time + dt.timedelta(hours=24)


def process_system(system, opsd_libs='OPSD'):
    lat = system._configs.getfloat('Location', 'latitude')
    lon = system._configs.getfloat('Location', 'longitude')
    loc = '{0:08.4f}'.format(lat).replace('.', '') + '_' + \
          '{0:08.4f}'.format(lon).replace('.', '')

    system_dir = system._configs.get('Database', 'dir')
    system_libs = os.path.join(system._configs.get('General', 'lib_dir'), 'systems')
    system_lib = os.path.join(system_libs, loc)

    if os.path.isdir(system_dir):
        return

    os.makedirs(system_dir, exist_ok=True)

    logger.info('Preparing system files for {0} in: {1}'.format(system.name, system_dir))

    if not os.path.exists(system_lib) and \
            os.path.exists(system_libs):
        os.mkdir(system_lib)

    if not os.path.isdir(opsd_libs):
        raise Exception("Unable to access OPSD directory: {0}".format(opsd_libs))

    index = 'utc_timestamp'
    data = pd.read_csv(os.path.join(opsd_libs, 'household_data_1min.csv'),
                       skipinitialspace=True, low_memory=False, sep=',',
                       index_col=[index], parse_dates=[index])

    data.index.rename('time', inplace=True)
    data = data.filter(regex=(system.id)).dropna(how='all')
    for column in data.columns:
        column_name = column.split(system.id + '_', 1)[1] + '_energy'
        data.rename(columns={column: column_name}, inplace=True)

    columns_power = ['import_power', 'export_power']
    columns_energy = ['import_energy', 'export_energy']

    data['import_energy'] = _process_energy(data['grid_import_energy'])
    data['import_power'] = _process_power(data['grid_import_energy'])

    data['export_energy'] = _process_energy(data['grid_export_energy'])
    data['export_power'] = _process_power(data['grid_export_energy'])

    if 'pv_energy' in data.columns:
        columns_power.append('pv_power')
        columns_energy.append('pv_energy')
        data['pv_energy'] = _process_energy(data['pv_energy'])
        data['pv_power'] = _process_power(data['pv_energy'])

    columns_power.append('el_power')
    columns_energy.append('el_energy')
    data['el_energy'] = data['import_energy']
    if 'pv_energy' in data.columns:
        pv_cons = data['pv_energy'] - data['export_energy']
        data['el_energy'] += pv_cons

    if 'heat_pump_energy' in data.columns:
        columns_power += ['th_power', 'hp_power']
        columns_energy += ['th_energy', 'hp_energy']

        data['hp_energy'] = _process_energy(data['heat_pump_energy'])
        data['hp_power'] = _process_power(data['heat_pump_energy'])

        data['el_energy'] -= data['hp_energy']

        # TODO: Make COP more sophisticated
        # Maybe try to differentiate between heating and warm water
        cop = 3.5
        data['th_power'] = _process_power(data['heat_pump_energy']) * cop  # , filter=False)
        
        # Offset and widening of thermal power from heat pump power, smoothen peaks and reduce offset again
        data_back = data['th_power'].iloc[::-1]
        data_back = data_back.rolling(window=200).mean()  
        data_front = data_back.rolling(window=50, win_type="gaussian", center=True).mean(std=15).iloc[::-1]
        data['th_power'] = data_front.rolling(window=150).mean().ffill().bfill()

        data_time = pd.DataFrame(index=data.index, data=data.index)
        data_time.columns = ['date']
        data_time['hours'] = (data_time['date'] - data_time['date'].shift(1))/np.timedelta64(1, 'h')

        data['th_energy'] = (data['th_power']/1000 * data_time['hours']).fillna(0).cumsum()

    data['el_power'] = _process_power(data['el_energy'])

    data = data[columns_power + columns_energy]
    time = data.index[0]
    time = time.replace(hour=0, minute=0) + dt.timedelta(hours=24)
    while time <= data.index[-1]:
        day = data[time:time+dt.timedelta(hours=23, minutes=59)]
        day.to_csv(os.path.join(system_dir, time.strftime('%Y%m%d_%H%M%S') + '.csv'), sep=',', encoding='utf-8')

        if os.path.exists(system_lib):
            day_file = os.path.join(system_lib, time.strftime('%Y%m%d_%H%M%S') + '.csv')
            if not os.path.exists(day_file):
                day.to_csv(day_file, sep=',', encoding='utf-8')

        time = time + dt.timedelta(hours=24)


def _process_energy(energy):
    energy = energy.fillna(0)
    return energy - energy[0]


def _process_power(energy, filter=True): #@ReservedAssignment
    delta_energy = energy.diff()
    delta_index = pd.Series(energy.index, index=energy.index)
    delta_index = (delta_index - delta_index.shift(1))/np.timedelta64(1, 'h')

    column_power = (delta_energy/delta_index).fillna(0)*1000

    if filter:
        from scipy import signal
        b, a = signal.butter(1, 0.25)
        column_power = signal.filtfilt(b, a, column_power, method='pad', padtype='even', padlen=15)
        column_power[column_power < 0.1] = 0

    return column_power

