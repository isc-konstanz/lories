# -*- coding: utf-8 -*-
"""
    corsys.io._var
    ~~~~~~~~~~~~~~
    
    
"""
from ..system import System
from ..weather import Weather
from ..cmpt import ElectricalEnergyStorage, ThermalEnergyStorage, Photovoltaic


_DEPRECATION = {
    'import_power': 'el_imp_power',
    'export_power': 'el_exp_power',
    'import_energy': 'el_imp_energy',
    'export_energy': 'el_exp_energy',
    'total_clouds': 'cloud_cover',
    'low_clouds': 'clouds_low',
    'mid_clouds': 'clouds_mid',
    'high_clouds': 'clouds_high',
    'wind_gust': 'wind_speed_gust'
}

_SYSTEM_POWER = {
    System.POWER_EL:     'Total Electrical Power [W]',
    System.POWER_EL_IMP: 'Imported Electrical Power [W]',
    System.POWER_EL_EXP: 'Exported Electrical Power [W]',
    System.POWER_TH:     'Total Thermal Power [W]',
    System.POWER_TH_HT:  'Heating Water Thermal Power [W]',
    System.POWER_TH_DOM: 'Domestic Water Thermal Power [W]',
}

_SYSTEM_ENERGY = {
    System.ENERGY_EL:     'Total Electrical Energy [kWh]',
    System.ENERGY_EL_IMP: 'Imported Electrical Energy [kWh]',
    System.ENERGY_EL_EXP: 'Exported Electrical Energy [kWh]',
    System.ENERGY_TH:     'Total Thermal Energy [kWh]',
    System.ENERGY_TH_HT:  'Heating Water Thermal Energy [kWh]',
    System.ENERGY_TH_DOM: 'Domestic Water Thermal Energy [kWh]',
}

SYSTEM = {
    **_SYSTEM_POWER,
    **_SYSTEM_ENERGY
}

_COMPONENTS_POWER = {
    ElectricalEnergyStorage.POWER_CHARGE: 'EES Charging Power [W]',
    ElectricalEnergyStorage.POWER_DISCHARGE: 'EES Discharging Power [W]'
}

_COMPONENTS_ENERGY = {
    ElectricalEnergyStorage.ENERGY_CHARGE:    'EES Charged Energy [kWh]',
    ElectricalEnergyStorage.ENERGY_DISCHARGE: 'EES Discharged Energy [kWh]'
}

_PV_POWER = {
    Photovoltaic.POWER:     'Generated PV Power [W]',
    Photovoltaic.POWER_EXP: 'Exported PV Power [W]'
}

_PV_ENERGY = {
    Photovoltaic.ENERGY:     'Generated PV Energy [kWh]',
    Photovoltaic.ENERGY_EXP: 'Exported PV Energy [kWh]'
}

PV = {
    **_PV_POWER,
    **_PV_ENERGY
}

_DC_POWER = {
    'dc_power': 'Generated DC Power [W]'
}

_DC_ENERGY = {
    'dc_energy': 'Generated DC Energy [kWh]'
}

DC = {
    **_DC_POWER,
    **_DC_ENERGY
}

STATES = {
    ElectricalEnergyStorage.STATE_OF_CHARGE: 'EES State of Charge [%]',
    ThermalEnergyStorage.TEMPERATURE:        'TES Temperature [°C]'
}

_AC_POWER = {
    'active_power':      'Total Active Power [W]',
    'l1_active_power':   'Phase 1 Active Power [W]',
    'l2_active_power':   'Phase 2 Active Power [W]',
    'l3_active_power':   'Phase 3 Active Power [W]',
    'reactive_power':    'Total Reactive Power [W]',
    'l1_reactive_power': 'Phase 1 Reactive Power [W]',
    'l2_reactive_power': 'Phase 2 Reactive Power [W]',
    'l3_reactive_power': 'Phase 3 Reactive Power [W]',
    'apparent_power':    'Total Apparent Power [W]',
    'l1_apparent_power': 'Phase 1 Apparent Power [W]',
    'l2_apparent_power': 'Phase 2 Apparent Power [W]',
    'l3_apparent_power': 'Phase 3 Apparent Power [W]',
    'import_power':      'Total Imported Power [W]',
    'l1_import_power':   'Phase 1 Imported Power [W]',
    'l2_import_power':   'Phase 2 Imported Power [W]',
    'l3_import_power':   'Phase 3 Imported Power [W]',
    'export_power':      'Total Exported Power [W]',
    'l1_export_power':   'Phase 1 Exported Power [W]',
    'l2_export_power':   'Phase 2 Exported Power [W]',
    'l3_export_power':   'Phase 3 Exported Power [W]'
}

_AC_ENERGY = {
    'active_energy':      'Total Active Energy [kWh]',
    'l1_active_energy':   'Phase 1 Active Energy [kWh]',
    'l2_active_energy':   'Phase 2 Active Energy [kWh]',
    'l3_active_energy':   'Phase 3 Active Energy [kWh]',
    'reactive_energy':    'Total Reactive Energy [kWh]',
    'l1_reactive_energy': 'Phase 1 Reactive Energy [kWh]',
    'l2_reactive_energy': 'Phase 2 Reactive Energy [kWh]',
    'l3_reactive_energy': 'Phase 3 Reactive Energy [kWh]',
    'apparent_energy':    'Total Apparent Energy [kWh]',
    'l1_apparent_energy': 'Phase 1 Apparent Energy [kWh]',
    'l2_apparent_energy': 'Phase 2 Apparent Energy [kWh]',
    'l3_apparent_energy': 'Phase 3 Apparent Energy [kWh]',
    'import_energy':      'Total Imported Energy [kWh]',
    'l1_import_energy':   'Phase 1 Imported Energy [kWh]',
    'l2_import_energy':   'Phase 2 Imported Energy [kWh]',
    'l3_import_energy':   'Phase 3 Imported Energy [kWh]',
    'export_energy':      'Total Exported Energy [kWh]',
    'l1_export_energy':   'Phase 1 Exported Energy [kWh]',
    'l2_export_energy':   'Phase 2 Exported Energy [kWh]',
    'l3_export_energy':   'Phase 3 Exported Energy [kWh]'
}

AC = {
    'l1_voltage': 'Phase 1 Voltage [V]',
    'l2_voltage': 'Phase 2 Voltage [V]',
    'l3_voltage': 'Phase 3 Voltage [V]',
    'l1_current': 'Phase 1 Current [A]',
    'l2_current': 'Phase 2 Current [A]',
    'l3_current': 'Phase 3 Current [A]',
    'frequency':  'Frequency [Hz]'
}

WEATHER = {
    Weather.GHI:                 'Global Horizontal Irradiance [W/m2]',
    Weather.DNI:                 'Direct Normal Irradiance [W/m2]',
    Weather.DHI:                 'Diffuse Horizontal Irradiance [W/m2]',
    Weather.TEMP_AIR:            'Air Temperature [°C]',
    Weather.TEMP_DEW_POINT:      'Dewpoint Temperature [°C]',
    Weather.HUMIDITY_REL:        'Relative Humidity [%]',
    Weather.PRESSURE_SEA:        'Atmospheric Pressure [hPa]',
    Weather.WIND_SPEED:          'Wind Speed [km/h]',
    Weather.WIND_SPEED_GUST:     'Wind Gust Speed [km/h]',
    Weather.WIND_DIRECTION:      'Wind Direction [°]',
    Weather.WIND_DIRECTION_GUST: 'Wind Gust Direction [°]',
    Weather.CLOUD_COVER:         'Total Cloud Cover [%]',
    Weather.CLOUDS_LOW:          'Low Cloud Cover [%]',
    Weather.CLOUDS_MID:          'Medium Cloud Cover [%]',
    Weather.CLOUDS_HIGH:         'High Cloud Cover [%]',
    Weather.SUNSHINE:            'Sunshine duration [min]',
    Weather.VISIBILITY:          'Visibility [m]',
    Weather.PRECIPITATION:       'Precipitation [mm]',
    Weather.PRECIPITATION_CONV:  'Precipitation Convective [mm]',
    Weather.PRECIPITATION_PROB:  'Precipitation Probability [%]',
    Weather.PRECIPITABLE_WATER:  'Precipitable water [cm]',
    Weather.SNOW_FRACTION:       'Snow Fraction [1/0]'
}

SOLAR = {
    'solar_elevation': 'Solar Elevation [°]',
    'solar_zenith':    'Solar Zenith [°]',
    'solar_azimuth':   'Solar Azimuth [°]'
}

TIME = {
    'hour':        'Hour',
    'day_of_week': 'Day of the Week',
    'day_of_year': 'Day of the Year',
    'month':       'Month',
    'year':        'Year'
}

POWER = {
    **_COMPONENTS_POWER,
    **_SYSTEM_POWER,
    **_PV_POWER,
    **_DC_POWER,
    **_AC_POWER
}

ENERGY = {
    **_COMPONENTS_ENERGY,
    **_SYSTEM_ENERGY,
    **_PV_ENERGY,
    **_DC_ENERGY,
    **_AC_ENERGY
}

COLUMNS = {
    **STATES,
    **POWER,
    **ENERGY,
    **AC,
    **WEATHER,
    **SOLAR,
    **TIME
}


def rename(name: str) -> str:
    if name in COLUMNS:
        return COLUMNS[name]
    return name.title()
