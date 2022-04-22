# -*- coding: utf-8 -*-
"""
    th-e-core.io._var
    ~~~~~~~~~~~~~~~~~
    
    
"""
from th_e_core.system import System
from th_e_core.pv.system import PVSystem


_DEPRECATION = {
    'import_power': 'el_imp_power',
    'export_power': 'el_exp_power',
    'import_energy': 'el_imp_energy',
    'export_energy': 'el_exp_energy'
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

_PV_POWER = {
    PVSystem.POWER:     'Generated Photovoltaic Power [W]',
    PVSystem.POWER_EXP: 'Exported Photovoltaic Power [W]'
}

_PV_ENERGY = {
    PVSystem.ENERGY:     'Generated Photovoltaic Energy [kWh]',
    PVSystem.ENERGY_EXP: 'Exported Photovoltaic Energy [kWh]'
}

PV = {
    **_PV_POWER,
    **_PV_ENERGY
}

WEATHER = {
    'ghi':                       'Global Horizontal Irradiance [W/m2]',
    'dni':                       'Direct Normal Irradiance [W/m2]',
    'dhi':                       'Diffuse Horizontal Irradiance [W/m2]',
    'temp_air':                  'Air Temperature [°C]',
    'humidity_rel':              'Relative Humidity [%]',
    'pressure_sea':              'Atmospheric Pressure [hPa]',
    'wind_speed':                'Wind Speed [km/h]',
    'wind_gust':                 'Wind Gust [km/h]',
    'wind_direction':            'Wind Direction [°]',
    'total_clouds':              'Total Cloud Cover [%]',
    'low_clouds':                'Low Cloud Cover [%]',
    'mid_clouds':                'Medium Cloud Cover [%]',
    'high_clouds':               'High Cloud Cover [%]',
    'precipitation':             'Precipitation [mm]',
    'precipitation_convective':  'Precipitation Convective [mm]',
    'precipitation_probability': 'Precipitation Probability [%]',
    'snow_fraction':             'Snow Fraction [1/0]'
}

POWER = {
    **_SYSTEM_POWER,
    **_PV_POWER
}

ENERGY = {
    **_SYSTEM_ENERGY,
    **_PV_ENERGY
}

COLUMNS = {
    **POWER,
    **ENERGY,
    **WEATHER
}
