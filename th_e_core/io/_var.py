# -*- coding: utf-8 -*-
"""
    th-e-core.io._var
    ~~~~~~~~~~~~~~~~~
    
    
"""
from th_e_core.system import System
from th_e_core.cmpt import ElectricalEnergyStorage, ThermalEnergyStorage, Photovoltaics


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

_COMPONENTS_POWER = {
    ElectricalEnergyStorage.POWER_CHARGE: 'EES charging power [W]'
}

_COMPONENTS_ENERGY = {
    ElectricalEnergyStorage.ENERGY_CHARGE:    'EES charged energy [kWh]',
    ElectricalEnergyStorage.ENERGY_DISCHARGE: 'EES discharged energy [kWh]'
}

_PV_POWER = {
    Photovoltaics.POWER: 'Generated PV Power [W]',
    Photovoltaics.POWER_EXP: 'Exported PV Power [W]'
}

_PV_ENERGY = {
    Photovoltaics.ENERGY: 'Generated PV Energy [kWh]',
    Photovoltaics.ENERGY_EXP: 'Exported PV Energy [kWh]'
}

PV = {
    **_PV_POWER,
    **_PV_ENERGY
}

STATES = {
    ElectricalEnergyStorage.STATE_OF_CHARGE: 'EES State of Charge [%]',
    ThermalEnergyStorage.TEMPERATURE:        'TES temperature [°C]'
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
    **_COMPONENTS_POWER,
    **_SYSTEM_POWER,
    **_PV_POWER
}

ENERGY = {
    **_COMPONENTS_ENERGY,
    **_SYSTEM_ENERGY,
    **_PV_ENERGY
}

COLUMNS = {
    **STATES,
    **POWER,
    **ENERGY,
    **WEATHER
}
