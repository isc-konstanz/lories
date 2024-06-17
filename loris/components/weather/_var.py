# -*- coding: utf-8 -*-
"""
loris.components.weather
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from loris.components.weather import Weather

DEPRECATED = {
    "total_clouds": "cloud_cover",
    "low_clouds": "clouds_low",
    "mid_clouds": "clouds_mid",
    "high_clouds": "clouds_high",
    "wind_gust": "wind_speed_gust",
    "humidity_rel": "relative_humidity",
}

WEATHER = {
    Weather.GHI:                 "Global Horizontal Irradiance [W/m2]",
    Weather.DNI:                 "Direct Normal Irradiance [W/m2]",
    Weather.DHI:                 "Diffuse Horizontal Irradiance [W/m2]",
    Weather.TEMP_AIR:            "Air Temperature [°C]",
    Weather.TEMP_DEW_POINT:      "Dewpoint Temperature [°C]",
    Weather.HUMIDITY_REL:        "Relative Humidity [%]",
    Weather.PRESSURE_SEA:        "Atmospheric Pressure [hPa]",
    Weather.WIND_SPEED:          "Wind Speed [km/h]",
    Weather.WIND_SPEED_GUST:     "Wind Gust Speed [km/h]",
    Weather.WIND_DIRECTION:      "Wind Direction [°]",
    Weather.WIND_DIRECTION_GUST: "Wind Gust Direction [°]",
    Weather.CLOUD_COVER:         "Total Cloud Cover [%]",
    Weather.CLOUDS_LOW:          "Low Cloud Cover [%]",
    Weather.CLOUDS_MID:          "Medium Cloud Cover [%]",
    Weather.CLOUDS_HIGH:         "High Cloud Cover [%]",
    Weather.SUNSHINE:            "Sunshine duration [min]",
    Weather.VISIBILITY:          "Visibility [m]",
    Weather.PRECIPITATION:       "Precipitation [mm]",
    Weather.PRECIPITATION_CONV:  "Precipitation Convective [mm]",
    Weather.PRECIPITATION_PROB:  "Precipitation Probability [%]",
    Weather.PRECIPITABLE_WATER:  "Precipitable water [cm]",
    Weather.SNOW_FRACTION:       "Snow Fraction [1/0]",
}
