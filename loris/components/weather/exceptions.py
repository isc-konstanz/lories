# -*- coding: utf-8 -*-
"""
    loris.components.weather.exceptions
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""
from loris.components import ComponentException, ComponentUnavailableException


class WeatherException(ComponentException):
    """
    Raise if an error occurred accessing the weather.

    """
    pass


class WeatherUnavailableException(ComponentUnavailableException, WeatherException):
    """
    Raise if a configured weather access can not be found.

    """
    pass
