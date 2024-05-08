# -*- coding: utf-8 -*-
"""
    loris._components.weather.exceptions
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""
from loris import ComponentException, ComponentUnavailableException


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
