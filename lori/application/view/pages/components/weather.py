# -*- coding: utf-8 -*-
"""
lori.application.view.pages.components.weather
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lori.components import WeatherProvider as Weather
from lori.application.view.pages import ComponentGroup, ComponentPage, register_component_group, register_component_page

KEY = "weather"
NAME = "Weather"


@register_component_page(Weather)
class WeatherPage(ComponentPage[Weather]):
    pass


@register_component_group(Weather, key=KEY, name=NAME)
class WeatherGroup(ComponentGroup[Weather]):
    pass
