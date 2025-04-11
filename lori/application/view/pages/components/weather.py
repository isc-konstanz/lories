# -*- coding: utf-8 -*-
"""
lori.application.view.pages.components.weather
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lori.application.view.pages import ComponentPage, register_component_group, register_component_page
from lori.components.weather import WeatherProvider


@register_component_page(WeatherProvider)
@register_component_group(WeatherProvider, name="Weather")
class WeatherPage(ComponentPage[WeatherProvider]):
    pass
