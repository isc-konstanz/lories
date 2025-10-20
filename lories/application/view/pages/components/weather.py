# -*- coding: utf-8 -*-
"""
lories.application.view.pages.components.weather
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lories.application.view.pages import ComponentPage, register_component_group, register_component_page
from lories.components.weather import WeatherProvider


@register_component_page(WeatherProvider)
@register_component_group(WeatherProvider, name="Weather")
class WeatherPage(ComponentPage[WeatherProvider]):
    pass
