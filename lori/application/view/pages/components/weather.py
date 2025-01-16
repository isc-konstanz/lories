# -*- coding: utf-8 -*-
"""
lori.application.view.pages.components.weather
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lori.application.view.pages import ComponentPage, register_component_group, register_component_page
from lori.components import WeatherProvider as Weather


@register_component_page(Weather)
@register_component_group(Weather, name="Weather")
class WeatherPage(ComponentPage[Weather]):
    pass
