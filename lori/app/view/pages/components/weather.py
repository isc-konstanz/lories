# -*- coding: utf-8 -*-
"""
lori.app.view.pages.components.weather
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lori import Weather
from lori.app.view.pages import ComponentGroup, ComponentPage, register_component_group, register_component_page


@register_component_page(Weather)
class WeatherPage(ComponentPage[Weather]):
    pass


@register_component_group(Weather)
class WeatherGroup(ComponentGroup[Weather]):
    pass
