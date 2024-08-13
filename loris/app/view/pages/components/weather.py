# -*- coding: utf-8 -*-
"""
loris.app.view.pages.components.weather
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from loris import Weather
from loris.app.view.pages import ComponentGroup, ComponentPage, register_component_group, register_component_page


@register_component_page(Weather)
class WeatherPage(ComponentPage[Weather]):
    pass


@register_component_group(Weather)
class WeatherGroup(ComponentGroup[Weather]):
    pass
