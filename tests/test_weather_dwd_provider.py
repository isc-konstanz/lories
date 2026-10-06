# -*- coding: utf-8 -*-
"""
tests.test_weather_dwd_provider
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Bright Sky reports precipitation and sunshine as totals over the previous hour. The provider declares them as
hourly rates, so they keep their meaning at any row spacing and are averaged, not summed, when resampled.
"""

import pytest

from lories.components.weather.dwd.provider import build_channels


@pytest.mark.parametrize("source", ["forecast", "current, historical"])
@pytest.mark.parametrize(("key", "unit"), [("precipitation", "mm/h"), ("sunshine", "min/h")])
def test_hourly_totals_are_declared_as_rates_and_averaged(source, key, unit):
    channels = {c["key"]: c for c in build_channels(connector="brightsky", source=source)}

    assert (channels[key]["unit"], channels[key]["aggregate"]) == (unit, "mean")
