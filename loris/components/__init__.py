# -*- coding: utf-8 -*-
"""
loris.components
~~~~~~~~~~~~~~~~


"""

from .activator import (  # noqa: F401
    ActivatorMeta,
    Activator,
)

from .component import (  # noqa: F401
    Component,
    ComponentException,
    ComponentUnavailableException,
)

from . import registry  # noqa: F401
from .registry import ComponentRegistration, register  # noqa: F401

from . import context  # noqa: F401
from .context import ComponentContext  # noqa: F401

from . import weather  # noqa: F401
from .weather import (  # noqa: F401
    Weather,
    WeatherException,
    WeatherUnavailableException,
)

registry.register(Weather, Weather.TYPE, factory=Weather.load)
