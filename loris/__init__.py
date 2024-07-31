# -*- coding: utf-8 -*-
"""
loris
~~~~~


"""

from . import _version

__version__ = _version.get_versions().get("version")
del _version

from .exceptions import (  # noqa: F401
    ResourceException,
    ResourceUnavailableException,
)

from .core import (  # noqa: F401
    Directory,
    Directories,
    Configurator,
    Configurations,
    ConfigurationException,
    ConfigurationUnavailableException,
    Context,
    Resource,
    Resources,
)

from .settings import Settings  # noqa: F401

from . import data  # noqa: F401
from .data import (  # noqa: F401
    ChannelState,
    Channel,
    Channels,
)

from . import connectors  # noqa: F401
from .connectors import (  # noqa: F401
    Connector,
    ConnectorException,
    ConnectionException,
)

from .location import (  # noqa: F401
    Location,
    LocationException,
    LocationUnavailableException,
)

from . import components  # noqa: F401
from .components.activator import Activator  # noqa: F401

from .components import (  # noqa: F401
    Component,
    ComponentException,
    ComponentUnavailableException,
)

from .components import weather  # noqa: F401
from .components.weather import (  # noqa: F401
    Weather,
    WeatherException,
    WeatherUnavailableException,
)

from . import system  # noqa: F401
from .system import System  # noqa: F401

from . import io  # noqa: F401

from . import application  # noqa: F401
from .application import Application  # noqa: F401


def load(name: str = "Loris", **kwargs) -> Application:
    return Application.load(name, **kwargs)
