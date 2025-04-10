# -*- coding: utf-8 -*-
"""
lori
~~~~


"""

from . import _version

__version__ = _version.get_versions().get("version")
del _version


from .core import (  # noqa: F401
    Directory,
    Directories,
    Configurations,
    ConfigurationException,
    ConfigurationUnavailableException,
    Configurator,
    Constant,
    Context,
    Entity,
    Resource,
    Resources,
    ResourceException,
    ResourceUnavailableException,
)

from . import converters  # noqa: F401
from .converters import (  # noqa: F401
    Converter,
    ConversionException,
)

from . import data  # noqa: F401
from .data import (  # noqa: F401
    ChannelState,
    Channel,
    Channels,
    Listener,
)

from . import connectors  # noqa: F401
from .connectors import (  # noqa: F401
    Connector,
    ConnectorException,
    ConnectionException,
    Database,
    DatabaseException,
    DatabaseUnavailableException,
)

from . import components  # noqa: F401
from .components import (  # noqa: F401
    Component,
    ComponentException,
    ComponentUnavailableException,
)

from . import location  # noqa: F401
from .location import (  # noqa: F401
    Location,
    LocationException,
    LocationUnavailableException,
)

from .components import (  # noqa: F401
    Weather,
    WeatherException,
    WeatherUnavailableException,
)

from .settings import Settings  # noqa: F401

from . import system  # noqa: F401
from .system import System  # noqa:

from . import simulation  # noqa: F401
from .simulation import (  # noqa: F401
    Durations,
    Progress,
    Results,
)

from . import io  # noqa: F401

from . import application  # noqa: F401
from .application import Application  # noqa: F401


def load(name: str = "Lori", **kwargs) -> Application:
    return Application.load(name, **kwargs)
