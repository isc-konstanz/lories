# -*- coding: utf-8 -*-
"""
lories
~~~~~~


"""

from . import _version

__version__ = _version.get_versions().get("version")
del _version


from .core import (  # noqa: F401
    Constant,
    Directory,
    Directories,
    Configurations,
    ConfigurationError,
    ConfigurationUnavailableError,
    Configurator,
    Resource,
    Resources,
    ResourceError,
    ResourceUnavailableError,
)

from . import data  # noqa: F401
from .data import (  # noqa: F401
    ChannelState,
    Channel,
    Channels,
    Converter,
    ConversionError,
    Listener,
)

from . import connectors  # noqa: F401
from .connectors import (  # noqa: F401
    Connector,
    ConnectorError,
    ConnectionError,
    Database,
    DatabaseException,
    DatabaseUnavailableException,
    Databases,
)

from . import components  # noqa: F401
from .components import (  # noqa: F401
    Component,
    ComponentError,
    ComponentUnavailableError,
)

from . import location  # noqa: F401
from .location import (  # noqa: F401
    Location,
    LocationException,
    LocationUnavailableException,
)

from .components import (  # noqa: F401
    Tariff,
    Weather,
)

from .settings import Settings  # noqa: F401

from . import system  # noqa: F401
from .system import System  # noqa: F401

from . import simulation  # noqa: F401
from .simulation import (  # noqa: F401
    Durations,
    Progress,
    Results,
)

from . import io  # noqa: F401

from . import application  # noqa: F401
from .application import Application  # noqa: F401


def load(name: str = "Lories", **kwargs) -> Application:
    return Application.load(name, **kwargs)
