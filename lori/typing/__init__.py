"""
lori.typing
~~~~~~~~~~~


"""

from typing import TypeVar

from ..core.typing import (
    Timestamp,
    Timezone,
    Entity,
    Context,
    ContextArgument,
    Resource,
    Resources,
    ResourcesArgument,
    Channel,
    Channels,
    ChannelsArgument,
    Converter,
    Converters,
    Listener,
    Configurations,
    Configurator,
    Registrator,
    Activator,
    Connector,
    Connectors,
    Database,
    Data,
    Component,
    Components,
)

from ..location import Location as _Location

from ..components import Weather as _Weather
from ..components import Tariff as _Tariff

from ..system import System as _System


Location = TypeVar("Location", bound=_Location)
Weather = TypeVar("Weather", bound=_Weather)
Tariff = TypeVar("Tariff", bound=_Tariff)
System = TypeVar("System", bound=_System)

__all__ = [
    "Timestamp",
    "Timezone",
    "Entity",
    "Context",
    "ContextArgument",
    "Resource",
    "Resources",
    "ResourcesArgument",
    "Channel",
    "Channels",
    "ChannelsArgument",
    "Converter",
    "Converters",
    "Listener",
    "Configurations",
    "Configurator",
    "Registrator",
    "Activator",
    "Connector",
    "Connectors",
    "Database",
    "Data",
    "Component",
    "Components",
    "Location",
    "Weather",
    "Tariff",
    "System",
]
