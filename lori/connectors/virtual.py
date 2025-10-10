# -*- coding: utf-8 -*-
"""
lori.connectors.virtual
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import random
from typing import Optional

import pandas as pd
import pytz as tz
from lori.connectors import Connector, ConnectorError, register_connector_type
from lori.core.configs import ConfigurationError
from lori.typing import Channel, Resource, Resources, Timestamp


# noinspection PyShadowingBuiltins
@register_connector_type("virtual", "random", "dummy")
class VirtualConnector(Connector):
    VIRTUAL: str = "virtual"
    RANDOM: str = "random"

    _data: pd.Series

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        index = []
        data = []
        for resource in resources:
            index.append(resource.id)

            generator = resource.get("generator", default=None)
            if generator == VirtualConnector.RANDOM:
                for attr in ["min", "max"]:
                    if attr not in resource:
                        raise ConfigurationError(f"Invalid dummy channel '{resource.id}', missing attribute: {attr}")
                data.append(float(random.randrange(int(resource.min * 100), int(resource.max * 100))) / 100.0)

            elif generator is not None:
                raise ConfigurationError(f"Invalid dummy channel '{resource.id}' generator: {generator}")
            else:
                data.append(resource.get("default", default=None))
        self._data = pd.Series(index=index, data=data)

    def read(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> pd.DataFrame:
        for resource in resources:
            generator = resource.get("generator", default=VirtualConnector.VIRTUAL)
            if generator == VirtualConnector.RANDOM:
                self._read_random(resource)

            elif generator != VirtualConnector.VIRTUAL:
                raise ConnectorError(self, f"Trying to read dummy channel '{resource.id}' with generator: {generator}")
        return self._data.to_frame(pd.Timestamp.now(tz.UTC).floor(freq="s")).T

    def _read_random(self, resource: Resource) -> None:
        range = int(abs(resource.max - resource.min))
        value = float(random.randrange(-range * 100, range * 100)) / 1000.0 + self._data[resource.id]
        if value < resource.min:
            value = resource.min
        if value > resource.max:
            value = resource.max
        self._data[resource.id] = value

    def write(self, data: pd.DataFrame) -> None:
        for id in data.columns:
            if id in self.channels:
                channel = self.channels[id]
                generator = channel.get("generator", default=VirtualConnector.VIRTUAL)
                if generator == VirtualConnector.VIRTUAL:
                    self._write_virtual(data, channel)

                elif generator == VirtualConnector.RANDOM:
                    self._write_random(data, channel)

                else:
                    raise ConnectorError(
                        self, f"Trying to write to dummy channel '{channel.id}' with generator: {generator}"
                    )

    def _write_virtual(self, data: pd.DataFrame, channel: Channel) -> None:
        self._data[channel.id] = data.at[data.index[-1], channel.id]

    def _write_random(self, data: pd.DataFrame, channel: Channel) -> None:
        value = data.at[data.index[-1], channel.id]
        if value < channel.min:
            value = channel.min
        if value > channel.max:
            value = channel.max
        self._data[channel.id] = value
