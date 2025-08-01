# -*- coding: utf-8 -*-
"""
lori.connectors.dummy
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import random
from typing import Optional

import pandas as pd
import pytz as tz
from lori import Channel, ConfigurationException, Resource, Resources
from lori.connectors import Connector, ConnectorException, register_connector_type
from lori.typing import TimestampType


# noinspection PyShadowingBuiltins
@register_connector_type("dummy", "random")
class DummyConnector(Connector):
    STATIC: str = "static"
    RANDOM: str = "random"
    VIRTUAL: str = "virtual"

    _data: pd.Series

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        index = []
        data = []
        for resource in resources:
            index.append(resource.id)

            generator = resource.get("generator", default=None)
            if generator == DummyConnector.STATIC:
                attr = "value"
                if attr not in resource:
                    raise ConfigurationException(
                        f"Invalid dummy channel '{resource.id}', missing attribute: {attr}"
                    )

            elif generator == DummyConnector.RANDOM:
                for attr in ["min", "max"]:
                    if attr not in resource:
                        raise ConfigurationException(
                            f"Invalid dummy channel '{resource.id}', missing attribute: {attr}"
                        )
                data.append(float(random.randrange(int(resource.min * 100), int(resource.max * 100))) / 100.0)

            elif generator is not None:
                raise ConfigurationException(f"Invalid dummy channel '{resource.id}' generator: {generator}")
            else:
                data.append(resource.get("default", default=None))
        self._data = pd.Series(index=index, data=data)

    def read(
        self,
        resources: Resources,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> pd.DataFrame:
        for resource in resources:
            generator = resource.get("generator", default=DummyConnector.VIRTUAL)
            if generator == DummyConnector.STATIC:
                self._read_static(resource)

            elif generator == DummyConnector.STATIC:
                self._read_random(resource)

            elif generator != DummyConnector.VIRTUAL:
                raise ConnectorException(
                    self, f"Trying to read dummy channel '{resource.id}' with generator: {generator}"
                )
        return self._data.to_frame(pd.Timestamp.now(tz.UTC).floor(freq="s")).T

    def _read_static(self, resource: Resource) -> None:
        value = resource.get("value")
        self._data[resource.id] = value

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
                generator = channel.get("generator", default=DummyConnector.VIRTUAL)
                if generator == DummyConnector.STATIC:
                    self._write_static(data, channel)

                elif generator == DummyConnector.RANDOM:
                    self._write_random(data, channel)

                elif generator == DummyConnector.VIRTUAL:
                    self._write_virtual(data, channel)

                else:
                    raise ConnectorException(
                        self, f"Trying to write to dummy channel '{channel.id}' with generator: {generator}"
                    )

    def _write_static(self, data: pd.DataFrame, channel: Channel) -> None:
        self._data[channel.id] = data.at[data.index[-1], channel.id]

    def _write_virtual(self, data: pd.DataFrame, channel: Channel) -> None:
        self._data[channel.id] = data.at[data.index[-1], channel.id]

    def _write_random(self, data: pd.DataFrame, channel: Channel) -> None:
        value = data.at[data.index[-1], channel.id]
        if value < channel.min:
            value = channel.min
        if value > channel.max:
            value = channel.max
        self._data[channel.id] = value
