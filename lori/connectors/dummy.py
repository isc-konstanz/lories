# -*- coding: utf-8 -*-
"""
lori.connectors.dummy
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import random
from typing import Optional

import pandas as pd
import pytz as tz
from lori import Channel, ConfigurationException, Resource, Resources
from lori.connectors import Connector, ConnectorException, register_connector_type


# noinspection PyShadowingBuiltins
@register_connector_type("dummy", "random")
class DummyConnector(Connector):
    _data: pd.Series

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        self._data = pd.Series(index=[r.id for r in resources])
        for resource in resources:
            generator = resource.get("generator", default=None)
            if generator == "random":
                for attr in ["min", "max"]:
                    if attr not in resource:
                        raise ConfigurationException(
                            f"Invalid dummy channel '{resource.id}', missing attribute: {attr}"
                        )
            elif generator is not None:
                raise ConfigurationException(f"Invalid dummy channel '{resource.id}' generator: {generator}")

    def read(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> pd.DataFrame:
        for resource in resources:
            generator = resource.get("generator", default=None)
            if generator == "random":
                self._read_random(resource)
            else:
                raise ConnectorException(
                    self, f"Trying to read dummy channel '{resource.id}' with generator: {generator}"
                )
        return self._data.to_frame(pd.Timestamp.now(tz.UTC).floor(freq="s")).T

    def _read_random(self, resource: Resource) -> None:
        if pd.isna(self._data[resource.id]):
            value = float(random.randrange(int(resource.min * 100), int(resource.max * 100))) / 100.0
        else:
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
                generator = channel.get("generator", default=None)
                if generator == "random":
                    self._write_random(data, channel)

    def _write_random(self, data: pd.DataFrame, channel: Channel) -> None:
        value = data.loc[data.index[-1], channel.id]
        if value < channel.min:
            value = channel.min
        if value > channel.max:
            value = channel.max
        self._data[channel.id] = value
