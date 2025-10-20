# -*- coding: utf-8 -*-
"""
lories.connectors.tasks.read
~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import inspect
from typing import Optional

import pandas as pd
from lories._core._channel import ChannelState  # noqa
from lories.connectors.tasks.task import ConnectorTask


class ReadTask(ConnectorTask):
    # noinspection PyArgumentList
    def run(self, inplace: bool = False, **kwargs) -> Optional[pd.DataFrame]:
        self._logger.debug(
            f"Reading {len(self.channels)} channels of '{type(self.connector).__name__}': {self.connector.id}"
        )
        signature = inspect.signature(type(self.connector).read)
        arguments = [p.name for p in signature.parameters.values() if p.kind == p.POSITIONAL_OR_KEYWORD]
        for argument in list(kwargs.keys()):
            if argument not in arguments:
                value = kwargs.pop(argument)
                self._logger.warning(
                    f"Trying to read Connector '{self.connector.id}' with unknown argument '{argument}': {value}"
                )

        data = self.connector.read(self.channels, **kwargs)
        if data is None or data.dropna(axis="columns", how="all").empty:
            if inplace:
                self.channels.set_state(ChannelState.NOT_AVAILABLE)
            return None
        if inplace:
            self.channels.set_frame(data)
        return data
