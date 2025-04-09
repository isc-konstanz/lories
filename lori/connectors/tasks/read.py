# -*- coding: utf-8 -*-
"""
lori.connectors.tasks.read
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from typing import Optional

import pandas as pd
from lori.connectors import Database
from lori.connectors.tasks.task import ConnectorTask


class ReadTask(ConnectorTask):
    results: pd.DataFrame

    def run(
        self,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> None:
        self._logger.debug(
            f"Reading {len(self.channels)} channels of '{type(self.connector).__name__}': {self.connector.id}"
        )
        if isinstance(self.connector, Database):
            self.results = self.connector.read(self.channels, start=start, end=end)
        else:
            if start is not None or end is not None:
                self._logger.warning(f"Trying to read slice of Connector '{self.connector.id}' from {start} to {end}")
            self.results = self.connector.read(self.channels)
