# -*- coding: utf-8 -*-
"""
lori.connectors.tasks.check
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from typing import Optional

import pandas as pd
from lori.connectors import Database
from lori.connectors.tasks.task import ConnectorTask


class CheckTask(ConnectorTask):
    exists: bool

    def run(
        self,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> None:
        self._logger.debug(
            f"Checking data for {len(self.channels)} channels of '{type(self.connector).__name__}': {self.connector.id}"
        )
        if isinstance(self.connector, Database):
            self.exists = self.connector.exists(self.channels, start=start, end=end)
        else:
            self.exists = False
