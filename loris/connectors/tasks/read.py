# -*- coding: utf-8 -*-
"""
    loris.connectors.tasks.read
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations

import pandas as pd
import datetime as dt
import logging

from typing import Optional
from loris.connectors.tasks.task import ConnectorTask

logger = logging.getLogger(__name__)


class ReadTask(ConnectorTask):

    def run(self,
            start: Optional[pd.Timestamp | dt.datetime] = None,
            end:   Optional[pd.Timestamp | dt.datetime] = None) -> None:

        logger.debug(f"Reading {len(self.channels)} channels of "
                     f"{type(self.connector).__name__}: "
                     f"{self.connector.uuid}")

        self.connector.read(self.channels, start, end)
