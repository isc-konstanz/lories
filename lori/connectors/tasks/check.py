# -*- coding: utf-8 -*-
"""
lori.connectors.tasks.check
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Optional

from lori._core._database import _Database  # noqa
from lori._core.typing import Timestamp  # noqa
from lori.connectors.tasks.task import ConnectorTask


class CheckTask(ConnectorTask):
    def run(
        self,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> bool:
        self._logger.debug(
            f"Checking data for {len(self.channels)} channels of '{type(self.connector).__name__}': {self.connector.id}"
        )
        if isinstance(self.connector, _Database):
            return self.connector.exists(self.channels, start=start, end=end)
        else:
            return False
