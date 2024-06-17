# -*- coding: utf-8 -*-
"""
loris.connectors.tasks.write
~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from loris import Channels
from loris.connectors.tasks.task import ConnectorTask


class LogTask(ConnectorTask):
    def run(self) -> None:
        self._logger.debug(
            f"Logging {len(self.channels)} channels of " f"{type(self.connector).__name__}: " f"{self.connector.uuid}"
        )
        # Pass copied channels instead of actual objects, including parsed logger specific channel configurations
        channels = Channels([c.from_logger() for c in self.channels])

        self.connector.write(channels)
