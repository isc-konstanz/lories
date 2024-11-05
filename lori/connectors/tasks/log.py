# -*- coding: utf-8 -*-
"""
lori.connectors.tasks.log
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from lori.connectors.tasks.task import ConnectorTask
from lori.data.channels import Channels


class LogTask(ConnectorTask):
    def run(self) -> None:
        self._logger.debug(
            f"Logging {len(self.channels)} channels of " f"{type(self.connector).__name__}: " f"{self.connector.id}"
        )
        # Pass copied connectors instead of actual objects, including parsed logger specific connector configurations
        channels = Channels(c.from_logger() for c in self.channels)

        self.connector.write(channels.to_frame(unique=True))
