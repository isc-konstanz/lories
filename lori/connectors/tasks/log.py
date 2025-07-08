# -*- coding: utf-8 -*-
"""
lori.connectors.tasks.log
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from lori.connectors.tasks.write import WriteTask
from lori.data.channels import Channels


class LogTask(WriteTask):
    def run(self) -> None:
        self._logger.debug(
            f"Logging {len(self.channels)} channels of '{type(self.connector).__name__}': {self.connector.id}"
        )
        # Pass copied connectors instead of actual objects, including parsed logger specific connector configurations
        channels = Channels(c.from_logger() for c in self.channels)

        self._run_write(channels)
