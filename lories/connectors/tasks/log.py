# -*- coding: utf-8 -*-
"""
lories.connectors.tasks.log
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from lories.connectors.tasks.write import WriteTask


class LogTask(WriteTask):
    def run(self) -> None:
        self._logger.debug(
            f"Logging {len(self.channels)} channels of '{type(self.connector).__name__}': {self.connector.id}"
        )
        # Pass copied connectors instead of actual objects, including parsed logger specific connector configurations
        channels = self.channels.from_logger()

        self._run_write(channels)
