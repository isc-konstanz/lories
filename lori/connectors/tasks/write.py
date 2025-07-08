# -*- coding: utf-8 -*-
"""
lori.connectors.tasks.write
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from lori.connectors.tasks.task import ConnectorTask
from lori.data.channels import Channels


class WriteTask(ConnectorTask):
    def run(self) -> None:
        self._logger.debug(
            f"Writing {len(self.channels)} channels of '{type(self.connector).__name__}': {self.connector.id}"
        )
        self._run_write(self.channels)

    def _run_write(self, channels: Channels) -> None:
        self.connector.write(channels.to_frame(unique=True))
