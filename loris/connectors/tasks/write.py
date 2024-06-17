# -*- coding: utf-8 -*-
"""
loris.connectors.tasks.write
~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from loris.connectors.tasks.task import ConnectorTask


class WriteTask(ConnectorTask):
    def run(self) -> None:
        self._logger.debug(
            f"Writing {len(self.channels)} channels of " f"{type(self.connector).__name__}: " f"{self.connector.uuid}"
        )

        self.connector.write(self.channels)
