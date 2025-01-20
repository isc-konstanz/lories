# -*- coding: utf-8 -*-
"""
lori.connectors.tasks.write
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from lori.connectors.tasks.task import ConnectorTask


class WriteTask(ConnectorTask):
    def run(self) -> None:
        self._logger.debug(
            f"Writing {len(self.channels)} channels of '{type(self.connector).__name__}': {self.connector.id}"
        )
        self.connector.write(self.channels.to_frame(unique=True))
