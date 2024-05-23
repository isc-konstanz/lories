# -*- coding: utf-8 -*-
"""
    loris.connectors.tasks.write
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""
import logging

from loris.connectors.tasks.task import ConnectorTask

logger = logging.getLogger(__name__)


class WriteTask(ConnectorTask):

    def run(self) -> None:
        logger.debug(f"Writing {len(self.channels)} channels of "
                     f"{type(self.connector).__name__}: "
                     f"{self.connector.uuid}")

        self.connector.write(self.channels)
