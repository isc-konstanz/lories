# -*- coding: utf-8 -*-
"""
    loris.connectors.tasks.connect
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""
import logging

from loris.channels import ChannelState
from loris.connectors.tasks.task import ConnectorTask

logger = logging.getLogger(__name__)


class ConnectTask(ConnectorTask):

    def run(self) -> None:
        logger.info(f"Connecting {type(self.connector).__name__}: {self.connector.uuid}")
        self.set_states(ChannelState.CONNECTING)
        self.connector.connect(self.channels)

        self.set_states(ChannelState.CONNECTED)
