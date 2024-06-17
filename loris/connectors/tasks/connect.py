# -*- coding: utf-8 -*-
"""
loris.connectors.tasks.connect
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from loris.channels import ChannelState
from loris.connectors.tasks.task import ConnectorTask


class ConnectTask(ConnectorTask):
    def run(self) -> None:
        self._logger.info(f"Connecting {type(self.connector).__name__}: {self.connector.uuid}")
        self.set_states(ChannelState.CONNECTING)
        self.connector.connect(self.channels)

        self.set_states(ChannelState.CONNECTED)
