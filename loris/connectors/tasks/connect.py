# -*- coding: utf-8 -*-
"""
loris.connectors.tasks.connect
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from loris.connectors.tasks.task import ConnectorTask
from loris.data.channels import ChannelState


class ConnectTask(ConnectorTask):
    # noinspection PyProtectedMember
    def run(self) -> None:
        self.connector.set_channels(ChannelState.CONNECTING)
        self.connector.connect(self.channels)

        self.connector.set_channels(ChannelState.CONNECTED)
