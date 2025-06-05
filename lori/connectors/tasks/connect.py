# -*- coding: utf-8 -*-
"""
lori.connectors.tasks.connect
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from lori.connectors import Connector
from lori.connectors.tasks.task import ConnectorTask
from lori.data.channels import ChannelState


class ConnectTask(ConnectorTask):
    # noinspection PyProtectedMember
    def run(self) -> Connector:
        self.connector.set_channels(ChannelState.CONNECTING)
        self.connector.connect(self.channels)
        self.connector.set_channels(ChannelState.CONNECTED)

        return self.connector
