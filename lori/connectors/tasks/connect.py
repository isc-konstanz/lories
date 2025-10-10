# -*- coding: utf-8 -*-
"""
lori.connectors.tasks.connect
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from lori._core._channel import ChannelState  # noqa
from lori._core._connector import Connector  # noqa
from lori.connectors.tasks.task import ConnectorTask


class ConnectTask(ConnectorTask):
    # noinspection PyProtectedMember
    def run(self) -> Connector:
        self.connector.set_channels(ChannelState.CONNECTING)
        self.connector.connect(self.channels)
        self.connector.set_channels(ChannelState.CONNECTED)

        return self.connector
