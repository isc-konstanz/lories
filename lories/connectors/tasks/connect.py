# -*- coding: utf-8 -*-
"""
lories.connectors.tasks.connect
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from lories._core._channel import ChannelState  # noqa
from lories._core._connector import Connector  # noqa
from lories.connectors.tasks.task import ConnectorTask


class ConnectTask(ConnectorTask):
    # noinspection PyProtectedMember
    def run(self) -> Connector:
        self.connector.set_channels(ChannelState.CONNECTING)
        self.connector.connect(self.channels)
        self.connector.set_channels(ChannelState.CONNECTED)

        return self.connector
