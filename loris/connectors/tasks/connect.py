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
        self.set_channels(ChannelState.CONNECTING)
        self.connector._do_connect(self.channels)

        self.set_channels(ChannelState.CONNECTED)
