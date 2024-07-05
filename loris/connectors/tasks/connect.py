# -*- coding: utf-8 -*-
"""
loris.connectors.tasks.connect
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from loris.channels import ChannelState
from loris.connectors.tasks.task import ConnectorTask


class ConnectTask(ConnectorTask):
    # noinspection PyProtectedMember
    def run(self) -> None:
        self.set_states(ChannelState.CONNECTING)
        self.connector._do_connect(self.channels)

        self.set_states(ChannelState.CONNECTED)
