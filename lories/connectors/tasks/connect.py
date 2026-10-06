# -*- coding: utf-8 -*-
"""
lories.connectors.tasks.connect
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from lories._core._channel import ChannelState  # noqa
from lories._core._connector import Connector  # noqa
from lories.connectors.errors import ConnectionError
from lories.connectors.tasks.task import ConnectorTask


class ConnectTask(ConnectorTask):
    # noinspection PyProtectedMember
    def run(self) -> Connector:
        channels = self.channels.filter(lambda c: c.has_connector(self.connector.id))
        channels.set_state(ChannelState.CONNECTING)
        try:
            self.connector.connect(self.channels)
        except ConnectionError:
            # ConnectorTask.__call__ disconnects and stamps DISCONNECTED
            raise
        except Exception:
            channels.filter(lambda c: c.state == ChannelState.CONNECTING).set_state(ChannelState.DISCONNECTED)
            raise
        self.connector.set_channels(ChannelState.CONNECTED)

        return self.connector
