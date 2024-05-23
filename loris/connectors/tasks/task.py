# -*- coding: utf-8 -*-
"""
    loris.connectors.tasks.task
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations
from abc import ABC, abstractmethod
from threading import Thread
from loris.channels import Channels, ChannelState
from loris.connectors import Connector, ConnectorException, ConnectionException


class ConnectorTask(ABC, Thread):

    connector: Connector
    channels: Channels

    def __init__(self, connector: Connector, channels: Channels, name: str = None, *args, **kwargs):
        super().__init__(name=name, target=self.__call__, *args, **kwargs)
        self.connector = connector
        self.channels = channels

    def __call__(self, **kwargs) -> ConnectorTask:
        try:
            self.run(**kwargs)

        except ConnectionException as e:
            try:
                self.connector.set_states(ChannelState.DISCONNECTING)
                self.connector.disconnect()
            finally:
                self.connector.set_states(ChannelState.DISCONNECTED)
            raise e

        except Exception as e:
            raise ConnectorException(self.connector, e)

        return self

    @abstractmethod
    def run(self, **kwargs):
        pass

    def set_states(self, channel_state: ChannelState) -> None:
        for channel in self.channels:
            channel.state = channel_state
