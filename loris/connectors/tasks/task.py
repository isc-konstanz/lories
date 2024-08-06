# -*- coding: utf-8 -*-
"""
loris.connectors.tasks.task
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from threading import Thread

from loris.connectors import ConnectionException, Connector, ConnectorException
from loris.data.channels import Channels, ChannelState


class ConnectorTask(ABC, Thread):
    connector: Connector
    channels: Channels

    def __init__(self, connector: Connector, channels: Channels, name: str = None, *args, **kwargs):
        super().__init__(name=name, target=self.__call__, *args, **kwargs)
        self._logger = logging.getLogger(self.__module__)
        self.connector = connector
        self.channels = channels

    def __call__(self, **kwargs) -> ConnectorTask:
        try:
            self.run(**kwargs)

        except ConnectionException as e:
            try:
                self.connector.set_channels(ChannelState.DISCONNECTING)
                self.connector.disconnect()
            finally:
                self.connector.set_channels(ChannelState.DISCONNECTED)
            raise e
        except ConnectorException as e:
            raise e
        except Exception as e:
            raise ConnectorException(e, connector=self.connector)

        return self

    @abstractmethod
    def run(self, **kwargs):
        pass
