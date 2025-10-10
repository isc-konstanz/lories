# -*- coding: utf-8 -*-
"""
lori.connectors.tasks.task
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from threading import Thread
from typing import Any

from lori._core._channel import ChannelState  # noqa
from lori._core._channels import Channels  # noqa
from lori._core._connector import Connector  # noqa
from lori.connectors.errors import ConnectionError, ConnectorError


class ConnectorTask(ABC, Thread):
    connector: Connector
    channels: Channels

    def __init__(self, connector: Connector, channels: Channels, name: str = None, **kwargs):
        super().__init__(name=name, target=self.__call__, **kwargs)
        self._logger = logging.getLogger(self.__module__)
        self.connector = connector
        self.channels = channels

    # noinspection PyUnresolvedReferences
    def __call__(self, **kwargs) -> Any:
        try:
            return self.run(**kwargs)

        except ConnectionError as e:
            try:
                self.connector.set_channels(ChannelState.DISCONNECTING)
                self.connector.disconnect()
            finally:
                self.connector.set_channels(ChannelState.DISCONNECTED)
                raise e
        except ConnectorError as e:
            raise e
        except Exception as e:
            raise ConnectorError(self.connector, str(e))

    @abstractmethod
    def run(self, **kwargs) -> Any:
        pass
