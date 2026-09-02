# -*- coding: utf-8 -*-
"""
lories.io.websocket
~~~~~~~~~~~~~~~~~~~


"""

from typing import Optional, Union

import websocket


class Websocket:
    _host: str
    _port: int
    _timeout: Optional[Union[int, float]]
    _ws: Optional[websocket.WebSocket]

    def __init__(
        self,
        host: str,
        port: int,
        timeout: Union[int, float],
        **kwargs,
    ):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._ws = None

    @property
    def _url(self) -> str:
        return f"ws://{self._host}:{self._port}"

    def connect(self):
        ws = websocket.WebSocket()
        ws.timeout = self._timeout
        ws.connect(self._url)
        self._ws = ws

    def send(self, message: str) -> str:
        if self._ws is None or not self._ws.connected:
            raise ConnectionError("WebSocket is not connected.")
        self._ws.send(message)
        return self._ws.recv()

    def close(self):
        if self._ws:
            self._ws.close()
            self._ws = None
