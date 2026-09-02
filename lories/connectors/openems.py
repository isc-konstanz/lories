# -*- coding: utf-8 -*-
"""
lories.connectors.openems
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import json
from abc import abstractmethod
from threading import Event, Thread
from typing import Dict, Optional

import websocket

import pandas as pd
import pytz
from lories.connectors import Connector, register_connector_type
from lories.connectors.errors import ConnectionError, ConnectorError
from lories.core.configs.parameters import ChannelParameter, Parameter
from lories.data.channels import Channel
from lories.io.jsonrpc import JsonRpc
from lories.typing import Resources


# noinspection PyAbstractClass
class OpenEMSConnector(Connector):
    """Abstract base for push-based OpenEMS WebSocket connectors.

    Concrete subclasses implement the wire protocol:

    * :class:`OpenEMSEdgeConnector`    - direct connection to an OpenEMS Edge device
    * :class:`OpenEMSBackendConnector` - connection via OpenEMS Backend B2B WebSocket
    """

    _host = Parameter(key="host", type=str, default="localhost", desc="OpenEMS WebSocket server host")
    _ws_port = Parameter(key="ws_port", type=int, default=8085, min=1, max=65535, desc="OpenEMS WebSocket server port")
    _username = Parameter(key="username", type=str, default="admin", desc="OpenEMS authentication username")
    _password = Parameter(
        key="password", type=str, default="admin", desc="OpenEMS authentication password", secret=True
    )
    _timeout = Parameter(
        key="timeout",
        type=int,
        default=10,
        min=1,
        desc="Seconds to wait for authentication to complete before failing connect",
    )

    # Per-channel parameters
    address = ChannelParameter(type=str, required=True, desc="OpenEMS channel address 'Component/Channel'")

    _host: str
    _ws_port: int
    _username: str
    _password: str
    _timeout: int
    _subscribe_count: int

    _ws_app: Optional[websocket.WebSocketApp]
    _ws_thread: Optional[Thread]
    _connected_event: Event
    _auth_error: Optional[str]
    _listeners: Dict[str, "OpenEMSListener"]
    _json_rpc: JsonRpc

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._ws_app = None
        self._ws_thread = None
        self._connected_event = Event()
        self._auth_error = None
        self._listeners = {}
        self._subscribe_count = 0
        self._json_rpc = JsonRpc()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def is_connected(self) -> bool:
        return self._ws_app is not None and self._connected_event.is_set()

    def connect(self, resources: Resources) -> None:
        # Build address → OpenEMSListener map from all channels bound to us
        self._listeners = {}
        for resource in resources:
            self._listeners[resource.address] = OpenEMSListener(resource.address, resource)

        if not self._listeners:
            self._logger.warning(
                f"{type(self).__name__} '{self.id}': no channels with an 'address' attribute – nothing to subscribe to"
            )

        ws_url = f"ws://{self._host}:{self._ws_port}/websocket"
        self._connected_event.clear()
        self._subscribe_count = 0
        self._auth_error = None

        def on_open(ws: websocket.WebSocketApp) -> None:
            self._logger.debug(f"OpenEMS WS opened to {ws_url}, authenticating …")
            ws.send(
                self._json_rpc.build_request(
                    [("authenticateWithPassword", {"username": self._username, "password": self._password})]
                )
            )

        def on_message(ws: websocket.WebSocketApp, message: str) -> None:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                self._logger.warning(f"OpenEMS WS received non-JSON message: {message!r}")
                return

            if "error" in data:
                if not self._connected_event.is_set():
                    # Authentication failed: unblock connect()'s wait immediately, instead
                    # of letting it block for the full timeout and raise a misleading error.
                    self._auth_error = str(data["error"])
                    self._connected_event.set()
                else:
                    self._logger.error(f"OpenEMS WS JSON-RPC error: {data['error']}")
                return

            notification = data.get("method")
            result = data.get("result", {})

            if isinstance(result, dict) and "token" in result:
                # Authentication successful → subscribe
                self._connected_event.set()
                self._logger.info(f"OpenEMS WS authenticated at {ws_url} (token={result['token'][:8]}…)")
                self._send_subscribe(ws)

            elif notification is not None:
                timestamp = pd.Timestamp.now(tz=pytz.UTC).floor(freq="s")
                self._dispatch_notification(notification, data.get("params", {}), self._listeners, timestamp)

        def on_error(ws: websocket.WebSocketApp, error: Exception) -> None:
            self._logger.error(f"OpenEMS WS error: {error}")

        def on_close(ws: websocket.WebSocketApp, close_status_code, close_msg) -> None:
            self._connected_event.clear()
            self._logger.info(f"OpenEMS WS disconnected from {ws_url} (code={close_status_code}, msg={close_msg})")

        self._ws_app = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        self._ws_thread = Thread(
            target=self._ws_app.run_forever,
            kwargs={"ping_interval": 30, "ping_timeout": 10},
            daemon=True,
            name=f"openems-ws-{self.id}",
        )
        self._ws_thread.start()

        # Block until authentication completes, fails, or times out
        if not self._connected_event.wait(timeout=self._timeout):
            self._ws_app.close()
            raise ConnectionError(
                self,
                f"Timeout ({self._timeout}s) waiting for OpenEMS authentication at {ws_url}",
            )

        if self._auth_error is not None:
            self._ws_app.close()
            raise ConnectionError(self, f"OpenEMS authentication failed at {ws_url}: {self._auth_error}")

    def disconnect(self) -> None:
        # Unsubscribe gracefully before closing
        if self._ws_app is not None and self._connected_event.is_set():
            try:
                self._send_unsubscribe()
            except Exception:
                pass  # best-effort; closing anyway

        self._connected_event.clear()
        if self._ws_app is not None:
            self._ws_app.close()
            self._ws_app = None
        if self._ws_thread is not None and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=5)
        self._ws_thread = None
        self._listeners = {}
        self._logger.info(f"OpenEMS WS disconnected from {self._host}:{self._ws_port}")

    def read(self, resources: Resources) -> pd.DataFrame:
        raise ConnectorError(
            self, f"{type(self).__name__} is push-based (WebSocket); it does not support pull-mode read()."
        )

    def write(self, data: pd.DataFrame) -> None:
        raise ConnectorError(self, f"{type(self).__name__} does not support write().")

    # ------------------------------------------------------------------
    # Abstract protocol hooks (implemented by subclasses)
    # ------------------------------------------------------------------

    @abstractmethod
    def _send_subscribe(self, ws: websocket.WebSocketApp) -> None:
        """Send the mode-specific subscribe message and increment ``_subscribe_count``."""
        ...

    @abstractmethod
    def _send_unsubscribe(self) -> None:
        """Send the mode-specific unsubscribe message over ``_ws_app``."""
        ...

    @abstractmethod
    def _dispatch_notification(
        self,
        notification: str,
        params: dict,
        listeners: Dict[str, "OpenEMSListener"],
        timestamp: pd.Timestamp,
    ) -> None:
        """Route an incoming JSON-RPC push notification to the appropriate listeners."""
        ...


# ----------------------------------------------------------------------
# Concrete connector: Edge
# ----------------------------------------------------------------------


@register_connector_type("openems_edge")
class OpenEMSEdgeConnector(OpenEMSConnector):
    """Direct connection to an OpenEMS **Edge** device.

    Wire protocol (Edge WebSocket API)::

        The Edge WebSocket server routes ``subscribeChannels`` through the
        ``EdgeRpcRequestHandler``, so every outbound request must be wrapped in
        an outer ``edgeRpc`` envelope, and every inbound ``currentData``
        notification arrives wrapped in the same ``edgeRpc`` envelope.

        Subscribe (client → Edge)::

            {
              "method": "edgeRpc",
              "params": {
                "edgeId": "0",
                "payload": {
                  "method": "subscribeChannels",
                  "params": {"count": N, "channels": ["Comp/Chan", …]}
                }
              }
            }

        Notification (Edge → client)::

            {
              "method": "edgeRpc",
              "params": {
                "edgeId": "0",
                "payload": {
                  "method": "currentData",
                  "params": {"Comp/Chan": value, …}
                }
              }
            }
    """

    # The Edge WebSocket server hard-codes EDGE_ID = "0" (ControllerApiWebsocket.java).
    _edge_id = Parameter(key="edge_id", type=str, default="0", desc="OpenEMS Edge ID (server-side fixed to '0')")

    _edge_id: str

    def _send_subscribe(self, ws: websocket.WebSocketApp) -> None:
        ws.send(
            self._json_rpc.build_request(
                [
                    ("edgeRpc", {"edgeId": self._edge_id}),
                    (
                        "subscribeChannels",
                        {"count": self._subscribe_count, "channels": list(self._listeners.keys())},
                    ),
                ]
            )
        )
        self._subscribe_count += 1

    def _send_unsubscribe(self) -> None:
        self._ws_app.send(
            self._json_rpc.build_request(
                [
                    ("edgeRpc", {"edgeId": self._edge_id}),
                    ("subscribeChannels", {"count": self._subscribe_count, "channels": []}),
                ]
            )
        )

    def _dispatch_notification(
        self,
        notification: str,
        params: dict,
        listeners: Dict[str, "OpenEMSListener"],
        timestamp: pd.Timestamp,
    ) -> None:
        if notification == "edgeRpc":
            payload = params.get("payload", {})
            if not isinstance(payload, dict):
                return
            inner_method = payload.get("method")
            inner_params = payload.get("params", {})
            if inner_method == "currentData" and isinstance(inner_params, dict):
                for address, value in inner_params.items():
                    if address in listeners:
                        listeners[address](timestamp, value)


# ----------------------------------------------------------------------
# Concrete connector: Backend
# ----------------------------------------------------------------------


@register_connector_type("openems_backend")
class OpenEMSBackendConnector(OpenEMSConnector):
    """Connection via OpenEMS **Backend B2B** WebSocket.

    Wire protocol::

        subscribe method   : subscribeEdgesChannels
        subscribe params   : {"count": N, "ids": ["edge0"], "channels": ["Comp/Chan", …]}
        notification method: edgesCurrentData
        notification params: {"edge0": {"Comp/Chan": value, …}}
    """

    _edge_id = Parameter(key="edge_id", type=str, default="edge0", desc="OpenEMS Backend edge ID")

    _edge_id: str

    def _send_subscribe(self, ws: websocket.WebSocketApp) -> None:
        ws.send(
            self._json_rpc.build_request(
                [
                    (
                        "subscribeEdgesChannels",
                        {
                            "count": self._subscribe_count,
                            "ids": [self._edge_id],
                            "channels": list(self._listeners.keys()),
                        },
                    )
                ]
            )
        )
        self._subscribe_count += 1

    def _send_unsubscribe(self) -> None:
        self._ws_app.send(
            self._json_rpc.build_request(
                [("subscribeEdgesChannels", {"count": self._subscribe_count, "ids": [], "channels": []})]
            )
        )

    def _dispatch_notification(
        self,
        notification: str,
        params: dict,
        listeners: Dict[str, "OpenEMSListener"],
        timestamp: pd.Timestamp,
    ) -> None:
        if notification == "edgesCurrentData":
            for edge_channels in params.values():
                if not isinstance(edge_channels, dict):
                    continue
                for address, value in edge_channels.items():
                    if address in listeners:
                        listeners[address](timestamp, value)


# ----------------------------------------------------------------------
# Listener helper
# ----------------------------------------------------------------------


class OpenEMSListener:
    """Receives a (timestamp, value) pair for a single OpenEMS channel address
    and forwards it to the associated lories :class:`~lories.data.channels.Channel`."""

    address: str
    _channel: Channel

    def __init__(self, address: str, channel: Channel) -> None:
        self.address = address
        self._channel = channel

    def __call__(self, timestamp: pd.Timestamp, value) -> None:
        if value is None:
            return
        self._channel.set(timestamp, value)
