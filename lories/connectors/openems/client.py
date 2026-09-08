# -*- coding: utf-8 -*-
"""
lories.connectors.openems.client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import base64
import json
import time
from abc import abstractmethod
from dataclasses import dataclass
from threading import Event, Thread
from typing import Any, Dict, Optional

import requests
import websocket

import pandas as pd
import pytz
from lories.connectors import Connector, register_connector_type
from lories.connectors.errors import ConnectionError, ConnectorError
from lories.core.configs.parameters import ChannelParameter, Parameter
from lories.data.channels import Channel
from lories.io.jsonrpc import JsonRpc
from lories.io.rest import Rest
from lories.typing import Resource, Resources


@dataclass(frozen=True)
class ChannelInfo:
    """One entry of the OpenEMS REST channel listing."""

    address: str
    component: str
    channel: str
    type: Optional[str]
    unit: Optional[str]
    access_mode: Optional[str]


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
    _rest_port = Parameter(
        key="rest_port", type=int, default=8084, min=1, max=65535, desc="OpenEMS REST/JSON server port"
    )
    _rest_endpoint = Parameter(
        key="rest_endpoint", type=str, default="rest", desc="OpenEMS REST/JSON server endpoint path"
    )

    # Per-channel parameters
    component = ChannelParameter(type=str, required=True, desc="OpenEMS component id, e.g. 'meter0'")
    channel = ChannelParameter(type=str, required=True, desc="OpenEMS channel name, e.g. 'ActivePower'")

    _host: str
    _ws_port: int
    _username: str
    _password: str
    _timeout: int
    _rest_port: int
    _rest_endpoint: str
    _subscribe_count: int

    _discovered: Optional[Dict[str, ChannelInfo]] = None

    _ws_app: Optional[websocket.WebSocketApp]
    _ws_thread: Optional[Thread]
    _connected_event: Event
    _connect_error: Optional[str]
    _listeners: Dict[str, "OpenEMSListener"]
    _json_rpc: JsonRpc

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._ws_app = None
        self._ws_thread = None
        self._connected_event = Event()
        self._connect_error = None
        self._listeners = {}
        self._subscribe_count = 0
        self._json_rpc = JsonRpc()
        self._discovered = None

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def discover(self, refresh: bool = False) -> Dict[str, ChannelInfo]:
        """Return every channel the OpenEMS device offers, keyed by 'Component/Channel'.

        The listing is fetched once over the REST/JSON API and cached for the lifetime of
        this connector; pass ``refresh`` to fetch it again. Only the configured parameters
        are needed, so this may be called before ``connect()`` - a bound component uses it
        at configure time to validate its addresses.
        """
        if self._discovered is not None and not refresh:
            return self._discovered

        rest = Rest(
            host=self._host,
            port=self._rest_port,
            username=self._username,
            password=self._password,
            endpoint=self._rest_endpoint,
            timeout=self._timeout,
        )
        raw = None
        fetched = False
        error: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                # Rest.get_request raises the built-in ConnectionError on a non-200 and lets
                # requests' own errors through; both are OSError subclasses, and a malformed
                # body surfaces from json.loads as a ValueError.
                raw = json.loads(rest.get_request("channel/.*/.*"))
                fetched = True
                break
            except (requests.RequestException, ValueError, OSError) as e:
                error = e
                self._logger.warning(f"OpenEMS REST channel discovery attempt {attempt}/3 failed: {e}")
                if attempt < 3:
                    time.sleep(attempt)
        if not fetched:
            raise ConnectorError(self, f"OpenEMS REST channel discovery failed after 3 attempts: {error}")
        if not isinstance(raw, list):
            raise ConnectorError(
                self, f"OpenEMS REST channel discovery returned {type(raw).__name__}, expected a list of channels"
            )

        self._discovered = {c.address: c for c in (self._build_info(entry) for entry in raw) if c is not None}
        return self._discovered

    @staticmethod
    def _build_info(entry: Dict[str, Any]) -> Optional[ChannelInfo]:
        if not isinstance(entry, dict):
            return None
        address = entry.get("address")
        if not isinstance(address, str) or "/" not in address:
            return None
        component, channel = address.split("/", 1)
        unit = entry.get("unit")
        return ChannelInfo(
            address=address,
            component=component,
            channel=channel,
            type=entry.get("type"),
            unit=unit if unit else None,
            access_mode=entry.get("accessMode"),
        )

    def _address(self, resource: Resource) -> str:
        """Compose the OpenEMS address a channel subscribes to from its two config keys."""
        component = resource.get("component")
        channel = resource.get("channel")
        if not component or not channel:
            raise ConnectorError(
                self,
                f"Channel '{resource.id}' is missing the OpenEMS 'component' and/or 'channel' config key",
            )
        return f"{component}/{channel}"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def is_connected(self) -> bool:
        return self._ws_app is not None and self._connected_event.is_set()

    def connect(self, resources: Resources) -> None:
        # Build address -> OpenEMSListener map from all channels bound to us
        self._listeners = {}
        for resource in resources:
            address = self._address(resource)
            self._listeners[address] = OpenEMSListener(address, resource)

        if not self._listeners:
            self._logger.warning(f"{type(self).__name__} '{self.id}': no channels bound - nothing to subscribe to")

        ws_url = f"ws://{self._host}:{self._ws_port}/websocket"
        self._connected_event.clear()
        self._subscribe_count = 0
        self._connect_error = None

        def on_open(ws: websocket.WebSocketApp) -> None:
            self._logger.debug(f"OpenEMS WS opened to {ws_url}")
            self._on_ws_open(ws)

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
                    self._connect_error = str(data["error"])
                    self._connected_event.set()
                else:
                    self._logger.error(f"OpenEMS WS JSON-RPC error: {data['error']}")
                return

            notification = data.get("method")
            result = data.get("result", {})

            if isinstance(result, dict) and "token" in result:
                # Authentication successful → subscribe
                self._connected_event.set()
                self._logger.info(f"OpenEMS WS authenticated at {ws_url} (token={result['token'][:8]}...)")
                self._send_subscribe(ws)

            elif notification is not None:
                timestamp = pd.Timestamp.now(tz=pytz.UTC).floor(freq="s")
                self._dispatch_notification(notification, data.get("params", {}), self._listeners, timestamp)

        def on_error(ws: websocket.WebSocketApp, error: Exception) -> None:
            if not self._connected_event.is_set():
                # Transport/handshake failure before the connection was established
                # (e.g. rejected upgrade): unblock connect()'s wait immediately.
                self._connect_error = str(error)
                self._connected_event.set()
            else:
                self._logger.error(f"OpenEMS WS error: {error}")

        def on_close(ws: websocket.WebSocketApp, close_status_code, close_msg) -> None:
            self._connected_event.clear()
            self._logger.info(f"OpenEMS WS disconnected from {ws_url} (code={close_status_code}, msg={close_msg})")

        self._ws_app = websocket.WebSocketApp(
            ws_url,
            header=self._ws_headers(),
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

        if self._connect_error is not None:
            self._ws_app.close()
            raise ConnectionError(self, f"OpenEMS connect failed at {ws_url}: {self._connect_error}")

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
    # Overridable connection hooks
    # ------------------------------------------------------------------

    def _ws_headers(self) -> Optional[Dict[str, str]]:
        """Additional HTTP headers for the WebSocket upgrade request (None by default)."""
        return None

    def _on_ws_open(self, ws: websocket.WebSocketApp) -> None:
        """First action once the socket is open.

        Default: authenticate with a JSON-RPC ``authenticateWithPassword`` frame;
        the auth response's token then triggers the subscribe (see ``on_message``).
        """
        ws.send(
            self._json_rpc.build_request(
                [("authenticateWithPassword", {"username": self._username, "password": self._password})]
            )
        )

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

    Authentication is HTTP Basic on the WebSocket upgrade request (verified
    against openems/backend 2025.11.0): there is no ``authenticateWithPassword``
    frame and no token response - an unauthenticated frame makes the server
    abort the socket. The connection counts as established as soon as the
    upgrade succeeds, so the subscribe is sent directly from ``on_open``.

    Wire protocol::

        subscribe method   : subscribeEdgesChannels
        subscribe params   : {"count": N, "ids": ["edge0"], "channels": ["Comp/Chan", …]}
        notification method: edgesCurrentData
        notification params: {"edge0": {"Comp/Chan": value, …}}

    The default B2B port is 8076 (not exposed by the stock Docker compose).
    """

    _edge_id = Parameter(key="edge_id", type=str, default="edge0", desc="OpenEMS Backend edge ID")

    _edge_id: str

    def discover(self, refresh: bool = False) -> Dict[str, ChannelInfo]:
        raise ConnectorError(
            self,
            "Channel discovery is only available on an OpenEMS Edge (REST); bind devices to an openems_edge connector",
        )

    def _ws_headers(self) -> Dict[str, str]:
        token = base64.b64encode(f"{self._username}:{self._password}".encode()).decode()
        return {"Authorization": f"Basic {token}"}

    def _on_ws_open(self, ws: websocket.WebSocketApp) -> None:
        self._connected_event.set()
        self._logger.info(f"OpenEMS B2B WS connected to {self._host}:{self._ws_port}")
        self._send_subscribe(ws)

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
