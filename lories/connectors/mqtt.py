# -*- coding: utf-8 -*-
"""
lories.connectors.mqtt
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Dict, Optional

from paho.mqtt.client import Client as MqttClient

import pandas as pd
import pytz as tz
from lories.connectors import Connector, register_connector_type
from lories.core.configs.parameters import ChannelParameter, Parameter, SelectParameter
from lories.data import Channel
from lories.typing import Configurations, Resources


@register_connector_type("mqtt")
class MqttConnector(Connector):
    """
    MQTT (Message Queuing Telemetry Transport) is a lightweight messaging protocol designed for use in IoT
    applications. It features a publish/subscribe model, QoS levels, and message persistence, making it efficient
    for resource-constrained devices with limited bandwidth or high-latency networks. However, MQTT's lack of
    advanced features like transactions and complex routing, coupled with its reliance on SSL/TLS for security,
    can limit its scalability and performance in large-scale environments.
    """

    _host = Parameter(key="host", type=str, default="localhost", desc="MQTT broker hostname")
    _port = Parameter(key="port", type=int, default=1883, min=1, max=65535, desc="MQTT broker TCP port")
    _transport = SelectParameter(
        ["tcp", "websockets", "unix"], key="transport", default="tcp", desc="MQTT transport protocol"
    )
    _clean_session = Parameter(
        key="clean_session",
        type=bool,
        default=True,
        desc="Start a fresh session and discard any persisted subscriptions on connect",
    )
    _timeout = Parameter(
        key="timeout",
        type=pd.Timedelta,
        default="60s",
        min="1s",
        desc="Keepalive interval for the broker connection",
    )
    _username = Parameter(key="username", type=str, required=False, desc="MQTT authentication username")
    _password = Parameter(key="password", type=str, required=False, desc="MQTT authentication password", secret=True)

    topic = ChannelParameter(type=str, desc="MQTT topic this channel subscribes to or publishes on")

    _host: str
    _port: int
    _transport: str
    _clean_session: bool
    _timeout: pd.Timedelta
    _username: Optional[str]
    _password: Optional[str]

    _client: Optional[MqttClient]
    _listeners: Dict[str, MqttListener]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._listeners = {}

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self._client = MqttClient(
            client_id=self.id.replace(".", "_"),
            clean_session=self._clean_session,
            transport=self._transport,
            # userdata=None,
            # protocol=mqtt.MQTTv311,
        )

        if self._username is not None and self._password is not None:
            self._client.username_pw_set(self._username, self._password)

        self._client.on_connect = self._on_connect_mqtt
        self._client.on_disconnect = self._on_disconnect_mqtt

    def is_connected(self) -> bool:
        return self._client is not None and self._client.is_connected()

    def connect(self, resources: Resources) -> None:
        self._client.connect(self._host, self._port, int(self._timeout.total_seconds()))

        channels = resources.filter(lambda r: isinstance(r, Channel))
        for topic, topic_channels in channels.groupby("topic"):
            topic_listener = MqttListener(topic)
            for channel in topic_channels:
                topic_listener.add_channel(channel)

            self._client.subscribe(topic)
            self._client.message_callback_add(topic, topic_listener)
            self._listeners[topic] = topic_listener

        self._client.loop_start()

    # noinspection PyUnusedLocal
    def _on_connect_mqtt(self, client: MqttClient, userdata, flags, rc):
        self._logger.info(f"Connected MQTT to {self._host}:{self._port}")

    def disconnect(self) -> None:
        if self.is_connected():
            self._client.loop_stop()
            self._client.disconnect()

    # noinspection PyUnusedLocal
    def _on_disconnect_mqtt(self, client: MqttClient, userdata, rc):
        self._logger.info(f"Disconnected MQTT from {self._host}:{self._port}")

    def read(self, resources: Resources) -> pd.DataFrame:
        raise NotImplementedError("Mqtt does not support reading data in a pull mode")

    def write(self, data: pd.DataFrame) -> None:
        for topic, topic_resources in self.resources.groupby("topic"):
            for resource in topic_resources:
                # TODO: implement writing to MQTT topics
                pass

        raise NotImplementedError("Mqtt does not yet support writing data")


class MqttListener:
    _channels: Dict[str, Channel]
    _topic: str

    def __init__(self, topic: str) -> None:
        self._channels = {}
        self._topic = topic

    def __call__(self, client: MqttClient, userdata, msg) -> None:
        timestamp = pd.Timestamp.now(tz=tz.UTC).floor(freq="s")
        payload = msg.payload.decode()

        for channel in self._channels.values():
            channel.set(timestamp, payload)

    def add_channel(self, channel: Channel) -> None:
        self._channels[channel.id] = channel
