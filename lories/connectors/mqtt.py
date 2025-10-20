# -*- coding: utf-8 -*-
"""
lories.connectors.mqtt
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

# TODO: add paho and jsonpath-ng to requirements.txt
from typing import Optional, Dict
import pandas as pd
import pytz as tz
import paho.mqtt.client as mqtt
import json
import jsonpath_ng as jp

from lories.data import Channel
from lories.core import Configurations, Resources
from lories.connectors import Connector, register_connector_type


@register_connector_type("mqtt")
class MqttConnector(Connector):
    TRANSPORTS = ["tcp", "websockets", "unix"]

    host: str
    port: int
    timeout: int
    username: Optional[str]
    password: Optional[str]

    keep_alive_interval: Optional[int]
    keep_alive_message: Optional[(str, str)]

    _client: Optional[mqtt.Client]
    _listeners: Dict[str, MqttListener]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._listeners = {}


    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        transport = configs.get("transport", default="tcp")
        if transport not in self.TRANSPORTS:
            raise ValueError(f"Invalid MQTT transport: {transport} (valid: {self.TRANSPORTS})")

        self._client = mqtt.Client(
            client_id=self.id,
            clean_session=configs.get_bool("clean_session", default=True),
            transport=transport,
            #userdata=None,
            #protocol=mqtt.MQTTv311,
        )

        self.host = configs.get("host", default="localhost")
        self.port = configs.get_int("port", default=1883)
        self.timeout = configs.get_int("timeout", default=60)
        self.username = configs.get("username", default=None)
        self.password = configs.get("password", default=None)

        if self.username is not None and self.password is not None:
            self._client.username_pw_set(self.username, self.password)
        elif self.username is not None or self.password is not None:
            raise ValueError("MQTT username and password must both be provided")

        # self._client.tls_set(
        #     ca_certs=configs.get("ca_certs", default=None),
        #     certfile=configs.get("certfile", default=None),
        #     keyfile=configs.get("keyfile", default=None),
        #     cert_reqs=configs.get("cert_reqs", default=None),
        #     tls_version=configs.get("tls_version", default=None),
        #     ciphers=configs.get("ciphers", default=None),
        #     keyfile_password=configs.get("keyfile_password", default=None),
        #     alpn_protocols=configs.get("alpn_protocols", default=None),
        # )

        self._client.on_connect = self._on_connect_mqtt
        self._client.on_disconnect = self._on_disconnect_mqtt


    def connect(self, resources: Resources) -> None:
        self._client.connect(self.host, self.port, self.timeout)
        # self._client.subscribe("#") # subscribe to all topics

        channels = resources.filter(lambda r: isinstance(r, Channel))
        for topic, grouped_channels in channels.groupby("topic"):
            topic_channel_listener = MqttListener(topic)
            self._client.subscribe(topic)
            self._client.message_callback_add(topic, topic_channel_listener)
            for channel in grouped_channels:
                topic_channel_listener.add_channel(channel)

            self._listeners[topic] = topic_channel_listener

        self._client.loop_start()

    def disconnect(self) -> None:
        if self._client is not None:
            self._client.loop_stop()
            self._client.disconnect()
            self._client = None

    def is_connected(self) -> bool:
        return self._client is not None and self._client.is_connected()

    def read(self, resources: Resources) -> pd.DataFrame:
        raise NotImplementedError("MqttConnector does not support directly reading data")

    def write(self, data: pd.DataFrame) -> None:
        for topic, grouped_resources in self.resources.groupby("topic"):
            for resource in grouped_resources:
                pass
                #TODO: implement writing to MQTT topics

        raise NotImplementedError("MqttConnector does not support writing data")


    def _on_connect_mqtt(self, client, userdata, flags, rc):
        self._logger.info(f"Connected MQTT to {self.host}:{self.port}")

    def _on_disconnect_mqtt(self, client, userdata, rc):
        self._logger.info(f"Disconnected MQTT from {self.host}:{self.port}")


class MqttListener:

    _topic: str
    _channels: Dict[str, Channel]

    def __init__(self, topic: str):
        self._topic = topic
        self._channels = {}

    def __call__(self, client, userdata, msg) -> None:
        now = pd.Timestamp.now(tz=tz.UTC).floor(freq="s")
        payload = msg.payload.decode()

        is_json = False
        if payload is not None and isinstance(payload, str) and payload.startswith("{") and payload.endswith("}"):
            try:
                payload = json.loads(payload)
                is_json = True
            except json.JSONDecodeError:
                pass

        for channel in self._channels.values():
            if not is_json:
                channel.set(now, payload)
                continue

            path = channel.get("path", None)
            if path is None:
                channel.set(now, payload)
                continue

            jsonpath_expr = jp.parse(path)
            matches = jsonpath_expr.find(payload)
            if matches is None:
                channel.set(now, payload)
                continue

            if len(matches) == 1:
                channel.set(now, matches[0].value)
            else:
                channel.set(now, [match.value for match in matches])

    def add_channel(self, channel: Channel) -> None:
        self._channels[channel.id] = channel
