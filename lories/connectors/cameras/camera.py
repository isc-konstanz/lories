# -*- coding: utf-8 -*-
"""
lories.connectors.cameras.camera
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from typing import Any, Dict, Optional

import pandas as pd
from lories.connectors.cameras._core import _CameraConnector
from lories.connectors.cameras.motion import MotionDetector
from lories.connectors.cameras.stream import CameraStream, CameraStreamUnavailableError
from lories.data import Channel
from lories.typing import Resources
from lories.util import to_bool


# noinspection PyAbstractClass
class CameraConnector(_CameraConnector):
    _stream: Optional[CameraStream] = None
    __stream_lock: bool = False

    _is_streaming: bool = False

    def __getstate__(self) -> Dict[str, Any]:
        state = super().__getstate__()
        state.pop("_stream", None)
        state["_is_streaming"] = False
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        state[f"_{CameraConnector.__name__}__stream_lock"] = True
        super().__setstate__(state)

    @property
    def stream(self) -> CameraStream:
        if self._stream is None:
            raise CameraStreamUnavailableError(self)

        return self._stream

    def is_streaming(self) -> bool:
        return self._stream is not None and self._stream.is_enabled() and self._is_streaming

    def connect(self, resources: Resources) -> None:
        super().connect(resources)

        if self.__stream_lock:
            return

        def _is_streaming(channel: Channel) -> bool:
            return to_bool(channel.get("stream", default=False)) or to_bool(channel.get("listener", default=False))

        stream_configs = self.configs.get_member(CameraStream.TYPE, defaults={})
        stream_channels = resources.filter(_is_streaming)
        stream_callbacks = []

        motion = MotionDetector(resources.filter(lambda c: to_bool(c.get("motion_detection", default=False))))
        motion.configure(stream_configs.get_member(MotionDetector.TYPE, defaults={}))
        if motion.is_enabled():
            stream_callbacks.append(motion)

        self._is_streaming = len(stream_channels) > 0
        if self._is_streaming:
            self._stream = CameraStream(self, stream_channels, stream_callbacks, motion)
            self._stream.configure(stream_configs)
            self._stream.start()

    def disconnect(self) -> None:
        super().disconnect()
        if self._is_streaming and not self.__stream_lock:
            self._stream.stop()

    def read(self, resources: Resources) -> pd.DataFrame:
        timestamp = pd.Timestamp.now(tz="UTC").floor(freq="s")

        # TODO: Wrap read_frame() and cache latest frame to only read if frame is older than a second
        data = self.read_frame()
        return pd.DataFrame(data=[[data] * len(resources)], index=[timestamp], columns=list(resources.ids))
