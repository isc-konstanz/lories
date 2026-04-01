# -*- coding: utf-8 -*-
"""
lories.connectors.cameras.camera
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd
from lories.connectors.cameras._core import _CameraConnector
from lories.connectors.cameras.motion import MotionDetector
from lories.connectors.cameras.stream import CameraStream, CameraStreamUnavailableError
from lories.core.configs.parameters import Parameter, ParameterGroup, ResourceParameter
from lories.data import Channel
from lories.typing import Resources
from lories.util import to_bool


# noinspection PyAbstractClass
class CameraConnector(_CameraConnector):
    _stream_config = ParameterGroup(
        key=CameraStream.TYPE,
        required=False,
        desc="Live streaming configuration",
        children=[
            Parameter(key="fps", type=int, default=30, min=1, max=120, desc="Target frames per second"),
            ParameterGroup(
                key=MotionDetector.TYPE,
                required=False,
                desc="Motion detection configuration",
                children=[
                    Parameter(
                        key="threshold",
                        type=int,
                        default=25,
                        min=0,
                        desc="Foreground mask binarization threshold (0 = skip)",
                    ),
                    Parameter(
                        key="dilate_iter", type=int, default=2, min=0, desc="Dilation iterations for foreground mask"
                    ),
                    Parameter(
                        key="alpha", type=float, default=0.05, min=0.0, max=1.0, desc="Background model learning rate"
                    ),
                    Parameter(key="var_threshold", type=int, default=32, min=4, desc="MOG2 variance threshold"),
                    Parameter(
                        key="persist_frames",
                        type=int,
                        default=3,
                        min=1,
                        desc="Consecutive frames before triggering motion",
                    ),
                    Parameter(
                        key="min_solidity", type=float, default=0.50, min=0.0, max=1.0, desc="Minimum contour solidity"
                    ),
                    Parameter(
                        key="min_extent", type=float, default=0.20, min=0.0, max=1.0, desc="Minimum contour extent"
                    ),
                    Parameter(
                        key="min_motion_area", type=int, default=1000, min=0, desc="Minimum contour area in pixels"
                    ),
                    Parameter(
                        key="blur_size",
                        type=int,
                        default=21,
                        min=1,
                        desc="Gaussian blur kernel size (auto-rounded to odd)",
                    ),
                    Parameter(
                        key="cooldown_seconds",
                        type=float,
                        default=2.0,
                        min=0.0,
                        desc="Minimum seconds between motion events",
                    ),
                    Parameter(key="mask", type=str, required=False, default=None, desc="Path to binary mask image"),
                ],
            ),
        ],
    )

    # Channel-level connection parameters
    listener = ResourceParameter(
        key="listener", type=bool, required=False, default=False, desc="Subscribe to stream frame events"
    )
    motion_detection = ResourceParameter(
        key="motion_detection", type=bool, required=False, default=False, desc="Enable motion detection on this channel"
    )

    _stream: Optional[CameraStream] = None
    __stream_lock: bool = False
    __streaming: bool = False

    def __getstate__(self) -> Dict[str, Any]:
        state = super().__getstate__()
        state.pop("_stream", None)
        state[f"_{CameraConnector.__name__}__streaming"] = False
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
        return self.__streaming and self._stream is not None and self._stream.is_enabled()

    # noinspection PyMethodMayBeStatic
    def _is_streaming(self, channel: Channel) -> bool:
        return to_bool(channel.get("stream", default=False)) or to_bool(channel.get("listener", default=False))

    def connect(self, resources: Resources) -> None:
        super().connect(resources)

        if self.__stream_lock:
            return

        stream_configs = self.configs.get_member(CameraStream.TYPE, defaults={})
        stream_channels = resources.filter(self._is_streaming)
        stream_callbacks = []

        motion = MotionDetector(resources.filter(lambda c: to_bool(c.get("motion_detection", default=False))))
        motion.configure(stream_configs.get_member(MotionDetector.TYPE, defaults={}))
        if motion.is_enabled():
            stream_callbacks.append(motion)

        self.__streaming = len(stream_channels) > 0
        if self.__streaming:
            self._stream = CameraStream(self, stream_channels, stream_configs, stream_callbacks, motion)
            self._stream.configure(stream_configs)
            self._stream.start()

    def disconnect(self) -> None:
        super().disconnect()
        if self.__streaming and not self.__stream_lock:
            self._stream.stop()

    def read(self, resources: Resources) -> pd.DataFrame:
        timestamp = pd.Timestamp.now(tz="UTC").floor(freq="s")

        # TODO: Wrap read_frame() and cache latest frame to only read if frame is older than a second
        data = []
        columns = []
        for resource in resources:
            columns.append(resource.id)
            data.append(self.read_frame(resource))
        return pd.DataFrame(data=[data], index=[timestamp], columns=columns)
