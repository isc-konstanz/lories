# -*- coding: utf-8 -*-
"""
lories.connectors.cameras.motion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

import os
import time
from typing import Optional, Sequence, Tuple

import numpy as np
from lories.core import Configurator
from lories.core.configs.parameters import Parameter
from lories.data import Channels
from lories.typing import Configurations

_AVAILABLE = True
_IMPORT_ERROR = None

try:
    import cv2
except ImportError as _e:
    _AVAILABLE = False
    _IMPORT_ERROR = f"Missing dependency: opencv-python — pip install opencv-python ({_e})"
    cv2 = None  # type: ignore


class MotionDetector(Configurator):
    TYPE: str = "motion"

    threshold = Parameter(
        key="threshold", type=int, default=25, min=0, desc="Foreground mask binarization threshold (0 = skip)"
    )
    dilate_iter = Parameter(
        key="dilate_iter", type=int, default=2, min=0, desc="Dilation iterations for foreground mask"
    )
    alpha = Parameter(key="alpha", type=float, default=0.05, min=0.0, max=1.0, desc="Background model learning rate")
    var_threshold = Parameter(key="var_threshold", type=int, default=32, min=4, desc="MOG2 variance threshold")
    persist_frames = Parameter(
        key="persist_frames", type=int, default=3, min=1, desc="Consecutive frames before triggering motion"
    )
    min_solidity = Parameter(
        key="min_solidity", type=float, default=0.50, min=0.0, max=1.0, desc="Minimum contour solidity"
    )
    min_extent = Parameter(key="min_extent", type=float, default=0.20, min=0.0, max=1.0, desc="Minimum contour extent")
    min_motion_area = Parameter(
        key="min_motion_area", type=int, default=1000, min=0, desc="Minimum contour area in pixels"
    )
    blur_size = Parameter(
        key="blur_size", type=int, default=21, min=1, desc="Gaussian blur kernel size (auto-rounded to odd)"
    )
    cooldown_seconds = Parameter(
        key="cooldown_seconds", type=float, default=2.0, min=0.0, desc="Minimum seconds between motion events"
    )
    _mask_path = Parameter(key="mask", type=str, required=False, default=None, desc="Path to binary mask image")

    threshold: int
    dilate_iter: int
    alpha: float
    var_threshold: int
    persist_frames: int
    min_solidity: float
    min_extent: float
    min_motion_area: int
    blur_size: int
    cooldown_seconds: float
    _mask_path: Optional[str]

    _mask: Optional[np.ndarray]
    _mask_size: Tuple[int, int]

    _persist_counter: int = 0
    _last_motion_time: float = 0.0

    __channels: Channels

    _bg_subtractor: Optional[cv2.BackgroundSubtractorMOG2]
    _kernel_open: Optional[np.ndarray]

    def __init__(self, channels: Channels, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__channels = channels

    def __call__(self, frame: bytes) -> None:
        annotated = self.detect(frame)
        if annotated:
            self._logger.info("Detected motion in camera frame")
            for channel in self.__channels:
                channel.value = annotated

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self.blur_size = self.blur_size if self.blur_size % 2 == 1 else self.blur_size + 1

        self._mask = None
        self._mask_size: Tuple[int, int] = (0, 0)

        self._bg_subtractor = cv2.createBackgroundSubtractorMOG2(
            history=900,
            varThreshold=max(4, self.var_threshold),
            detectShadows=False,
        )
        self._kernel_open = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))

    def _get_mask(self, h: int, w: int) -> np.ndarray:
        size = (h, w)

        if self._mask is not None and self._mask_size == size:
            return self._mask

        if self._mask_path is None or not os.path.exists(self._mask_path):
            return np.full(size, 255, dtype=np.uint8)

        mask_img = cv2.imread(self._mask_path, cv2.IMREAD_GRAYSCALE)
        if mask_img is None:
            return np.full(size, 255, dtype=np.uint8)

        self._mask = cv2.resize(mask_img, (w, h), interpolation=cv2.INTER_NEAREST)
        self._mask_size = size
        return self._mask

    @staticmethod
    def _merge_overlapping_boxes(contours: list) -> Sequence[Tuple[int, int, int, int]]:
        if not contours:
            return []

        rects = [cv2.boundingRect(cnt) for cnt in contours]
        n = len(rects)

        parent = list(range(n))

        def find(node: int) -> int:
            while parent[node] != node:
                parent[node] = parent[parent[node]]
                node = parent[node]
            return node

        def union(a: int, b: int) -> None:
            parent[find(a)] = find(b)

        x1s = [x for x, y, w, h in rects]
        y1s = [y for x, y, w, h in rects]
        x2s = [x + w for x, y, w, h in rects]
        y2s = [y + h for x, y, w, h in rects]

        for i in range(n):
            for j in range(i + 1, n):
                if x1s[i] <= x2s[j] and x2s[i] >= x1s[j] and y1s[i] <= y2s[j] and y2s[i] >= y1s[j]:
                    union(i, j)

        groups: dict[int, Tuple[int, int, int, int]] = {}
        for i in range(n):
            root = find(i)
            if root not in groups:
                groups[root] = (x1s[i], y1s[i], x2s[i], y2s[i])
            else:
                gx1, gy1, gx2, gy2 = groups[root]
                groups[root] = (
                    min(gx1, x1s[i]),
                    min(gy1, y1s[i]),
                    max(gx2, x2s[i]),
                    max(gy2, y2s[i]),
                )

        return [(x1, y1, x2 - x1, y2 - y1) for x1, y1, x2, y2 in groups.values()]

    def _process(self, bgr: np.ndarray) -> list:
        h, w = bgr.shape[:2]

        gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
        gray = cv2.GaussianBlur(gray, (self.blur_size, self.blur_size), 0)

        fg = self._bg_subtractor.apply(gray, learningRate=self.alpha)

        if self.threshold > 0:
            _, fg = cv2.threshold(fg, self.threshold, 255, cv2.THRESH_BINARY)

        clean = cv2.morphologyEx(fg, cv2.MORPH_OPEN, self._kernel_open, iterations=2)

        if self.dilate_iter > 0:
            clean = cv2.dilate(clean, self._kernel_open, iterations=self.dilate_iter)

        mask = self._get_mask(h, w)
        clean = cv2.bitwise_and(clean, (mask > 0).astype(np.uint8) * 255)

        contours, _ = cv2.findContours(clean, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        valid: list = []
        for cnt in contours:
            area = cv2.contourArea(cnt)
            if area < self.min_motion_area:
                continue
            x, y, w_r, h_r = cv2.boundingRect(cnt)
            if w_r == 0 or h_r == 0:
                continue
            hull_area = cv2.contourArea(cv2.convexHull(cnt))
            if hull_area < 1.0:
                continue
            solidity = area / hull_area
            extent = area / float(w_r * h_r)
            if solidity >= self.min_solidity and extent >= self.min_extent:
                valid.append(cnt)

        return valid

    def _reset_background_model(self) -> None:
        self._bg_subtractor = cv2.createBackgroundSubtractorMOG2(
            history=900,
            varThreshold=max(4, self.var_threshold),
            detectShadows=False,
        )
        self._persist_counter = 0

    def detect(self, frame: bytes) -> bytes:
        now = time.monotonic()

        if now - self._last_motion_time < self.cooldown_seconds:
            return b""

        arr = np.frombuffer(frame, dtype=np.uint8)
        bgr = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if bgr is None:
            self._reset_background_model()
            return b""

        motion_contours = self._process(bgr)

        if motion_contours:
            self._persist_counter += 1

            if self._persist_counter >= self.persist_frames:
                self._persist_counter = 0
                self._last_motion_time = now

                bboxes = self._merge_overlapping_boxes(motion_contours)

                for x, y, w, h in bboxes:
                    cv2.rectangle(bgr, (x, y), (x + w, y + h), (0, 0, 255), 2)

                ok, encoded = cv2.imencode(".jpg", bgr)
                return encoded.tobytes() if ok else b""

        else:
            self._persist_counter = 0

        return b""

    def is_enabled(self) -> bool:
        return super().is_enabled() and len(self.__channels) > 0
