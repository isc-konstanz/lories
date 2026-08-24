# -*- coding: utf-8 -*-
"""
lories.data.processors.motion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional, Sequence, Tuple

import cv2

import numpy as np
import pandas as pd
from lories.core.typing import Timestamp
from lories.data.processors import Processor, register_processor_type
from lories.util import to_timedelta


def _seconds(value: Any) -> float:
    """Accept duration strings like '10s' / '2min' or numeric seconds."""
    if isinstance(value, str):
        return to_timedelta(value).total_seconds()
    return float(value)


@register_processor_type("motion")
class MotionDetector(Processor):
    TYPE: str = "motion"

    _mask: Optional[np.ndarray]
    _mask_path: Optional[str]
    _mask_size: Tuple[int, int]

    _persist_counter: int
    _last_motion_time: pd.Timestamp
    _cooldown_logged: bool

    _bg_subtractor: cv2.BackgroundSubtractorMOG2
    _kernel: np.ndarray

    def __init__(
        self,
        blur_size: int = 21,
        var_threshold: int = 32,
        mask: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        blur = int(blur_size) | 1  # ensure odd
        super().__init__(blur_size=blur, var_threshold=int(var_threshold), mask=mask, **kwargs)

        self._mask_path = mask
        self._mask = None
        self._mask_size = (0, 0)

        self._persist_counter = 0
        self._last_motion_time = pd.Timestamp(0, tz="UTC")
        self._cooldown_logged = False

        self._bg_subtractor = cv2.createBackgroundSubtractorMOG2(
            history=900,
            varThreshold=max(4, int(var_threshold)),
            detectShadows=False,
        )
        self._kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))

    def __getstate__(self) -> Dict[str, Any]:
        # cv2.BackgroundSubtractorMOG2 is not picklable; rebuild on the other side.
        state = super().__getstate__()
        state.pop("_bg_subtractor", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        super().__setstate__(state)
        self._reset_background_model()

    def process(
        self,
        timestamp: Timestamp,
        value: Any,
        threshold: int = 25,
        dilate_iter: int = 2,
        alpha: float = 0.05,
        persist_frames: int = 3,
        min_solidity: float = 0.50,
        min_extent: float = 0.20,
        min_motion_area: int = 1000,
        blur_size: int = 21,
        cooldown: Any = "2s",
        **kwargs: Any,
    ) -> Any:
        if not isinstance(value, (bytes, bytearray)):
            return Processor.SKIP

        # .copy() detaches the array from the bytes buffer. np.frombuffer alone
        # leaves a (bytes <-> memoryview <-> ndarray) cycle that surfaces as
        # "Exception ignored in tp_clear of memoryview" when the channel value
        # gets reassigned mid-decode.
        arr = np.frombuffer(value, dtype=np.uint8).copy()
        bgr = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if bgr is None:
            self._reset_background_model()
            return Processor.SKIP

        # Always run detection so the bg subtractor keeps learning. Without
        # this, the bg model would freeze during cooldown and the first frame
        # after cooldown would show a stale diff, re-triggering motion on a
        # static scene.
        contours = self._detect_contours(
            bgr,
            threshold=int(threshold),
            dilate_iter=int(dilate_iter),
            alpha=float(alpha),
            min_solidity=float(min_solidity),
            min_extent=float(min_extent),
            min_motion_area=int(min_motion_area),
            blur_size=int(blur_size) | 1,
        )

        # Cooldown: bg model already updated above, just suppress the emit.
        cooldown_s = _seconds(cooldown)
        if (timestamp - self._last_motion_time).total_seconds() < cooldown_s:
            self._persist_counter = 0
            if not self._cooldown_logged:
                self._logger.info("Motion cooldown active for %.1fs; suppressing emits", cooldown_s)
                self._cooldown_logged = True
            return Processor.SKIP

        if self._cooldown_logged:
            self._logger.debug("Motion cooldown ended")
            self._cooldown_logged = False

        if not contours:
            self._persist_counter = 0
            return Processor.SKIP

        self._persist_counter += 1
        if self._persist_counter < int(persist_frames):
            return Processor.SKIP

        self._persist_counter = 0
        self._last_motion_time = timestamp

        for x, y, w, h in self._merge_overlapping_boxes(contours):
            cv2.rectangle(bgr, (x, y), (x + w, y + h), (0, 0, 255), 2)

        ok, encoded = cv2.imencode(".jpg", bgr)
        if not ok:
            return Processor.SKIP
        self._logger.info("Motion detected; cooldown %.1fs", cooldown_s)
        return encoded.tobytes()

    def _detect_contours(
        self,
        bgr: np.ndarray,
        threshold: int,
        dilate_iter: int,
        alpha: float,
        min_solidity: float,
        min_extent: float,
        min_motion_area: int,
        blur_size: int,
    ) -> list:
        h, w = bgr.shape[:2]

        gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
        gray = cv2.GaussianBlur(gray, (blur_size, blur_size), 0)

        fg = self._bg_subtractor.apply(gray, learningRate=alpha)
        if threshold > 0:
            _, fg = cv2.threshold(fg, threshold, 255, cv2.THRESH_BINARY)

        clean = cv2.morphologyEx(fg, cv2.MORPH_OPEN, self._kernel, iterations=2)
        if dilate_iter > 0:
            clean = cv2.dilate(clean, self._kernel, iterations=dilate_iter)

        mask = self._get_mask(h, w)
        clean = cv2.bitwise_and(clean, (mask > 0).astype(np.uint8) * 255)

        contours, _ = cv2.findContours(clean, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        valid: list = []
        for cnt in contours:
            area = cv2.contourArea(cnt)
            if area < min_motion_area:
                continue
            x, y, w_r, h_r = cv2.boundingRect(cnt)
            if w_r == 0 or h_r == 0:
                continue
            hull_area = cv2.contourArea(cv2.convexHull(cnt))
            if hull_area < 1.0:
                continue
            solidity = area / hull_area
            extent = area / float(w_r * h_r)
            if solidity >= min_solidity and extent >= min_extent:
                valid.append(cnt)
        return valid

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

    def _reset_background_model(self) -> None:
        var_threshold = int(self.get("var_threshold", default=32))
        self._bg_subtractor = cv2.createBackgroundSubtractorMOG2(
            history=900,
            varThreshold=max(4, var_threshold),
            detectShadows=False,
        )
        self._persist_counter = 0

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
