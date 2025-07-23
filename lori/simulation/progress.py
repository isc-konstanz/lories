# -*- coding: utf-8 -*-
"""
lori.simulation.progress
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import json
import logging
import multiprocessing as process
import os
from typing import Any, Optional


class Progress:
    # noinspection PyUnresolvedReferences
    def __init__(
        self,
        desc: Optional[str] = None,
        total: Optional[int] = None,
        value: Optional[process.Value | int] = None,
        file: Optional[str] = None,
        **kwargs,
    ) -> None:
        self._logger = logging.getLogger(self.__module__)

        # Manually disable the tqdm progress bars for testing on broken windows consoles
        # kwargs['disable'] = True
        # kwargs['file'] = sys.stdout

        if value is not None and type(value).__name__ == "Synchronized" and value.value > 0:
            kwargs["initial"] = value.value

        self._total = total
        self._value = value
        self._file = file
        if os.path.exists(file):
            with open(self._file, "r", encoding="utf-8") as file:
                self.status = json.load(file).get("status", "loaded")
        else:
            self.status = "initialized"
        try:
            from tqdm import tqdm

            self._bar = tqdm(desc=desc, total=total, **kwargs)

        except ImportError as e:
            self._logger.debug("Unable to import tqdm progress library: %s", e)
            self._bar = None

    def close(self) -> None:
        if self._bar:
            self._bar.close()

    # noinspection PyTypeChecker
    def complete(self, status: str = "success", **results: Any) -> None:
        self._update(self._total, status, dump=False)
        if self._file is not None:
            with open(self._file, "w", encoding="utf-8") as file:
                json.dump({"status": status, **results}, file, ensure_ascii=False, indent=4)

    # noinspection PyUnresolvedReferences
    def update(self) -> None:
        if self._value is not None:
            if type(self._value).__name__ == "Synchronized":
                with self._value.get_lock():
                    self._value.value += 1
                    self._update(self._value.value)
            else:
                self._value += 1
                self._update(self._value)
        elif self._bar:
            self._bar.update()

    # noinspection PyTypeChecker
    def _update(self, value: int, status: str = "running", dump: bool = True) -> None:
        progress = value / self._total * 100
        if progress % 1 <= 1 / self._total * 100 and self._file is not None and dump:
            with open(self._file, "w", encoding="utf-8") as file:
                results = {
                    "status": status,
                    "progress": int(progress),
                }
                json.dump(results, file, ensure_ascii=False, indent=4)

        self.status = status
        if self._bar:
            self._bar.update(value - self._bar.n)
