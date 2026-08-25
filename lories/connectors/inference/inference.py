# -*- coding: utf-8 -*-
"""
lories.connectors.inference.inference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Abstract connector subtype for AI inference runtimes. An inference connector
owns exactly one loaded model (weights file, device, threads) and extends the
connector contract with ``infer``, following the ``Database`` pattern of
extending ``Connector`` through a wrapping metaclass.

Inference connectors serve no channels: binding a channel to one is a
configuration error, and ``read``/``write`` raise. All inference connectors in
the process share one concurrency gate (default: one inference at a time), so
multiple models on weak hardware serialize instead of thrashing.
"""

from __future__ import annotations

import os
from abc import abstractmethod
from functools import wraps
from threading import BoundedSemaphore, Lock
from typing import Any, ClassVar, Dict, Optional

import pandas as pd
from lories.connectors.connector import Connector, ConnectorMeta
from lories.connectors.errors import ConnectionError, ConnectorError
from lories.core import Configurations, Resources
from lories.core.configs.errors import ConfigurationError
from lories.core.configs.parameters import Parameter


class InferenceConnectorMeta(ConnectorMeta):
    # noinspection PyProtectedMember
    def __call__(cls, *args, **kwargs):
        connector = super().__call__(*args, **kwargs)
        cls._wrap_method(connector, "infer")

        return connector


# noinspection PyAbstractClass
class InferenceConnector(Connector, metaclass=InferenceConnectorMeta):
    # Process-wide inference gate shared by ALL inference connectors, sized on
    # first configure. Bounds concurrent model passes so several models on a
    # weak box serialize instead of oversubscribing the cores.
    __gate: ClassVar[Optional[BoundedSemaphore]] = None
    __gate_limit: ClassVar[Optional[int]] = None
    __gate_lock: ClassVar[Lock] = Lock()

    _weights = Parameter(
        key="weights",
        type=str,
        required=True,
        desc="Path to the model weights file",
    )
    _device = Parameter(
        key="device",
        type=str,
        default="auto",
        desc="Runtime device, e.g. 'auto', 'cpu' or 'cuda'",
    )
    _threads = Parameter(
        key="threads",
        type=int,
        required=False,
        min=1,
        desc="Compute threads for the model runtime (default: the runtime's own default)",
    )
    _max_concurrent = Parameter(
        key="max_concurrent",
        type=int,
        default=1,
        min=1,
        desc="Process-wide cap on concurrent inferences, shared by all inference connectors",
    )

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        if not getattr(type(self), "__available__", True):
            reason = getattr(type(self), "__import_error__", "missing dependency")
            raise ConfigurationError(f"'{type(self).__name__}' is unavailable: {reason}")
        if not os.path.isfile(self._weights):
            raise ConfigurationError(f"Model weights not found: {self._weights}")

        self._configure_gate(self._max_concurrent)

    @classmethod
    def _configure_gate(cls, limit: int) -> None:
        base = InferenceConnector
        with base.__gate_lock:
            if base.__gate is None:
                base.__gate = BoundedSemaphore(limit)
                base.__gate_limit = limit
            elif base.__gate_limit != limit:
                raise ConfigurationError(
                    f"Conflicting 'max_concurrent' values for inference connectors: the shared "
                    f"gate is sized {base.__gate_limit}, this connector declares {limit}. "
                    f"All inference connectors must agree."
                )

    @classmethod
    def _gate(cls) -> BoundedSemaphore:
        base = InferenceConnector
        with base.__gate_lock:
            if base.__gate is None:
                base.__gate = BoundedSemaphore(1)
                base.__gate_limit = 1
            return base.__gate

    @abstractmethod
    def infer(self, inputs: Any, **kwargs) -> Any:
        """Run one inference pass. Inputs and outputs are numpy structures."""

    @property
    @abstractmethod
    def meta(self) -> Dict[str, Any]:
        """Metadata of the loaded model (e.g. ``input_hw``); empty before connect."""

    # noinspection PyUnresolvedReferences, PyTypeChecker
    @wraps(infer, updated=())
    def _do_infer(self, inputs: Any, locking: bool = True, *args, **kwargs) -> Any:
        try:
            if not self._lock.acquire(blocking=locking, timeout=self._lock_timeout):
                raise ConnectorError(self, f"Timeout acquiring lock for inference {type(self).__name__}: {self.id}")
            if not self._is_connected():
                raise ConnectionError(self, f"Trying to infer with unconnected {type(self).__name__}: {self.id}")

            with type(self)._gate():
                return self._run_infer(inputs, *args, **kwargs)

        finally:
            self._lock.release()

    def connect(self, resources: Resources) -> None:
        if len(resources) > 0:
            raise ConfigurationError(
                f"'{self.id}' is a compute runtime and serves no channels; remove the connector "
                f"reference from: {', '.join(r.id for r in resources)}"
            )

    def disconnect(self) -> None:
        pass

    def read(self, resources: Resources) -> pd.DataFrame:
        raise ConnectorError(self, f"'{self.id}' is a compute runtime and serves no channels")

    def write(self, data: pd.DataFrame) -> None:
        raise ConnectorError(self, f"'{self.id}' is a compute runtime and serves no channels")
