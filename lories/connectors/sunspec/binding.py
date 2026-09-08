# -*- coding: utf-8 -*-
"""
lories.connectors.sunspec.binding
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union

from lories.core import ConfigurationError, Constant
from lories.typing import Configurations


class SunSpecBinding:
    """
    Mixin that binds a `BindableComponent` vocabulary to a SunSpec device. Subclasses declare
    the models they accept, the model used when the config names none, and the point name for
    each constant they can read; every constant left out of `POINTS` stays unbound, to be
    filled by TOML or by another component. A `POINTS` entry may be a plain point name, or a
    `(point, scale)` tuple when the raw SunSpec value needs an additional linear scale factor
    beyond the model's own scale-factor registers.

    Wiring a device needs its Modbus unit id, its model id, and, if several instances of that
    model sit on that one unit, the instance. The unit id is what lets several devices share
    one connector: the `sunspec` connector owns a transport endpoint and routes each channel to
    the unit its `device` key names, so an inverter and a meter behind one gateway are two
    components against one connector. Because a binding is written as ordinary channel config,
    TOML can override `device`, `model`, `point`, `instance` or `connector` per channel, or
    repurpose a channel for another protocol entirely.

    This mixin knows point names and model ids. Everything about Modbus, transports and reads
    belongs to the `sunspec` connector.
    """

    CONNECTOR_TYPES = ("sunspec",)

    MODELS: List[int] = []
    DEFAULT_MODEL: Optional[int] = None
    POINTS: Dict[Constant, Union[str, Tuple[str, float]]] = {}

    device: int
    model: int
    instance: int

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        from lories.components.binding import BindableComponent

        if not issubclass(cls, BindableComponent):
            raise TypeError(f"{cls.__name__} mixes in SunSpecBinding without a BindableComponent base")
        mro = cls.__mro__
        if mro.index(SunSpecBinding) > mro.index(BindableComponent):
            raise TypeError(f"{cls.__name__} must list SunSpecBinding before BindableComponent")

    def _configure_bindings(self, configs: Configurations) -> None:
        super()._configure_bindings(configs)
        self.device = configs.get_int("device")
        if self.device is None:
            raise ConfigurationError(
                f"{type(self).__name__} requires the SunSpec 'device' unit id its connector addresses it by"
            )
        self.model = configs.get_int("model", default=type(self).DEFAULT_MODEL)
        if self.model not in type(self).MODELS:
            raise ConfigurationError(
                f"Invalid SunSpec model '{self.model}' for {type(self).__name__}, expected one of {type(self).MODELS}"
            )
        self.instance = configs.get_int("instance", default=1)

    def _bind(self, constant: Constant) -> Dict[str, Any]:
        point = self._point(constant)
        if point is None:
            return {}
        binding = {
            "point": point,
            "device": self.device,
            "model": self._model(constant),
            "instance": self.instance,
            "connector": self._connector_id,
        }
        scale = self._scale(constant)
        if scale is not None:
            binding["converter"] = {"type": "linear", "scale": scale}
        return binding

    def _point(self, constant: Constant) -> Optional[str]:
        entry = type(self).POINTS.get(constant)
        if entry is None:
            return None
        return entry[0] if isinstance(entry, tuple) else entry

    def _scale(self, constant: Constant) -> Optional[float]:
        entry = type(self).POINTS.get(constant)
        if isinstance(entry, tuple):
            return entry[1]
        return None

    # noinspection PyUnusedLocal
    def _model(self, constant: Constant) -> int:
        return self.model
