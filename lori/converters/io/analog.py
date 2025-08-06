# -*- coding: utf-8 -*-
"""
lori.converters.io.analog
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Optional

from lori import ConfigurationException, Configurations
from lori.converters import ConversionException, register_converter_type
from lori.converters.converter import FloatConverter


@register_converter_type("analog_input")
class AnalogInput(FloatConverter):
    INPUT_KEY: str = "input"

    _input_max: float
    _input_min: float
    _input_zero: float

    _invert: bool
    _factor: float
    _divisor: float

    max: float
    min: float

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.min = configs.get_float("value_min", default=0.0)
        self.max = configs.get_float("value_max")
        if self.max <= self.min:
            raise ConfigurationException(
                f"Invalid value boundaries for '{self.id}' with min ({self.min}) > max ({self.max})"
            )
        self._configure_input(configs)
        self._assert_input(self._input_min, self._input_max, self._input_zero)

        self._invert = configs.get_bool("invert", default=False)
        self._factor = configs.get_float("factor", default=1)
        self._divisor = (self._input_max - self._input_min) / (self.max - self.min)

    def _configure_input(
        self,
        configs: Configurations,
        default_max: float = None,
        default_min: float = 0.0,
    ) -> None:
        self._input_max = configs.get_float(f"{self.INPUT_KEY}_max", default=default_max)
        self._input_min = configs.get_float(f"{self.INPUT_KEY}_min", default=default_min)
        self._input_zero = configs.get_float(f"{self.INPUT_KEY}_zero", default=self._input_min)

    # noinspection PyShadowingBuiltins
    def _assert_input(self, min: float, max: float, zero: float) -> None:
        if min is None:
            raise ConfigurationException(f"Missing input minimum for '{self.id}'")
        if max is None:
            raise ConfigurationException(f"Missing input maximum for '{self.id}'")
        if max <= min:
            raise ConfigurationException(f"Invalid input boundaries for '{self.id}' with min ({min}) > max ({max})")

        if zero is None:
            raise ConfigurationException(f"Missing input zero point for '{self.id}'")
        if min > zero > max:
            raise ConfigurationException(f"Invalid input zero point for '{self.id}': {zero}")

    def convert(self, value: Any, **kwargs) -> Optional[float]:
        if self._input_min > value > self._input_max:
            raise ConversionException(
                f"Invalid input signal out of limit ({self._input_min} to {self._input_max}): " + str(value)
            )
        _value = (value * self._factor - self._input_zero) / self._divisor

        if self._invert:
            _value = self.max - _value
        return _value
