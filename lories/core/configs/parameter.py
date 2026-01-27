# -*- coding: utf-8 -*-
"""
lories.core.configs.configurator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations


from lories._core import _Context, _Entity  # noqa
from lories._core._configurations import Configurations, _Configurations  # noqa
from lories._core._configurator import Configurator as ConfiguratorType  # noqa
from lories._core._configurator import _Configurator  # noqa



class Parameters:
    def __init__(self, name: str, params: list[Parameter]) -> None:
        self.name = name
        self.params = params

    def __str(self):
        return f"Parameters(name={self.name}, params={[str(p) for p in self.params]})"


class Parameter:
    def __init__(self, *, name: str, default=None, required=True, desc=None):
        if not isinstance(name, str) or name == "":
            raise ValueError("Parameter 'name' must be a non-empty string")
        self.name = name

        if not isinstance(desc, str) or desc == "":
            # TODO: logger warning
            raise ValueError("Parameter 'desc' must be a non-empty string")
        self.desc = desc

        self.default = default
        self.required = required

    def __str__(self):
        return f"Parameter(name={self.name}, default={self.default}, required={self.required}, desc={self.desc})"


def with_parameters(param_list):
    def _collect_params(param_items, collected):
        for obj in param_items:
            if isinstance(obj, Parameters):
                collected[obj.name] = _collect_params(obj.params, {})
            elif isinstance(obj, Parameter):
                collected[obj.name] = obj
            else:
                raise TypeError(f"Invalid parameter type: {type(obj)}")
        return collected

    def decorator(cls):
        parent_params = getattr(cls, "__config_parameters__", {}).copy()
        collected = _collect_params(param_list, {})
        cls.__config_parameters__ = {**parent_params, **collected}
        return cls

    return decorator

