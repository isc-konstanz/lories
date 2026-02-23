# -*- coding: utf-8 -*-
"""
lories.data.processors.processor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import ABCMeta
from collections import OrderedDict
from functools import wraps
from typing import Any, Dict, List, Optional

from lories._core import _Processor  # noqa
from lories.util import to_bool, update_recursive
from lories.core.typing import Timestamp


class ProcessorMeta(ABCMeta):
    def __call__(cls, *args, **kwargs):
        processor = super().__call__(*args, **kwargs)
        cls._wrap_method(processor, "process")

        return processor

    # noinspection PyShadowingBuiltins
    @staticmethod
    def _wrap_method(object: Any, method: str) -> None:
        _wrap_method = getattr(object, f"_do_{method}")
        _run_method = getattr(object, method)
        setattr(object, f"_run_{method}", _run_method)
        setattr(object, method, _wrap_method)


# noinspection PyAbstractClass
class Processor(_Processor, metaclass=ProcessorMeta):
    __configs: OrderedDict[str, Any]

    enabled: bool = False

    # noinspection PyShadowingBuiltins
    def __init__(self,
        id: Optional[str] = None,
        key: Optional[str] = None,
        name: Optional[str] = None,
        enabled: bool = True,
        **configs
    ) -> None:
        super().__init__(id, key, name)
        self.enabled = to_bool(enabled)
        self.__configs = OrderedDict(configs)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Processor) and self._get_vars() == other._get_vars()

    def __hash__(self) -> int:
        return hash(*self._get_vars().values())

    def __contains__(self, attr: str) -> bool:
        return attr in self._get_attrs()

    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        configs = Processor.__getattribute__(self, f"_{Processor.__name__}__configs")
        if attr in configs.keys():
            return configs[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no configuration '{attr}'")

    def __getitem__(self, attr: str) -> Any:
        value = self.get(attr)
        if value is not None:
            return value
        raise KeyError(attr)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.id})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\tid={self.id}\n\t" + "\n\t".join(
            f"{k}={v}" for k, v in self._get_vars().items()
        )

    def __call__(self, timestamp: Timestamp, value: Any) -> Any:
        return self._do_process(timestamp, value)

    # noinspection PyTypeChecker
    @wraps(_Processor.process, updated=())
    def _do_process(self, timestamp: Timestamp, value: Any, *args, **kwargs) -> Any:
        kwargs.update(self._get_configs())
        prepared_value = self._at_process(timestamp, value, **kwargs)
        processed_value = self._run_process(timestamp, prepared_value, *args, **kwargs)

        self._on_process(timestamp, value, processed_value)
        return processed_value

    # noinspection PyMethodMayBeStatic, PyUnusedLocal
    def _at_process(self, timestamp: Timestamp, value: Any, **kwargs) -> Any:
        return value

    def _on_process(self, timestamp: Timestamp, value: Any, update: Any) -> None:
        pass

    def get(self, attr: str, default: Optional[Any] = None) -> Any:
        return self._get_vars().get(attr, default)

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        return {
            **self.__configs,
            "enabled": self.enabled,
        }

    def _get_attrs(self) -> List[str]:
        return [*self._copy_configs().keys(), "enabled"]

    def _get_configs(self) -> Dict[str, Any]:
        return self.__configs

    def _copy_configs(self) -> Dict[str, Any]:
        return OrderedDict(**self._get_configs())

    def __update_configs(self, configs: Dict[str, Any]) -> None:
        update_recursive(self.__configs, configs)

    def _update(
        self,
        enabled: Optional[str | bool] = None,
        **configs: Any,
    ) -> None:
        if enabled is not None:
            self.enabled = to_bool(enabled)
        self.__update_configs(configs)

    def to_configs(self) -> Dict[str, Any]:
        return {
            **self.__configs,
            "enabled": self.enabled,
        }
