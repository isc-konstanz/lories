# -*- coding: utf-8 -*-
"""
lori.simulation.result
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any, Dict, Iterator, List, MutableMapping, Optional

from lori.core import Constant


class Result(MutableMapping):
    __map: OrderedDict[str, Any]

    _key: str
    _name: str
    _header: str

    summary: Any

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        key: str,
        name: str,
        summary: Any,
        header: str = "Summary",
        **kwargs: Any,
    ) -> None:
        self._key = key
        self._name = name
        self._header = header
        self.__map = OrderedDict(kwargs)

        self.summary = summary

    def __len__(self) -> int:
        return len(self.__map) + 4

    def __iter__(self) -> Iterator[str]:
        return iter(self._get_attrs())

    def __contains__(self, attr: str) -> bool:
        return attr in self._get_attrs()

    def __getattr__(self, attr: str) -> Any:
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        configs = Result.__getattribute__(self, f"_{Result.__name__}__map")
        if attr in configs.keys():
            return configs[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no value '{attr}'")

    def __getitem__(self, key: str) -> Any:
        value = self.get(key)
        if value is not None:
            return value
        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        if key in ["key", "name", "header"]:
            raise KeyError(f"Unable to set result '{key}' attribute")
        elif key == "summary":
            self.summary = value
        else:
            self.__map[key] = value

    def __delitem__(self, key: str) -> None:
        if key in ["key", "name", "header", "summary"]:
            raise KeyError(f"Unable to delete result '{key}' attribute")
        del self.__map[key]

    def get(self, attr: str, default: Optional[Any] = None) -> Any:
        return self._get_vars().get(attr, default)

    def _get_attrs(self) -> List[str]:
        return ["key", "name", "header", "summary", *self.__map.keys()]

    def _get_vars(self) -> Dict[str, Any]:
        return OrderedDict(key=self.key, name=self.name, header=self.header, summary=self.summary, **self.__map)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.key})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{k}={v}" for k, v in self._get_vars().items())

    @property
    def key(self) -> str:
        return self._key

    @property
    def name(self) -> str:
        return self._name

    @property
    def header(self) -> str:
        return self._header

    @classmethod
    def from_const(cls, constant: Constant, summary: Any, header: Optional[str] = "Summary", **kwargs) -> Result:
        return cls(constant.key, constant.full_name(unit=True), summary, header=header, **kwargs)
