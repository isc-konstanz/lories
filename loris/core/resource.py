# -*- coding: utf-8 -*-
"""
loris.core.resource
~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import builtins
import logging
from collections import OrderedDict
from typing import Any, Dict, List, Mapping, Optional, Type

from loris.core import ConfigurationException
from loris.util import parse_key, parse_name, parse_type


class Resource:
    _id: str
    _key: str
    _name: str

    _type: Type

    __configs: OrderedDict[str, Any]

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        id: str = None,
        key: str = None,
        name: Optional[str] = None,
        type: Type | str = None,
        **configs: Any,
    ) -> None:
        self._logger = logging.getLogger(__name__)

        if key is None:
            raise ConfigurationException(f"Invalid configuration, missing specified {builtins.type(self).__name__} Key")
        self._key = parse_key(key)
        self._id = id if id is not None else self._key
        if self._key != key:
            self._logger.warning(f"{builtins.type(self).__name__} Key contains invalid characters: {key}")

        if name is None:
            name = parse_name(self.key)
        self._name = name

        self._type = parse_type(type)
        self.__configs = OrderedDict(configs)

    def __contains__(self, attr: str) -> bool:
        return attr in self._get_attrs()

    def __getattr__(self, attr: str) -> Any:
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        configs = Resource.__getattribute__(self, f"_{Resource.__name__}__configs")
        if attr in configs.keys():
            return configs[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no configuration '{attr}'")

    def get(self, attr: str, default: Optional[Any] = None) -> Any:
        return self.__configs.get(attr, default)

    def _get_attrs(self) -> List[str]:
        return ["id", "key", "name", "type", *self._copy_configs().keys()]

    def _get_vars(self) -> Dict[str, Any]:
        return OrderedDict(id=self.id, key=self.key, name=self.name, type=self.type, **self._copy_configs())

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.id})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{k}={v}" for k, v in self._get_vars().items())

    @property
    def id(self) -> str:
        return self._id

    @property
    def key(self) -> str:
        return self._key

    @property
    def name(self) -> str:
        return self._name

    @property
    def type(self) -> Type:
        return self._type

    def copy(self) -> Resource:
        return type(self)(
            id=self.id,
            key=self.key,
            type=self.type,
            **self._copy_configs(),
        )

    # noinspection PyShadowingBuiltins
    def _copy_configs(self) -> Dict[str, Any]:
        return OrderedDict(**self.__configs)

    def _update_configs(self, configs: Mapping[str, Any]) -> None:
        self.__configs.update(configs)
