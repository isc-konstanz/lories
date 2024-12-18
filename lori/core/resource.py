# -*- coding: utf-8 -*-
"""
lori.core.resource
~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from collections import OrderedDict
from logging import Logger
from typing import Any, Dict, List, Optional, Type

from lori.core import ConfigurationException, Identifier
from lori.util import parse_type, update_recursive


class Resource(Identifier):
    __configs: OrderedDict[str, Any]

    _key: str
    _name: str
    _group: str

    _type: Type

    _logger: Logger

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        id: str = None,
        key: str = None,
        name: Optional[str] = None,
        group: Optional[str] = None,
        type: Type | str = None,
        **configs: Any,
    ) -> None:
        super().__init__(id=id, key=key, name=name)
        self._logger = logging.getLogger(self.__module__)

        if group is None:
            group = ".".join(self._id.split(".")[:-1])
        self._group = group

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

    def __getitem__(self, attr: str) -> Any:
        value = self.get(attr)
        if value is not None:
            return value
        raise KeyError(attr)

    def get(self, attr: str, default: Optional[Any] = None) -> Any:
        return self._get_vars().get(attr, default)

    def _get_attrs(self) -> List[str]:
        return ["id", "key", "name", "group", "type", *self._copy_configs().keys()]

    def _get_vars(self) -> Dict[str, Any]:
        return OrderedDict(id=self.id, key=self.key, name=self.name, type=self.type, **self._copy_configs())

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.id})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{k}={v}" for k, v in self._get_vars().items())

    @property
    def key(self) -> str:
        return self._key

    @property
    def name(self) -> str:
        return self._name

    @property
    def group(self) -> str:
        return self._group

    @property
    def type(self) -> Type:
        return self._type

    # noinspection PyShadowingBuiltins
    def _update(
        self,
        id: Optional[str] = None,
        key: Optional[str] = None,
        name: Optional[str] = None,
        type: Optional[str | Type] = None,
        **configs: Any,
    ) -> None:
        if id is not None and id != self.id:
            raise ConfigurationException(f"Invalid channel update, trying to change ID from '{self.id}' to '{id}'")
        if key is not None and key != self.key:
            raise ConfigurationException(f"Invalid channel update, trying to change Key from '{self.key}' to '{key}'")
        if name is not None:
            self._name = name
        if type is not None:
            self._type = parse_type(type)
        self.__update_configs(configs)

    def __update_configs(self, configs: Dict[str, Any]) -> None:
        update_recursive(self.__configs, configs)

    def _copy_configs(self) -> Dict[str, Any]:
        return OrderedDict(**self.__configs)

    def copy(self) -> Resource:
        return type(self)(
            id=self.id,
            key=self.key,
            name=self.name,
            group=self.group,
            type=self.type,
            **self._copy_configs(),
        )
