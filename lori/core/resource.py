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

from lori._core._resource import Resource as ResourceType  # noqa
from lori._core._resource import _Resource  # noqa
from lori.core.configs import ConfigurationError
from lori.core.errors import ResourceError
from lori.core.resources import Resources
from lori.util import parse_type, update_recursive, validate_key


class Resource(_Resource):
    __configs: OrderedDict[str, Any]

    _group: str
    _unit: Optional[str]
    _type: Type[Any]

    _logger: Logger

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        id: str = None,
        key: str = None,
        name: Optional[str] = None,
        group: Optional[str] = None,
        unit: Optional[str] = None,
        type: Type | str = None,
        **configs: Any,
    ) -> None:
        super().__init__(id=id, key=key, name=name)
        self._logger = logging.getLogger(self.__module__)

        if group is None:
            group = "_".join(self._id.split(".")[:-1])
        self._group = self._assert_group(group)
        self._unit = self._assert_unit(unit)
        self._type = self._assert_type(parse_type(type))
        self.__configs = OrderedDict(configs)

    @classmethod
    def _assert_group(cls, __group: str) -> str:
        if __group is None:
            raise ResourceError(f"Invalid {cls.__name__}, missing specified 'group'")
        _group = validate_key(__group)
        if _group != __group:
            raise ResourceError(f"Invalid characters in '{cls.__name__}' group: " + __group)
        return _group

    @classmethod
    def _assert_unit(cls, __unit: str) -> str:
        # TODO: Assert unit to be somewhat alphanumeric with certain symbols allowed
        return __unit

    # noinspection PyShadowingBuiltins
    @classmethod
    def _assert_type(cls, __type: Type[Any]) -> Type[Any]:
        # TODO: Validate type to be valid pandas dtypes
        if not isinstance(__type, type):
            raise ResourceError(f"Invalid {cls.__name__}, with type '{__type}' being not allowed.")
        return __type

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
        return ["id", "key", "name", "group", "type", "unit", *self._copy_configs().keys()]

    def _get_vars(self) -> Dict[str, Any]:
        return OrderedDict(
            id=self.id,
            key=self.key,
            name=self.name,
            group=self.group,
            type=self.type,
            unit=self.unit,
            **self._copy_configs(),
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.id})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{k}={v}" for k, v in self._get_vars().items())

    def full_name(self, unit: bool = False) -> str:
        name = self.name
        if unit and self.unit is not None and len(self.unit) > 0:
            name += f" [{self.unit}]"
        return name

    @property
    def group(self) -> str:
        return self._group

    @property
    def unit(self) -> Optional[str]:
        return self._unit

    @property
    def type(self) -> Type:
        return self._type

    # noinspection PyShadowingBuiltins
    def _update(
        self,
        id: Optional[str] = None,
        key: Optional[str] = None,
        name: Optional[str] = None,
        group: Optional[str] = None,
        type: Optional[str | Type] = None,
        unit: Optional[str] = None,
        **configs: Any,
    ) -> None:
        if id is not None and id != self.id:
            raise ConfigurationError(f"Invalid channel update, trying to change ID from '{self.id}' to '{id}'")
        if key is not None and key != self.key:
            raise ConfigurationError(f"Invalid channel update, trying to change Key from '{self.key}' to '{key}'")
        if name is not None:
            self._name = name
        if group is not None:
            self._group = self._assert_group(group)
        if type is not None:
            self._type = self._assert_type(parse_type(type))
        self._unit = self._assert_unit(unit)
        self.__update_configs(configs)

    def __update_configs(self, configs: Dict[str, Any]) -> None:
        update_recursive(self.__configs, configs)

    def _copy_configs(self) -> Dict[str, Any]:
        return OrderedDict(**self.__configs)

    def _copy_args(self) -> Dict[str, Any]:
        return {
            "id": self._id,
            **self.to_configs(),
        }

    def copy(self) -> ResourceType:
        return self.duplicate()

    def duplicate(self, **changes) -> ResourceType:
        arguments = self._copy_args()
        arguments.update(changes)
        return super().duplicate(**arguments)

    def to_list(self) -> Resources:
        return Resources([self])

    def to_configs(self) -> Dict[str, Any]:
        return {
            "key": self._key,
            "name": self._name,
            "group": self._group,
            "unit": self._unit,
            "type": self._type,
            **self._copy_configs(),
        }
