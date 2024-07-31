# -*- coding: utf-8 -*-
"""
loris.core.resource
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import builtins
import logging
from collections import OrderedDict
from pydoc import locate
from typing import Any, Dict, List, Optional, Type

from loris.core import ConfigurationException
from loris.util import parse_id, parse_name


class Resource:
    _uuid: str
    _id: str
    _name: str

    _type: Optional[Type]

    __configs: OrderedDict[str, Any]

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        uuid: str = None,
        id: str = None,
        name: Optional[str] = None,
        type: Optional[str | Type] = None,
        **configs: Any,
    ) -> None:
        self._logger = logging.getLogger(__name__)

        if id is None:
            raise ConfigurationException(f"Invalid configuration, missing specified {builtins.type(self).__name__} ID")
        self._id = parse_id(id)
        self._uuid = uuid if uuid is not None else self._id
        if self._id != id:
            self._logger.warning(f"{builtins.type(self).__name__} ID contains invalid characters: {id}")

        if name is None:
            name = parse_name(self.id)
        self._name = name

        if isinstance(type, str):
            type = locate(type)
        self._type = type
        self.__configs = OrderedDict(configs)

    def _get_attrs(self) -> List[str]:
        return ["uuid", "id", "name", "type", *self.__configs.keys()]

    def _get_vars(self) -> Dict[str, Any]:
        return OrderedDict(uuid=self.uuid, id=self.id, name=self.name, type=self.type, **self.__configs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.uuid})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{k}={v}" for k, v in self._get_vars().items())

    def __contains__(self, attr):
        return attr in self._get_attrs()

    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        configs = Resource.__getattribute__(self, f"_{Resource.__name__}__configs")
        if attr in configs.keys():
            return configs[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no configuration '{attr}'")

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def type(self) -> Optional[Type]:
        return self._type

    def copy(self) -> Resource:
        return type(self)(**self._get_vars())
