# -*- coding: utf-8 -*-
"""
lori.core.entity
~~~~~~~~~~~~~~~~


"""

from typing import Any, Optional

from lori.core import ResourceException
from lori.util import parse_name, validate_key


class Entity:
    _id: str
    _key: str
    _name: str

    # noinspection PyProtectedMember, PyShadowingBuiltins
    def __init__(
        self,
        id: Optional[str] = None,
        key: Optional[str] = None,
        name: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        key = self._assert_key(key)
        id = self._assert_id(id, key)

        self._id = id
        self._key = key
        self._name = self._assert_name(name, key)

    def __eq__(self, other: Any) -> bool:
        return self is other

    def __hash__(self) -> int:
        return hash(id(self))

    # noinspection PyShadowingBuiltins
    @classmethod
    def _assert_id(cls, __id: Optional[str], __key: Optional[str]) -> str:
        if __id is None and __key is None:
            raise ResourceException(f"Invalid {cls.__name__}, missing specified 'id'")
        if __id is None:
            __id = __key
        _id = ".".join(validate_key(i) for i in __id.split("."))
        if _id != __id:
            raise ResourceException(f"Invalid characters in '{cls.__name__}' id: " + __id)
        return _id

    @classmethod
    def _assert_key(cls, __key: str) -> str:
        if __key is None:
            raise ResourceException(f"Invalid {cls.__name__}, missing specified 'key'")
        _key = validate_key(__key)
        if _key != __key:
            raise ResourceException(f"Invalid characters in '{cls.__name__}' key: " + __key)
        return _key

    @classmethod
    def _assert_name(cls, __name: Optional[str], __key: Optional[str]) -> str:
        if __name is None and __key is None:
            raise ResourceException(f"Invalid {cls.__name__}, missing specified 'name'")
        if __name is None:
            __name = parse_name(__key)
        return __name

    @property
    def id(self) -> str:
        return self._id

    @property
    def key(self) -> str:
        return self._key

    @property
    def name(self) -> str:
        return self._name

    # noinspection PyUnresolvedReferences
    def copy(self):
        try:
            copier = super().copy
        except AttributeError:
            copier = self.duplicate
        return copier()

    # noinspection PyUnresolvedReferences, PyShadowingBuiltins
    def duplicate(
        self,
        id: Optional[str] = None,
        key: Optional[str] = None,
        name: Optional[str] = None,
        **changes,
    ):
        if key is None:
            key = self.key
        try:
            duplicator = super().duplicate
        except AttributeError:
            duplicator = type(self)
        return duplicator(
            id=id,
            key=key,
            name=name,
            **changes,
        )
