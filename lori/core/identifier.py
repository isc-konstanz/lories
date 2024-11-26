# -*- coding: utf-8 -*-
"""
lori.core.identifier
~~~~~~~~~~~~~~~~~~~~


"""

from typing import Optional

from lori.core import ResourceException
from lori.util import parse_name, validate_key


# noinspection SpellCheckingInspection
class Identifier:
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

    # noinspection PyShadowingBuiltins
    @classmethod
    def _assert_id(cls, id: Optional[str], key: Optional[str]) -> str:
        if id is None and key is None:
            raise ResourceException(f"Invalid {cls.__name__}, missing specified 'id'")
        if id is None:
            id = key
        _id = ".".join(validate_key(i) for i in id.split("."))
        if _id != id:
            raise ResourceException(f"Invalid characters in '{cls.__name__}' id: " + key)
        return _id

    @classmethod
    def _assert_key(cls, key: str) -> str:
        if key is None:
            raise ResourceException(f"Invalid {cls.__name__}, missing specified 'key'")
        _key = validate_key(key)
        if _key != key:
            raise ResourceException(f"Invalid characters in '{cls.__name__}' key: " + key)
        return _key

    @classmethod
    def _assert_name(cls, name: Optional[str], key: Optional[str]) -> str:
        if name is None and key is None:
            raise ResourceException(f"Invalid {cls.__name__}, missing specified 'name'")
        if name is None:
            name = parse_name(key)
        return name

    @property
    def id(self) -> str:
        return self._id

    @property
    def key(self) -> str:
        return self._key

    @property
    def name(self) -> str:
        return self._name
