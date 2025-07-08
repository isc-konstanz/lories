# -*- coding: utf-8 -*-
"""
lori.core.entity
~~~~~~~~~~~~~~~~


"""

from typing import Any, Optional
from copy import deepcopy

from lori.core import ResourceException
from lori.util import parse_name, validate_key



class Entity:
    _id: str
    _key: str
    _name: str

    _deepcopy_blacklist_keys: list
    _deepcopy_blacklist_classes: list
    _access_classes: list
    #_context_cls: Context

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

        from lori import Application
        from lori.converters import Converter
        from lori.connectors import Connector
        from lori.data import DataAccess
        from lori.core import Context, RegistratorAccess

        self._deepcopy_blacklist_keys = ['_lock']
        self._deepcopy_blacklist_classes = [Application, Converter, Connector]
        self._access_classes = [DataAccess, RegistratorAccess]
        self._context_cls = Context

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

    def copy(self, new_key):
        return deepcopy(self, memo={0:new_key})

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result

        for key, value in list(self.__dict__.items()):
            if self._in_blacklist(key):
                # print(f"Skipping deepcopy for {type(self)}    {self.id}.{key} of type {type(value)}")
                new_value = value
            else:
                # print(f"Deepcopying {type(self)}    {self.id}.{key} of type {type(value)}")
                new_value = deepcopy(value, memo)
                if isinstance(new_value, self._access_classes[0]): # DataAccess
                    for context_key, context_entity in list(new_value.items()):
                        if context_entity.id != context_key:
                            pass
                            # del new_value[context_key]
                            # new_value[str(context_entity.id)] = context_entity
                if isinstance(new_value, self._access_classes[1]): # RegistratorAccess
                    for context_key, context_entity in list(new_value.items()):
                        if context_entity.id != context_key:
                            #print(f"Skipping deepcopy for:")
                            #print(new_value)
                            pass
                            # del new_value[context_key]
                            # new_value[str(context_entity.id)] = context_entity
            setattr(result, key, new_value)
        if isinstance(result, Entity) and not self._in_blacklist(None):
            ids = result.id.split(".")
            ids[0] = memo[0]
            new_id = ".".join(ids)
            if result.key == result.id:
                result._key = new_id

            result._id = new_id
        #if isinstance(self, tuple(self._deepcopy_access_keys)):
        #    pass


        return result


    def _in_blacklist(self, key: str) -> bool:
        """
        Check if the given key is in the blacklist.
        """
        if isinstance(self, tuple(self._deepcopy_blacklist_classes)):
            return True
        elif key is not None:
            if key in self._deepcopy_blacklist_keys:
                return True
            elif hasattr(self, key) and isinstance(self.__getattribute__(key), tuple(self._deepcopy_blacklist_classes)):
                return True
        return False

        # return (key in self._deepcopy_blacklist_keys
        #         or isinstance(self.__getattribute__(key), tuple(self._deepcopy_blacklist_classes))
        #         or isinstance(self, tuple(self._deepcopy_blacklist_classes)))
