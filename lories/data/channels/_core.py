# -*- coding: utf-8 -*-
"""
lories.data.channels.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import ABC
from collections import OrderedDict
from typing import Any, Dict, Generic, List, Optional, Sequence, Type, TypeVar, get_args, get_origin

from lories._core._channel import Channel  # noqa
from lories._core._connector import Connector, _Connector  # noqa
from lories._core._database import _Database  # noqa
from lories._core._registrator import Registrator, _Registrator, _RegistratorContext  # noqa
from lories._core._tasks import TaskContext, _TaskContext  # noqa
from lories.core.errors import ResourceError, ResourceUnavailableError
from lories.util import to_bool, update_recursive

# FIXME: Remove this once Python >= 3.12 is a requirement
try:
    from typing import get_bound

except ImportError:

    def get_bound(cls):
        return getattr(cls, "__bound__", None)


# noinspection PyProtectedMember
class _ChannelWrapper(ABC, Generic[Registrator]):
    __registrator_class: Type[Registrator]
    __registrator: Optional[Registrator]
    __configs: OrderedDict[str, Any]

    enabled: bool = False

    # noinspection PyProtectedMember, PyUnresolvedReferences, PyTypeChecker
    @classmethod
    def build(cls, channel: Channel, **configs) -> ChannelWrapper:
        registrator_class = cls._build_registrator_class()
        registrator_id = configs.pop(registrator_class.TYPE, None)
        if registrator_id is None:
            registrator = None
        else:
            try:
                registrator = cls._build_registrator(
                    channel._context.__getattribute__(f"{registrator_class.TYPE}s"),
                    channel.path,
                    registrator_id,
                )
            except AttributeError:
                raise ResourceUnavailableError(f"Missing {registrator_class.TYPE} context for channel '{channel.id}'")

        return cls(registrator_class, registrator, **configs)

    # noinspection PyShadowingBuiltins
    @classmethod
    def _build_registrator(cls, context: _RegistratorContext, path: Sequence[str], id: str) -> Optional[Registrator]:
        if "." not in id:
            for i in reversed(range(1, len(path))):
                _id = ".".join([*path[:i], id])
                if _id in context.keys():
                    id = _id
                    break
        if id is None:
            return None
        return context.get(id, None)

    @classmethod
    def _build_registrator_class(cls) -> Type[Registrator]:
        for _cls in cls.mro():
            for _base in getattr(_cls, "__orig_bases__", ()):
                if isinstance(_base, type):
                    continue
                if issubclass(get_origin(_base), _ChannelWrapper):
                    (_arg,) = get_args(_base)
                    if not isinstance(_arg, type):
                        _arg = get_bound(_arg)
                    return _arg
        raise ResourceError("Unable to extract context registrator")

    def _get_registrator(self) -> Optional[Registrator]:
        return self.__registrator

    def __init__(self, __class: Type[Registrator], registrator: Optional[Registrator], **configs) -> None:
        registrator = self._assert_registrator(registrator)
        self.enabled = to_bool(configs.pop("enabled", registrator is not None and registrator.is_enabled()))
        self.__registrator_class = __class
        self.__registrator = registrator
        self.__configs = OrderedDict(configs)

    @classmethod
    def _assert_registrator(cls, registrator: Registrator) -> Registrator:
        if not isinstance(registrator, _Registrator):
            raise ResourceError(
                f"Invalid '{cls.__name__}' registrator: " f"{None if registrator is None else type(registrator)}"
            )
        return registrator

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, _ChannelWrapper) and self._get_vars() == other._get_vars()

    def __hash__(self) -> int:
        return hash(*self._get_vars().values())

    def __contains__(self, attr: str) -> bool:
        return attr in self._get_attrs()

    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        configs = _ChannelWrapper.__getattribute__(self, f"_{_ChannelWrapper.__name__}__configs")
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

    @property
    def id(self) -> Optional[str]:
        return self.__registrator.id if self.__registrator is not None else None

    @property
    def key(self) -> Optional[str]:
        return self.__registrator.key if self.__registrator is not None else None

    def get(self, attr: str, default: Optional[Any] = None) -> Any:
        return self._get_vars().get(attr, default)

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        return {
            self.__registrator_class.TYPE: self.key,
            **self.__configs,
            "enabled": self.enabled,
        }

    def _get_attrs(self) -> List[str]:
        return [self.__registrator_class.TYPE, *self._copy_configs().keys(), "enabled"]

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
        registrator = configs.pop(self.__registrator.TYPE, None)
        if registrator is not None:
            if not (isinstance(registrator, str) and self.__registrator.key == registrator.split(".")[-1]):
                raise ResourceError(f"Unable to update '{type(self).__name__}' registrator '{registrator}'")

        if enabled is not None:
            self.enabled = to_bool(enabled)
        self.__update_configs(configs)

    def to_configs(self) -> Dict[str, Any]:
        return {
            self.__registrator_class.TYPE: self.key,
            **self.__configs,
            "enabled": self.enabled,
        }

    def is_configured(self) -> bool:
        return self.__registrator.is_configured() if self.enabled else False

    def copy(self) -> ChannelWrapper:
        return type(self)(**self._get_vars())

    def duplicate(self, channel: Channel, **changes) -> ChannelWrapper:
        arguments = self._get_vars()
        arguments.update(changes)

        return type(self).build(channel, **arguments)


ChannelWrapper = TypeVar("ChannelWrapper", bound=_ChannelWrapper)
