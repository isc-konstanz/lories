# -*- coding: utf-8 -*-
"""
loris.core.activator
~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections import OrderedDict
from functools import wraps
from typing import Dict, Optional

from loris.core import Context, Registrator, Resource, ResourceException, Resources
from loris.core.configs import ConfigurationException, Configurations, Configurator, ConfiguratorMeta
from loris.location import Location
from loris.util import parse_name


class ActivatorMeta(ConfiguratorMeta):
    # noinspection PyProtectedMember
    def __call__(cls, *args, **kwargs):
        activator = super().__call__(*args, **kwargs)

        activator._Activator__activate = activator.activate
        activator.activate = activator._do_activate

        activator._Activator__deactivate = activator.deactivate
        activator.deactivate = activator._do_deactivate

        return activator


# noinspection PyAbstractClass
class Activator(Registrator, metaclass=ActivatorMeta):
    _name: str

    _active: bool = False

    # noinspection PyProtectedMember
    def __init__(
        self,
        context: Optional[Context | Registrator] = None,
        configs: Optional[Configurations] = None,
        key: Optional[str] = None,
        name: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(context, configs, key=key, *args, **kwargs)
        self._name = self._assert_name(context, configs, name)

    def __enter__(self) -> Activator:
        self.activate()
        return self

    # noinspection PyShadowingBuiltins
    def __exit__(self, type, value, traceback):
        self.deactivate()

    # noinspection PyUnusedLocal
    def _assert_key(self, context: Optional[Context], configs: Optional[Configurations], key: Optional[str]) -> str:
        if configs is not None and key is None:
            if configs.has_section(self.SECTION) and "name" in configs[self.SECTION]:
                key = configs[self.SECTION]["name"]
            elif "name" in configs:
                key = configs["name"]
        return super()._assert_key(context, configs, key)

    # noinspection PyUnusedLocal
    def _assert_name(self, context: Optional[Context], configs: Optional[Configurations], name: Optional[str]) -> str:
        if configs is not None:
            if configs.has_section(self.SECTION) and "name" in configs[self.SECTION]:
                name = configs[self.SECTION]["name"]
            elif "name" in configs:
                name = configs["name"]
        if name is None:
            name = parse_name(self._key)
        return name

    # noinspection PyShadowingBuiltins
    def _convert_vars(self, convert: callable = str) -> Dict[str, str]:
        vars = self._get_vars()
        values = OrderedDict()
        try:
            id = vars.pop("key", self.id)
            key = vars.pop("key", self.key)
            if id != key:
                values["key"] = id
            values["key"] = key
            values["name"] = vars.pop("name", self.name)
        except (ResourceException, AttributeError):
            # Abstract properties are not yet instanced
            pass

        values.update(
            {
                k: str(v) if not isinstance(v, (Resource, Resources, Configurator, Context, Location)) else convert(v)
                for k, v in vars.items()
            }
        )
        values["context"] = convert(self.context)
        values["configurations"] = convert(self.configs)
        values["configured"] = str(self.is_configured())
        values["active"] = str(self.is_active())
        values["enabled"] = str(self.is_enabled())
        return values

    @property
    def name(self) -> str:
        return self._name

    def is_active(self) -> bool:
        return self._active

    def activate(self) -> None:
        pass

    # noinspection PyUnresolvedReferences
    @wraps(activate, updated=())
    def _do_activate(self, *args, **kwargs) -> None:
        if not self.is_enabled():
            raise ConfigurationException(f"Trying to activate disabled {type(self).__name__}: {self.name}")
        if not self.is_configured():
            raise ConfigurationException(f"Trying to activate unconfigured {type(self).__name__}: {self.name}")
        if self.is_active():
            self._logger.warning(f"{type(self).__name__} '{self.id}' already active")
            return

        self.__activate(*args, **kwargs)
        self._on_activate()
        self._active = True

    def _on_activate(self) -> None:
        pass

    def deactivate(self) -> None:
        pass

    # noinspection PyUnresolvedReferences
    @wraps(deactivate, updated=())
    def _do_deactivate(self) -> None:
        if not self.is_active():
            return

        self.__deactivate()
        self._on_deactivate()
        self._active = False

    def _on_deactivate(self) -> None:
        pass
