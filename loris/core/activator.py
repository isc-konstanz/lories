# -*- coding: utf-8 -*-
"""
loris.core.activator
~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from functools import wraps
from typing import Any, Dict, List, Optional

from loris.core import ConfigurationException, Configurations, ConfiguratorMeta, Context, Registrator, ResourceException
from loris.util import parse_id, parse_name


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
        context: Optional[Registrator | Context] = None,
        configs: Configurations = None,
        *args, **kwargs
    ) -> None:
        if configs is None:
            raise ConfigurationException("Missing configuration")

        if self.SECTION not in configs.sections:
            configs._add_section(self.SECTION, {"type": self.TYPE})
        section = configs.get_section(self.SECTION)
        if "type" not in section:
            section["id"] = self.TYPE
        if "name" in configs:
            section["name"] = configs.pop("name")
        if "id" not in configs and "name" in section:
            section["id"] = parse_id(section["name"])
        elif "id" in configs:
            section["name"] = parse_name(configs["id"])
        super().__init__(context, configs, *args, **kwargs)
        self._name = configs[self.SECTION].get("name")

    def __enter__(self) -> Activator:
        self._do_activate()
        return self

    # noinspection PyShadowingBuiltins
    def __exit__(self, type, value, traceback):
        self._do_deactivate()

    # noinspection PyShadowingBuiltins
    def _parse_vars(self, vars: Optional[Dict[str, Any]] = None, parse: callable = str) -> List[str]:
        if vars is None:
            vars = self._get_vars()
        values = []
        try:
            uuid = vars.pop("uuid", self.uuid)
            id = vars.pop("id", self.id)
            if uuid != id:
                values.append(f"uuid={uuid}")
            values.append(f"id={id}")
            values.append(f"name={vars.pop('name', self.name)}")
        except (ResourceException, AttributeError):
            # Abstract properties are not yet instanced
            pass
        values += [f"{k}={v if not isinstance(type(v), type) else parse(v)}"
                   for k, v in vars.items()]

        values.append(f"context={parse(self.context)}")
        values.append(f"configurations={repr(self.configs)}")
        values.append(f"configured={self.is_configured()}")
        values.append(f"active={self.is_active()}")
        values.append(f"enabled={self.is_enabled()}")
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
    def _do_activate(self) -> None:
        if not self.is_enabled():
            raise ConfigurationException(f"Trying to activate disabled {type(self).__name__}: {self.name}")
        if not self.is_configured():
            raise ConfigurationException(f"Trying to activate unconfigured {type(self).__name__}: {self.name}")
        if self.is_active():
            self._logger.warning(f"{type(self).__name__} '{self.uuid}' already active")
            return

        self.__activate()
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
