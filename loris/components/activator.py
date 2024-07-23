# -*- coding: utf-8 -*-
"""
loris.components.activator
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from abc import abstractmethod
from functools import wraps
from typing import Collection

from loris.configs import ConfigurationException, Configurations, Configurator, ConfiguratorMeta
from loris.util import get_variables


class ActivatorMeta(ConfiguratorMeta):
    # noinspection PyProtectedMember
    def __call__(cls, *args, **kwargs):
        activator = super().__call__(*args, **kwargs)

        activator._Activator__activate = activator.activate
        activator.activate = activator._do_activate

        activator._Activator__deactivate = activator.deactivate
        activator.deactivate = activator._do_deactivate

        return activator


class Activator(Configurator, metaclass=ActivatorMeta):
    _active: bool = False

    def __init__(self, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        self.__logger = logging.getLogger(Activator.__module__)

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

        uuid = vars.pop("uuid", self.uuid)
        id = vars.pop("id", self.id)
        if uuid != id:
            values.append(f"uuid={uuid}")
        values.append(f"id={id}")
        values.append(f"name={vars.pop('name', self.name)}")

        values += [f"{k}={v if not isinstance(v, (Channel, Configurator, Location)) else parse(v)}"
                   for k, v in vars.items()]

        values.append(f"context={parse(self.context)}")
        values.append(f"configurations={repr(self.configs)}")
        values.append(f"configured={self.is_configured()}")
        values.append(f"active={self.is_active()}")
        values.append(f"enabled={self.is_enabled()}")
        return values

    def is_active(self) -> bool:
        return self._active

    @abstractmethod
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
            self.__logger.warning(f"{type(self).__name__} '{self.name}' already active")
            return
        self.__logger.info(f"Activating {type(self).__name__}: {self.name}")

        self.__activate()
        self._do_activate_members(list(get_variables(self, Activator).values()))
        self._on_activate()
        self._active = True

        self.__logger.debug(f"Activated {type(self).__name__}: {self.name}")

    # noinspection PyProtectedMember
    def _do_activate_members(self, activators: Collection[Activator]) -> None:
        for activator in activators:
            if not activator.is_enabled():
                self.__logger.debug(f"Skipping activating disabled {type(activator).__name__}: {activator.name}")
            activator._do_activate()

    def _on_activate(self) -> None:
        pass

    @abstractmethod
    def deactivate(self) -> None:
        pass

    # noinspection PyUnresolvedReferences
    @wraps(deactivate, updated=())
    def _do_deactivate(self) -> None:
        if not self.is_active():
            return
        self.__logger.info(f"Deactivating {type(self).__name__}: {self.name}")

        self.__deactivate()
        self._do_deactivate_members(list(get_variables(self, Activator).values()))
        self._on_deactivate()
        self._active = False

        self.__logger.debug(f"Deactivated {type(self).__name__}: {self.name}")

    # noinspection PyProtectedMember
    def _do_deactivate_members(self, activators: Collection[Activator]) -> None:
        for activator in activators:
            try:
                activator._do_deactivate()

            except Exception as e:
                self.__logger.warning(f"Error deactivating {type(self).__name__} '{activator.name}': {e}")
                self.__logger.exception(e)

    def _on_deactivate(self) -> None:
        pass
