# -*- coding: utf-8 -*-
"""
lori.application.main
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from typing import Optional, Type

from lori import Settings, System
from lori.application import Interface, InterfaceUnavailableException
from lori.data.manager import DataManager


class Application(DataManager):
    _interface: Optional[Interface] = None

    @classmethod
    def load(cls, name: str, factory: Type[System] = System, **kwargs) -> Application:
        settings = Settings(name, **kwargs)
        app = cls(settings)
        app.configure(settings, factory)
        return app

    # noinspection PyProtectedMember
    def __init__(self, settings: Settings, **kwargs) -> None:
        super().__init__(settings, **kwargs)
        if not settings.has_section(Interface.SECTION):
            settings._add_section(Interface.SECTION, {"enabled": False})
        self._interface = Interface(self, settings.get_section(Interface.SECTION))

    # noinspection PyProtectedMember
    def configure(self, settings: Settings, factory: Type[System]) -> None:
        self._logger.debug(f"Setting up {type(self).__name__}: {self.name}")

        systems = []
        systems_flat = self.settings["systems"]["flat"]
        system_dirs = self.settings.dirs.to_dict()
        system_dirs["conf_dir"] = None
        if self.settings["systems"]["scan"]:
            if self.settings["systems"]["copy"]:
                factory.copy(self.settings)
            system_dirs["scan_dir"] = str(self.settings.dirs.data)
            for system in factory.scan(self, **system_dirs, flat=systems_flat):
                systems.append(system)
        else:
            systems.append(factory.load(self, **system_dirs, flat=systems_flat))
        for system in systems:
            if system.key in self._components:
                self._components.get(system.key).configs.update(system.configs)
            else:
                self._components._add(system)

        super().configure(settings)
        if self._interface.is_enabled():
            self._interface.configure(settings.get_section(Interface.SECTION))

    # noinspection PyTypeChecker
    @property
    def settings(self) -> Settings:
        return self.configs

    @property
    def interface(self) -> Interface:
        if self._interface is None:
            raise InterfaceUnavailableException(f"Application '{self.name}' has no interface configured")
        return self._interface

    def main(self) -> None:
        try:
            action = self.settings["action"]
            if action == "run":
                self.run()

            elif action == "start":
                self.start()

        except Exception as e:
            self._logger.warning(repr(e))
            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.exception(e)
            exit(1)

    def start(self) -> None:
        self._logger.info(f"Starting {type(self).__name__}: {self.name}")
        self.__interrupt.clear()
        self.__runner.start()
        if self._interface is not None and self._interface.is_enabled():
            self._interface.start()
        else:
            self.__runner.join()
