# -*- coding: utf-8 -*-
"""
lori.application.main
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from typing import Optional, Type

from lori import Settings, System
from lori.application import Interface
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
        super().__init__(settings, name=settings["name"], **kwargs)
        if not settings.has_section(Interface.SECTION):
            settings._add_section(Interface.SECTION, {"enabled": False})
        self._interface = Interface(self, settings.get_section(Interface.SECTION))

    # noinspection PyProtectedMember, PyTypeChecker, PyMethodOverriding
    def configure(self, settings: Settings, factory: Type[System]) -> None:
        super().configure(settings)
        self._logger.debug(f"Setting up {type(self).__name__}: {self.name}")

        systems = []
        system_dirs = settings.dirs.to_dict()
        system_dirs["conf_dir"] = None
        systems_section = settings.get_section("systems")
        systems_flat = systems_section.get_bool("flat")
        if systems_section.get_bool("scan"):
            if systems_section.get_bool("copy"):
                factory.copy(self.settings)
            system_dirs["scan_dir"] = str(settings.dirs.data)
            systems.extend(factory.scan(self._components, **system_dirs, flat=systems_flat))
        else:
            systems.append(factory.load(self._components, **system_dirs, flat=systems_flat))

        self._components._configure(systems)
        self._components.sort()

        if not self._components.has_type(System) and settings.dirs.data.is_default():
            self._components.load(settings, configs_dir=settings.dirs.conf)

        if self._interface.is_enabled():
            self._interface.configure(settings.get_section(Interface.SECTION))

    # noinspection PyTypeChecker
    @property
    def settings(self) -> Settings:
        return self.configs

    @property
    def interface(self) -> Interface:
        return self._interface

    def main(self) -> None:
        try:
            action = self.settings["action"]
            if action == "run":
                self.run(
                    start=self.settings.get_date("start", default=None),
                    end=self.settings.get_date("end", default=None),
                )
            elif action == "start":
                self.start()

            elif action == "rotate":
                self.rotate(full=self.settings.get_bool("full"))

            elif action == "replicate":
                self.replicate(full=self.settings.get_bool("full"), force=self.settings.get_bool("force"))

        except Exception as e:
            self._logger.warning(repr(e))
            if self._logger.level == logging.DEBUG:
                self._logger.exception(e)
            exit(1)

    def activate(self) -> None:
        super().activate()
        if self._interface.is_configured():
            self._interface.activate()

    def deactivate(self, *_) -> None:
        super().deactivate()
        if self._interface.is_active():
            self._interface.deactivate()

    def start(self) -> None:
        if self._interface.is_active():
            self._executor.submit(self._interface.start)
        super().start()
