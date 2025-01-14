# -*- coding: utf-8 -*-
"""
lori.application
~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from typing import Collection, Type

from lori import Settings, System
from lori.components import ComponentContext
from lori.connectors import ConnectorContext
from lori.data.manager import DataManager


class Application(DataManager):
    TYPE: str = "app"
    SECTION: str = "application"
    INCLUDES: Collection[str] = [ComponentContext.SECTION, ConnectorContext.SECTION]

    @classmethod
    def load(cls, name: str, factory: Type[System] = System, **kwargs) -> Application:
        settings = Settings(name, **kwargs)
        app = cls(settings)
        app.configure(settings, factory)
        return app

    def __init__(self, settings: Settings, **kwargs) -> None:
        super().__init__(settings, name=settings["name"], **kwargs)

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
            systems.extend(factory.scan(self, **system_dirs, flat=systems_flat))
        else:
            systems.append(factory.load(self, **system_dirs, flat=systems_flat))
        for system in systems:
            if system.key in self._components:
                self._components.get(system.key).configs.update(system.configs)
            else:
                self._components._add(system)

        super().configure(settings)

    # noinspection PyTypeChecker
    @property
    def settings(self) -> Settings:
        return self.configs

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
