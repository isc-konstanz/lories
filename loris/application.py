# -*- coding: utf-8 -*-
"""
    loris.application
    ~~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations

import logging

from loris import Settings, System
from loris.util import to_bool
from loris.data.manager import DataManager

logger = logging.getLogger(__name__)


# noinspection PyUnresolvedReferences
def load(name: str = 'loris', **kwargs) -> Application:
    return Application.load(Settings(name, **kwargs))


class Application(DataManager):

    # noinspection PyProtectedMember
    @classmethod
    def load(cls, settings: Settings):
        app = cls(settings)

        systems_flat = to_bool(settings.get('system_flat', default=False))
        system_dirs = settings.dirs.encode()
        system_dirs['conf_dir'] = None
        if to_bool(settings.get('system_scan', default=False)):
            if to_bool(settings.get('system_copy', default=False)):
                System.copy(settings)
            system_dirs['scan_dir'] = settings.dirs.data
            for system in System.scan(app, **system_dirs, flat=systems_flat):
                app.components._add(system)
        else:
            app.components._add(System.load(app, **system_dirs, flat=systems_flat))
        app.configure()
        return app

    # noinspection PyTypeChecker
    @property
    def settings(self) -> Settings:
        return self.configs

    @property
    def name(self) -> str:
        return self.settings.application

    # noinspection PyProtectedMember
    def run(self, *args, **kwargs) -> None:
        logger.info(f"Running {type(self).__name__}: {self.name}")
        logger.info(f"Reading {len(self.filter(lambda c: c.has_reader()))} channels of application: {self.name}")
        self.read(*args, **kwargs)
        for system in self.components.get_all(System):
            try:
                logger.info(f"Running {type(system).__name__}: {system.name}")
                system.run(*args, **kwargs)

            except Exception as e:
                logger.warning(f"Error running system \"{system.id}\":", str(e))

        logger.info(f"Writing {len(self.filter(lambda c: c.has_writer()))} channels of application: {self.name}")
        self.write()
