# -*- coding: utf-8 -*-
"""
lori.application.main
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import sys
import traceback
from typing import Optional, Type

import pandas as pd
from lori import Settings, System
from lori.application import Interface
from lori.connectors import Database, DatabaseException
from lori.data.manager import DataManager
from lori.simulation import Results
from lori.typing import Timestamp
from lori.util import slice_range, to_timedelta


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
        if not settings.has_section(Interface.TYPE):
            settings._add_section(Interface.TYPE, {"enabled": False})

        # Check if the tasked action may be headless
        if settings.get("action").lower() == "start":
            self._interface = Interface(self, settings.get_section(Interface.TYPE))

    # noinspection PyProtectedMember, PyTypeChecker, PyMethodOverriding
    def configure(self, settings: Settings, factory: Type[System] = System) -> None:
        super().configure(settings)
        self._logger.debug(f"Setting up {type(self).__name__}: {self.name}")
        components = []

        system_dirs = settings.dirs.to_dict()
        system_dirs["conf_dir"] = None
        systems_section = settings.get_section("systems")
        systems_flat = systems_section.get_bool("flat")
        if systems_section.get_bool("scan"):
            if systems_section.get_bool("copy"):
                factory.copy(self.settings)
            system_dirs["scan_dir"] = str(settings.dirs.data)
            components.extend(factory.scan(self._components, **system_dirs, flat=systems_flat))
        else:
            components.append(factory.load(self._components, **system_dirs, flat=systems_flat))

        if not self._components.has_type(System) and settings.dirs.data.is_default():
            components += self._components.load(configs_dir=settings.dirs.conf, configure=False, sort=False)

        self._components.configure(components)

        if self._has_interface():
            self._interface.configure(settings.get_section(Interface.TYPE))

    # noinspection PyTypeChecker
    @property
    def settings(self) -> Settings:
        return self.configs

    @property
    def interface(self) -> Interface:
        return self._interface

    def _has_interface(self) -> bool:
        return self._interface is not None and self._interface.is_enabled()

    def main(self) -> None:
        action = self.settings["action"]
        try:
            if action == "run":
                with self:
                    self.run(
                        start=self.settings.get_date("start", default=None),
                        end=self.settings.get_date("end", default=None),
                    )
            elif action == "start":
                with self:
                    self.start()

            elif action == "simulate":
                with self:
                    self.simulate(
                        start=self.settings.get_date("start", default=None),
                        end=self.settings.get_date("end", default=None),
                    )

            elif action == "rotate":
                self.rotate(full=self.settings.get_bool("full"))

            elif action == "replicate":
                self.replicate(full=self.settings.get_bool("full"), force=self.settings.get_bool("force"))

        except Exception as e:
            self._logger.warning(f"Error during '{action}': {str(e)}")
            self._logger.exception(e)
            exit(1)

    def start(self, wait: bool = True) -> None:
        _has_interface = self._has_interface()
        if _has_interface:
            wait = False
        super().start(wait)

        if _has_interface:
            self._interface.start()

    # noinspection PyUnresolvedReferences, PyProtectedMember, PyShadowingBuiltins
    def simulate(
        self,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        **kwargs,
    ) -> None:
        simulation = self.settings.get_section("simulation", defaults={"data": {"include": True}})

        timezone = simulation.get("timezone", None)
        if start is None:
            start = simulation.get_date("start", default=None, timezone=timezone)
        if end is None:
            end = simulation.get_date("end", default=None, timezone=timezone)

        if start is None or end is None or end < start:
            self._logger.error("Invalid settings missing specified simulation period")
            sys.exit(1)

        slice = simulation.get_bool("slice", default=True)
        freq = simulation.get("freq", default=None)

        database_id = simulation.get("database", default="results")
        database_section = simulation.get_section("databases", defaults={})
        if database_id == "results" and "results" not in database_section:
            database_section["results"] = {
                "type": "tables",
                "file": ".results.h5",
                "compression_level": 9,
                "compression_lib": "zlib",
            }
        self.connectors.load(database_section)

        database = self.connectors.get(database_id)
        if not isinstance(database, Database):
            raise DatabaseException(database, f"Invalid results cache type '{type(database)}'")

        error = False
        summary = []
        systems = self.components.get_all(System)
        for system in systems:
            self._logger.info(f"Starting simulation of system '{system.name}': {system.id}")
            if slice and freq is not None and start + to_timedelta(freq) < end:
                slices = slice_range(start, end, timezone=system.location.timezone, freq=freq)
            else:
                slices = [(start, end)]

            with Results(system, database, simulation.get_section("data"), total=len(slices)) as results:
                results.durations.start("Simulation")
                try:
                    for slice_start, slice_end in slices:
                        slice_prior = results.data.tail(1) if not results.data.empty else None
                        results.submit(
                            system.simulate,
                            slice_start,
                            slice_end,
                            slice_prior,
                            **kwargs,
                        )
                        results.progress.update()

                    results.durations.stop("Simulation")
                    self._logger.debug(
                        f"Finished simulation of system '{system.name}' in {results.durations['Simulation']} minutes"
                    )

                    self._logger.debug(f"Starting evaluation of system '{system.name}': {system.id}")
                    results.durations.start("Evaluation")
                    results_data = system.evaluate(results)
                    results.report()

                    # TODO: Call evaluations from configs

                    results.durations.stop("Evaluation")
                    self._logger.debug(
                        f"Finished evaluation of system '{system.name}' in {results.durations['Evaluation']} minutes"
                    )

                    summary.append(results.to_frame())

                except Exception as e:
                    error = True
                    self._logger.error(f"Error simulating system {system.name}: {str(e)}")
                    self._logger.debug("%s: %s", type(e).__name__, traceback.format_exc())
                    results.durations.complete()
                    results.progress.complete(
                        status="error",
                        message=str(e),
                        error=type(e).__name__,
                        trace=traceback.format_exc(),
                    )

        if not error and len(summary) > 1:
            try:
                from lori.io import excel

                excel_file = str(self.configs.dirs.data.joinpath("summary.xlsx"))
                excel.write(excel_file, "Summary", pd.concat(summary, axis="index"))

            except ImportError:
                pass
