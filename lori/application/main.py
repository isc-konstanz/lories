# -*- coding: utf-8 -*-
"""
lori.application.main
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import sys
import traceback
from typing import Collection, Optional, Type

import pandas as pd
from lori import Settings, System
from lori.application import Interface
from lori.connectors import Database
from lori.core import Configurations, Registrator, RegistratorContext
from lori.data.manager import DataManager
from lori.simulation import Results
from lori.simulation.comparisons import Comparison, Comparisons
from lori.typing import TimestampType
from lori.util import slice_range, to_timedelta


class Application(DataManager):
    _interface: Optional[Interface] = None

    @classmethod
    def load(cls, name: str, factory: Type[System] = System, **kwargs) -> Application:
        settings = Settings(name, **kwargs)
        app = cls(settings)
        app.configure(settings, factory)
        return app

    def load_comparisons(self, configs: Configurations, *systems: System) -> Collection[System]:
        comparisons = Comparisons.load(configs)
        comparison_systems = []
        for system in systems:
            comparison_systems.extend(self._load_comparison_system(system, comparisons.from_system(system)))
        return comparison_systems

    # noinspection SpellCheckingInspection
    def _load_comparison_system(
        self,
        system: System,
        comparisons: Collection[Comparison],
    ) -> Collection[System]:
        def _clear_system() -> None:
            self.converters._remove(*[c for c in self.converters if c.split(".")[0] == system.id])
            self.connectors._remove(*[c for c in self.connectors if c.split(".")[0] == system.id])
            self.components._remove(*[c for c in self.components if c.split(".")[0] == system.id])
            self._remove(*[c.id for c in self.channels if c.id.split(".")[0] == system.id])

        if len(comparisons) == 0:
            self._logger.warning(f"No hyperparameters configured for system '{system.name}'. Will be removed")
            _clear_system()
            return []

        simulation_dir = system.configs.dirs.data.joinpath(".systems")
        if not simulation_dir.exists():
            simulation_dir.mkdir(parents=True, exist_ok=True)

        systems = []
        for comparison in comparisons:
            system_key = f"{system.key}_{comparison.key}"
            system_name = f"{system.name} ({comparison.name})"
            system_dir = simulation_dir.joinpath(system_key)
            system_dirs = system.configs.dirs.copy()
            system_dirs.data = system_dir
            system_dirs.conf = system_dir.joinpath("conf")
            system_configs = system.configs.copy(system_dirs)
            system_configs["key"] = system_key
            system_configs["name"] = system_name
            system_configs.write()

            self._logger.info(f"Preparing comparison system '{system_name}': {system_key}")
            system_duplicate = system.duplicate(
                key=system_key,
                name=system_name,
                configs=system_configs,
            )
            comparison.write(system_duplicate)
            systems.append(system_duplicate)
            self._components._add(system_duplicate)
        self._components.sort()

        _clear_system()
        return systems

    # noinspection PyProtectedMember
    def __init__(self, settings: Settings, **kwargs) -> None:
        super().__init__(settings, name=settings["name"], **kwargs)
        if not settings.has_section(Interface.SECTION):
            settings._add_section(Interface.SECTION, {"enabled": False})
        self._interface = Interface(self, settings.get_section(Interface.SECTION))

    # noinspection PyProtectedMember, PyTypeChecker, PyMethodOverriding
    def configure(self, settings: Settings, factory: Type[System], **_) -> None:
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
        has_interface = self._interface.is_enabled()
        if has_interface:
            wait = False
        super().start(wait)

        if has_interface:
            self._interface.start()

    # noinspection PyUnresolvedReferences, PyProtectedMember
    def simulate(
        self,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
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

        database_id = simulation.get("database", default="results")
        database_section = simulation.get_section("databases", defaults={})
        if len(database_section) > 0:
            self._connectors.load(database_section)
        database = self._connectors.get(database_id) if database_id in self._connectors else None

        error = False
        summary = []
        systems = self._components.get_all(System)

        if "comparisons" in simulation:
            systems = self.load_comparisons(simulation["comparisons"], *systems)

        def _has_no_system(registrator: Registrator) -> bool:
            if any(registrator.id == s.id for s in systems):
                return False
            return isinstance(registrator.context, RegistratorContext)

        try:
            self.activate(_has_no_system)

            for system in systems:

                def _is_system(registrator) -> bool:
                    return registrator.id.split(".")[0] == system.id

                try:
                    self._connect(*self._connectors.filter(_is_system))
                    self._activate(*self._components.filter(_is_system))

                    results = self._simulate(system, simulation, start, end, database, **kwargs)
                    if results.is_success():
                        summary.append(results.to_frame())
                    else:
                        error = True
                finally:
                    self._deactivate(*self._components.filter(_is_system))
                    self._disconnect(*self._connectors.filter(_is_system))
        finally:
            self.deactivate(filter=_has_no_system)

        if not error and len(summary) > 1:
            try:
                from lori.io import excel

                excel_file = str(self.configs.dirs.data.joinpath("summary.xlsx"))
                excel.write(excel_file, "Summary", pd.concat(summary, axis="index"))

            except ImportError:
                pass

    # noinspection PyUnresolvedReferences, PyProtectedMember, PyShadowingBuiltins
    def _simulate(
        self,
        system: System,
        configs: Configurations,
        start: TimestampType,
        end: TimestampType,
        database: Optional[Database] = None,
        **kwargs,
    ) -> Results:
        slice = configs.get_bool("slice", default=True)
        freq = configs.get("freq", default=None)

        if slice and freq is not None and start + to_timedelta(freq) < end:
            slices = slice_range(start, end, timezone=system.location.timezone, freq=freq)
        else:
            slices = [(start, end)]

        with Results(
            system,
            database,
            configs.get_section("data"),
            total=len(slices),
        ) as results:
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
            except Exception as e:
                self._logger.error(f"Error simulating system {system.name}: {str(e)}")
                self._logger.debug("%s: %s", type(e).__name__, traceback.format_exc())
                results.durations.complete()
                results.progress.complete(
                    status="error",
                    message=str(e),
                    error=type(e).__name__,
                    trace=traceback.format_exc(),
                )
        return results
