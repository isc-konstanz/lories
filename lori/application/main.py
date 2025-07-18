# -*- coding: utf-8 -*-
"""
lori.application.main
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import sys
import traceback
from collections import OrderedDict
from typing import Any, Collection, Dict, Optional, OrderedDict, Type

import numpy as np
import pandas as pd
from lori import Settings, System
from lori.application import Interface
from lori.connectors import Database, DatabaseException
from lori.core import ConfigurationException, Configurations, Configurator, Context, ResourceException
from lori.data.manager import DataManager
from lori.simulation import Results
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

    def _load_hyper_systems(self, configs: Configurations, *systems: System) -> Collection[System]:
        # noinspection PyShadowingBuiltins, PyShadowingNames
        def _load_hyper_parameters(configs: Configurations, context: Optional[str] = None) -> Dict[str, Any]:
            hyper_parameters = OrderedDict()
            for key, value in configs.items():
                id = f"{context}.{key}" if context else key
                if isinstance(value, Configurations):
                    hyper_parameters.update(_load_hyper_parameters(value, id))
                elif isinstance(value, list):
                    hyper_parameters[id] = value
                else:
                    raise ResourceException(f"Hyperparameter '{id}' must be a list, got {type(value)}")
            return hyper_parameters

        hyper_systems = []
        hyper_parameters = _load_hyper_parameters(configs)
        if not hyper_parameters:
            return systems

        for system in systems:
            system_parameters = {k: v for k, v in hyper_parameters.items() if k.split(".")[0] == system.id}
            hyper_systems.extend(self._load_hyper_system(system_parameters, system))
        return hyper_systems

    # noinspection SpellCheckingInspection
    def _load_hyper_system(self, parameters: Dict[str, Any], system: System) -> Collection[System]:
        def _clear_system() -> None:
            self.converters._remove(*[c for c in self.converters if c.split(".")[0] == system.id])
            self.connectors._remove(*[c for c in self.connectors if c.split(".")[0] == system.id])
            self.components._remove(*[c for c in self.components if c.split(".")[0] == system.id])
            self._remove(*[c.id for c in self.channels if c.id.split(".")[0] == system.id])

        if len(parameters) == 0:
            self._logger.warning(f"No hyperparameters configured for system '{system.id}'. Will be removed")
            _clear_system()
            return []

        simulation_dir = system.configs.dirs.data.joinpath(".systems")
        if not simulation_dir.exists():
            simulation_dir.mkdir(parents=True, exist_ok=True)

        systems = []
        meshgrid = np.meshgrid(*[np.array(v) for v in parameters.values()], indexing="ij")
        meshgrid = np.array(meshgrid, dtype=object).reshape(len(parameters.keys()), -1).T
        meshkeys = [(i, k.split(".")[-1]) for i, k in enumerate(parameters.keys())]
        for system_params in meshgrid:
            system_path = f"{system.key}_{'_'.join(f'{k}-{str(system_params[i]).lower()}' for i, k in meshkeys)}"
            system_name = f"{system.name} ({', '.join(f'{k.title()}: {str(system_params[i])}' for i, k in meshkeys)})"
            system_key = f"{system.key}_{'_'.join(str(v).lower() for v in system_params)}"
            system_dir = simulation_dir.joinpath(system_path)
            system_dirs = system.configs.dirs.copy()
            system_dirs.data = system_dir
            system_dirs.conf = system_dir.joinpath("conf")
            system_configs = system.configs.copy(system_dirs)
            system_configs["key"] = system_key
            system_configs["name"] = system_name
            system_configs.write()

            self._logger.info(f"Preparing hyperparameter system '{system_name}': {system_key}")
            system_duplicate = system.duplicate(
                key=system_key,
                name=system_name,
                configs=system_configs,
            )

            # noinspection PyUnresolvedReferences, PyArgumentList, PyShadowingNames
            def _get_member(_object: Any, _key: str) -> Any:
                if not isinstance(_object, Context):
                    return getattr(_object, _key)
                return _object.get(_key)

            for key, parameter in [(k, system_params[i]) for i, k in enumerate(parameters.keys())]:
                _key = key.split(".")[1:]
                configurator = system_duplicate
                try:
                    while len(_key) > 1:
                        configurator = _get_member(configurator, _key.pop(0))

                except (AttributeError, KeyError) as e:
                    raise ConfigurationException(f"Invalid hyperparameter '{key}' for key {e}")
                if not isinstance(configurator, Configurator):
                    raise ConfigurationException(
                        f"Invalid configurator type for hyperparameter '{key}': {type(configurator)}"
                    )
                configurations = configurator.configs
                configurations[_key[0]] = parameter
                configurations.write()
                if configurations.enabled:
                    configurator.update(configurations)

            systems.append(system_duplicate)

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

    # noinspection PyUnresolvedReferences, PyProtectedMember, PyShadowingBuiltins
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

        if "hyperparameters" in simulation:
            systems = self._load_hyper_systems(simulation["hyperparameters"], *systems)

        # TODO: Implement optional Callable argument to self.activate(), to filter registrators by system id
        # self.activate(lambda r: filter by system name)
        with self:
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
