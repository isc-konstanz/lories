# -*- coding: utf-8 -*-
"""
lori.application.main
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations
from collections.abc import Iterable

import sys
import traceback
from collections import OrderedDict
from typing import Any, Collection, Dict, Optional, OrderedDict, Type

import numpy as np
import pandas as pd
import re

from lori import Settings, System
from lori.application import Interface
from lori.core import ConfigurationException, Configurations, Configurator, Context, Registrator, RegistratorContext
from lori.connectors import Database
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
        def _load_static_parameters(
                configs: Configurations | dict,
                context: Optional[str] = None,
        ) -> Dict[str, Any]:
            hyper_parameters = OrderedDict[str, Any]()
            for key, value in configs.items():
                id = f"{context}.{key}" if context else key
                if isinstance(value, Configurations):
                    hyper_parameters.update(_load_static_parameters(value, id))
                else:
                    hyper_parameters[id] = value
            return hyper_parameters

        # noinspection PyShadowingBuiltins, PyShadowingNames
        def _convert_ranges(value: any) -> any:
            if isinstance(value, str):
                #TODO: use full_match instead of match?

                i_range_pattern = r"<range\(\s*(-?\d+)\s*,\s*(-?\d+)\s*,\s*(-?\d+)\s*\)>"
                i_range_match = re.match(i_range_pattern, value)
                if i_range_match:
                    start, stop, step = map(int, i_range_match.groups())
                    value = list(range(start, stop, step))
                    return  value  # Return unchanged if no match

                f_range_pattern = r"<range\(\s*([-+]?\d*\.?\d+)\s*,\s*([-+]?\d*\.?\d+)\s*,\s*([-+]?\d*\.?\d+)\s*\)>"
                f_range_match = re.match(f_range_pattern, value)
                if f_range_match:
                    start, stop, step = f_range_match.groups()
                    value = np.arange(float(start), float(stop), float(step)).tolist()
                    return value

                linspace_patters = r"<linspace\(\s*([-+]?\d*\.?\d+)\s*,\s*([-+]?\d*\.?\d+)\s*,\s*([-+]?\d+)\s*\)>"
                linspace_match = re.match(linspace_patters, value)
                if linspace_match:
                    start, stop, count = linspace_match.groups()
                    value = np.linspace(float(start), float(stop), int(count)).tolist()
                    return value

            return value

        def _convert_hyperparameters(
                component_id: str,
                hyperparameters: Dict,
                static_parameters: Dict,
                context: str) -> Dict:
            name = ", ".join(f"{k.title()}={v}" for k, v in hyperparameters.items())
            name = f"{component_id} ({name})"
            _hyperparameters = _load_static_parameters(hyperparameters, context=context)
            parameters = static_parameters.copy()
            parameters.update(_hyperparameters)
            return {name: parameters}


        # noinspection PyShadowingBuiltins, PyShadowingNames
        def _load_group_parameters(configs: Configurations) -> Dict[str, any]:
            system_id = configs.pop("system")
            registrator = configs.pop("registrator", default="components")
            key = configs.key

            mesh = configs.pop("mesh", default=False)

            hyper_names = configs.pop("hyperparameters", default=[])
            hyperparameters = {name: _convert_ranges(configs.pop(name)) for name in hyper_names}

            static_parameters = _load_static_parameters(configs, context=f"{system_id}.{registrator}.{key}")

            parameters = {}
            if len(hyperparameters) == 0:
                parameters.update({key: static_parameters.copy()})

            elif not mesh:
                if not all(isinstance(v, list) for v in hyperparameters.values()):
                    raise ConfigurationException("Hyperparameters must be lists if meshgrid is False")
                lengths = {len(v) for v in hyperparameters.values()}
                if len(lengths) > 1:
                    raise ConfigurationException("Hyperparameters must have the same length if meshgrid is False")

                for index in range(next(iter(lengths))):
                    _hyperparameters = {k: v[index] for k, v in hyperparameters.items()}
                    para = _convert_hyperparameters(key, _hyperparameters, static_parameters, context=f"{system_id}.{registrator}.{key}")
                    parameters.update(para)

            else:
                meshgrid = np.meshgrid(*[np.array(v) for v in hyperparameters.values()], indexing="ij")
                meshgrid = np.array(meshgrid, dtype=object).reshape(len(hyperparameters.keys()), -1).T

                for index in range(len(meshgrid)):
                    _hyperparameters = {k: v for k, v in zip(hyperparameters.keys(), meshgrid[index])}
                    para = _convert_hyperparameters(key, _hyperparameters, static_parameters, context=f"{system_id}.{registrator}.{key}")
                    parameters.update(para)

            return parameters

        def _mesh_group(group: list[Dict], context: list) -> list:
            if len(group) == 0:
                name = ", ".join([key for key, value in context])
                parameters = OrderedDict()
                for key, value in context:
                    parameters.update(value)

                return [{name: parameters}]

            else:
                results = []
                for g in group[0].items():
                    results.extend(_mesh_group(group[1:], [*context, g]))  # flatten
                return results

        mesh_group_names = configs.get("mesh_groups", default=[])
        groups = configs.get_section("group", defaults={})

        base_parameters = _load_static_parameters(configs.get_section("static", defaults={}))
        group_parameters = {name: _load_group_parameters(group) for name, group in groups.items()}

        for mesh_group_name in mesh_group_names:
            if mesh_group_name not in groups:
                raise ConfigurationException(f"Mesh group '{mesh_group_name}' not found in groups")

        mesh_groups = []
        non_mesh_groups = []
        for group in groups:
            if group in mesh_group_names:
                mesh_groups.append(group_parameters[group])
            else:
                non_mesh_groups.append(group_parameters[group])

        scenarios = {"Reference": OrderedDict()}
        if len(mesh_groups) >= 2:
            meshed = _mesh_group(mesh_groups, [])
            [scenarios.update({k: v}) for d in meshed for k, v in d.items()]

        [scenarios.update({k: v}) for d in non_mesh_groups for k, v in d.items()]

        hyper_systems = []
        for name, parameters in scenarios.items():
            params = base_parameters.copy()
            params.update(parameters)
            scenarios.update({name: params}) #TODO: fix

        for system in systems:
            hyper_systems.extend(
                self._load_hyper_system(
                    system,
                    scenarios,
                )
            )

        return hyper_systems

    # noinspection SpellCheckingInspection
    def _load_hyper_system(
            self,
            system: System,
            scenarios: dict[str, dict],
    ) -> Collection[System]:
        def _clear_system() -> None:
            self.converters._remove(*[c for c in self.converters if c.split(".")[0] == system.id])
            self.connectors._remove(*[c for c in self.connectors if c.split(".")[0] == system.id])
            self.components._remove(*[c for c in self.components if c.split(".")[0] == system.id])
            self._remove(*[c.id for c in self.channels if c.id.split(".")[0] == system.id])

        if len(scenarios) == 0:
            self._logger.warning(f"No hyperparameters configured for system '{system.name}'. Will be removed")
            _clear_system()
            return []

        simulation_dir = system.configs.dirs.data.joinpath(".systems")
        if not simulation_dir.exists():
            simulation_dir.mkdir(parents=True, exist_ok=True)
        
        systems = []

        for index, (name, params) in enumerate(scenarios.items()):
            replace_map = {
                " ": "_",
                "=": "_",
                "(": "",
                ")": "",
                ",": "",
                ".": "f"
            }
            key = name.lower()
            for old, new in replace_map.items():
                key = key.replace(old, new)

            system_key = f"{system.key}_{key}"
            system_path = f"{system.key}_sim_{index:03d}"
            system_name = f"{system.name} ({name})"
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

            for key, value in params.items():
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

                def handle_replace(v: any) -> Any:
                    if v == "<system.key>":
                        return system_key
                    return v

                value = handle_replace(value)

                configurations = configurator.configs
                configurations[_key[0]] = value
                configurations.write()
                if configurations.enabled:
                    configurator.update(configurations)

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

        if "comparison" in simulation:
            systems = self._load_hyper_systems(simulation["comparison"], *systems)

        def _has_no_system(registrator: Registrator) -> bool:
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
            total=len(slices)
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
