# -*- coding: utf-8 -*-
"""
lori.simulation.comparisons
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import re
from collections import OrderedDict
from collections.abc import Callable, Mapping
from functools import partial
from typing import Any, AnyStr, Collection, Dict, Iterable, Iterator, Optional, OrderedDict, Sequence

import numpy as np
from lori.core import ConfigurationException, Configurations, Configurator, Context
from lori.system import System
from lori.util import parse_name

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


# noinspection SpellCheckingInspection
class Comparisons:
    __groups: Collection[ComparisonGroup]
    __static: Collection[ComparisonGroup]

    _mesh: Collection[str]

    @classmethod
    def load(cls, configs: Configurations) -> Comparisons:
        mesh = configs.pop("mesh", default=[])
        static = [ComparisonGroup(k, **p) for k, p in configs.pop_section("static", defaults={}).items()]
        groups = [ComparisonGroup(k, **p) for k, p in configs.items()]

        return cls(mesh, groups, static)

    def __init__(
        self,
        mesh: Collection[str],
        groups: Collection[ComparisonGroup],
        static: Collection[ComparisonGroup] = (),
    ) -> None:
        for mesh_key in mesh:
            if all(mesh_key != group.key for group in groups):
                raise ConfigurationException(f"Unable to find mesh key '{mesh_key}' in specified groups")

        self._mesh = mesh
        self.__static = static
        self.__groups = groups

    # noinspection PyProtectedMember
    def from_system(self, system: System) -> Collection[Comparison]:
        comparisons = []

        static = {}
        for group in self.__static:
            if group.in_system(system):
                static.update(group._parse_parameters(system))

        mesh_keys = []
        mesh_comparisons = {}
        for group in self.__groups:
            group_comparisons = group.from_system(system, **static)
            if group.key in self._mesh:
                group_keys = []
                for group_comparison in group_comparisons:
                    group_keys.append(group_comparison.key)
                    mesh_comparisons[group_comparison.key] = group_comparison
                mesh_keys.append(group_keys)
            else:
                comparisons.extend(group_comparisons)

        for mesh_key1, mesh_key2 in _mesh_grid(mesh_keys):
            mesh_comparison = mesh_comparisons[mesh_key1].merge(mesh_comparisons[mesh_key2])
            comparisons.append(mesh_comparison)
        return comparisons


class ComparisonGroup(Mapping[AnyStr, Any]):
    def __init__(
        self,
        __key: AnyStr,
        system: Optional[AnyStr] = None,
        context: Literal["converters", "connectors", "components"] = "components",
        registrator: Optional[AnyStr] = None,
        ignore: Collection[AnyStr] = (),
        mesh: bool = False,
        **parameters: Any,
    ) -> None:
        super().__init__()
        self.__parameters = OrderedDict(**_parse_parameters(parameters))
        self._ignore = ignore
        self._mesh = mesh

        self.key = __key
        self.system = system

        if context not in ["converters", "connectors", "components"]:
            raise ConfigurationException(f"Invalid comparison group context '{context}'")
        self.context = context

        if registrator is None:
            registrator = __key
        self.registrator = registrator

    def __repr__(self) -> AnyStr:
        return f"{ComparisonGroup.__name__}({self.__parse_id()})"

    def __str__(self) -> AnyStr:
        string = f"[{self.__parse_id()}]\n"
        for k, v in self.__parameters.items():
            string += f"{k} = {v}\n"
        return string

    def __iter__(self) -> Iterator[AnyStr]:
        return iter(self.__parameters)

    def __len__(self) -> int:
        return len(self.__parameters)

    def __contains__(self, key: AnyStr) -> bool:
        return key in self.__parameters

    def __getitem__(self, key: AnyStr) -> Any:
        return self.__parameters[key]

    def __parse_id(self):
        return f"{self.system if self.system is not None else '<system_id>'}.{self.context}.{self.registrator}"

    def _parse_parameters(
        self,
        system: Optional[System] = None,
        parameters: Mapping[AnyStr, Any] = None,
    ) -> Dict[AnyStr, Any]:
        if parameters is None:
            parameters = self.__parameters
        return _parse_parameters(
            parameters,
            context=f"{self.context}.{self.registrator}",
            parser=partial(_substitute_parameter, replacements=parameters.copy(), system=system),
        )

    def from_system(self, system: System, **static: Any) -> Collection[Comparison]:
        comparisons = []
        parameters = {}
        for key, parameter in self.__parameters.items():
            if key not in self._ignore:
                parameters[key] = _substitute_ranges(parameter)

        key = parameters.pop("key", None)
        name = parameters.pop("name", None)

        def _substitute_key(_key: Optional[AnyStr], default: AnyStr, _parameters: Dict[AnyStr, Any]) -> AnyStr:
            return _substitute_parameter(_key if _key is not None else default, _parameters.copy(), system)

        if self._mesh:
            for mesh_index, mesh_parameters in enumerate(_mesh_grid(parameters.values())):
                mesh_parameters = {k: v for k, v in zip(parameters.keys(), mesh_parameters)}
                comparison_key = _substitute_key(key, f"{self.key}_{mesh_index + 1}", mesh_parameters)
                comparison_name = _substitute_key(name, parse_name(comparison_key), mesh_parameters)
                comparison_parameters = {**static}
                comparison_parameters.update(self._parse_parameters(system, mesh_parameters))
                comparison = Comparison(comparison_key, comparison_name, **comparison_parameters)
                comparisons.append(comparison)
        else:
            lengths = set(len(v) for v in parameters.values() if isinstance(v, list))
            if len(lengths) > 1:
                raise ConfigurationException(
                    "Comparison list parameters must be of the same length or 'mesh' needs to be true"
                )
            length = next(iter(lengths)) if len(lengths) > 0 else 1
            for group_index in range(length):
                group_parameters = {k: v[group_index] if isinstance(v, list) else v for k, v in parameters.items()}
                comparison_key = _substitute_key(key, f"{self.key}_{group_index + 1}", group_parameters)
                comparison_name = _substitute_key(name, parse_name(comparison_key), group_parameters)
                comparison_parameters = {**static}
                comparison_parameters.update(self._parse_parameters(system, group_parameters))
                comparison = Comparison(comparison_key, comparison_name, **comparison_parameters)
                comparisons.append(comparison)
        return comparisons

    def in_system(self, system: System) -> bool:
        return self.system is None or self.system == system.id


class Comparison(Mapping[str, Any]):
    @classmethod
    def load(cls, key: AnyStr, name: AnyStr, configs: Configurations) -> Comparison:
        return cls(key, name, **_parse_parameters(configs))

    def __init__(self, key: AnyStr, name: AnyStr, **parameters: Any) -> None:
        self.__parameters = OrderedDict(**parameters)
        self.key = key
        self.name = name

    def __repr__(self) -> AnyStr:
        return f"{Comparison.__name__}({self.key})"

    def __str__(self) -> AnyStr:
        string = f"[{self.name}]\n"
        for k, v in self.__parameters.items():
            string += f"{k} = {v}\n"
        return string

    def __iter__(self) -> Iterator[AnyStr]:
        return iter(self.__parameters)

    def __len__(self) -> int:
        return len(self.__parameters)

    def __contains__(self, key: AnyStr) -> bool:
        return key in self.__parameters

    def __getitem__(self, key: AnyStr) -> Any:
        return self.__parameters[key]

    def merge(self, parameters: Mapping[AnyStr, Any] | Comparison) -> Comparison:
        comparison = self.__parameters.copy()
        comparison.update(parameters)

        key = self.key
        name = self.name
        if isinstance(parameters, Comparison):
            key += f"_{parameters.key}"
            name += f", {parameters.name}"

        return Comparison(key, name, **comparison)

    def write(self, system: System) -> None:
        def _get_member(_object: Any, _key: AnyStr) -> Any:
            if not isinstance(_object, Context):
                return getattr(_object, _key)
            return _object.get(_key)

        for key, value in self.__parameters.items():
            _keys = key.split(".")
            _key = _keys[0]
            configurator = system
            try:
                while len(_keys) > 1:
                    del _keys[0]
                    configurator = _get_member(configurator, _key)
                    _key = _keys[0]
                del _keys[0]

            except (AttributeError, KeyError):
                # raise ConfigurationException(f"Invalid comparison parameter '{key}' for key {e}")
                pass

            if not isinstance(configurator, Configurator):
                raise ConfigurationException(
                    f"Invalid configurator type for comparison parameter '{key}': {type(configurator)}"
                )

            configurations = configurator.configs
            if len(_keys) > 0:
                section = configurations
                while len(_keys) > 0:
                    section = section.get_section(_key, ensure_exists=True)
                    _key = _keys.pop(0)
                section[_key] = value
                section.write()
            else:
                configurations[_key] = value
                configurations.write()
            if configurations.enabled:
                configurator.update(configurations)


def _mesh_grid(objects: Collection[Any]) -> Iterable[Sequence[Any]]:
    if objects is None or len(objects) == 0:
        return []
    mesh_grid = np.meshgrid(*[np.array(v) for v in objects], indexing="ij")
    return np.array(mesh_grid, dtype=object).reshape(len(objects), -1).T


# noinspection PyShadowingBuiltins
def _parse_parameters(
    mapping: Mapping[str, Any],
    context: Optional[str] = None,
    parser: Callable[[AnyStr], AnyStr] = None,
) -> Dict[str, Any]:
    parameters = OrderedDict[str, Any]()
    for key, value in mapping.items():
        id = f"{context}.{key}" if context else key
        if isinstance(value, Mapping):
            parameters.update(_parse_parameters(value, id))
        elif parser is not None:
            parameters[id] = parser(value)
        else:
            parameters[id] = value
    return parameters


def _substitute_parameter(
    parameter: Any,
    replacements: Mapping[AnyStr, Any] = None,
    system: Optional[System] = None,
) -> Any:
    substitute_pattern = r".*<[\w.]+>.*"
    if not isinstance(parameter, str) or not re.fullmatch(substitute_pattern, parameter):
        return parameter

    if replacements is None:
        replacements = {}
    if system is not None:
        replacements.update(
            system_id=str(system.id),
            system_key=str(system.key),
            system_dir=str(system.configs.dirs.data),
        )

    for search, replacement in replacements.items():
        if isinstance(replacement, str) and re.fullmatch(substitute_pattern, replacement):
            continue
        replace_pattern = f"<{search}>"
        if replace_pattern == parameter:
            parameter = replacement
        if replace_pattern in parameter:
            parameter = parameter.replace(replace_pattern, str(replacement))
        if not re.fullmatch(substitute_pattern, parameter):
            break
    if re.fullmatch(substitute_pattern, parameter):
        raise ConfigurationException(f"Unable to substitute parameter '{parameter}'")
    return parameter


# noinspection PyShadowingBuiltins, PyShadowingNames, SpellCheckingInspection
def _substitute_ranges(value: Any) -> Any:
    if isinstance(value, str):
        int_range_pattern = r"\s*<range\(\s*(-?\d+)\s*,\s*(-?\d+)\s*,\s*(-?\d+)\s*\)>\s*"
        int_range_match = re.fullmatch(int_range_pattern, value)
        if int_range_match:
            start, stop, step = map(int, int_range_match.groups())
            value = list(range(start, stop, step))
            return value  # Return unchanged if no match

        float_range_pattern = r"\s*<range\(\s*([-+]?\d*\.?\d+)\s*,\s*([-+]?\d*\.?\d+)\s*,\s*([-+]?\d*\.?\d+)\s*\)>\s*"
        float_range_match = re.fullmatch(float_range_pattern, value)
        if float_range_match:
            start, stop, step = float_range_match.groups()
            value = np.arange(float(start), float(stop), float(step)).tolist()
            return value

        linspace_patters = r"\s*<linspace\(\s*([-+]?\d*\.?\d+)\s*,\s*([-+]?\d*\.?\d+)\s*,\s*([-+]?\d+)\s*\)>\s*"
        linspace_match = re.fullmatch(linspace_patters, value)
        if linspace_match:
            start, stop, count = linspace_match.groups()
            value = np.linspace(float(start), float(stop), int(count)).tolist()
            return value

    return value
