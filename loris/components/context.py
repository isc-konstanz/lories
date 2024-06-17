# -*- coding: utf-8 -*-
"""
loris.components.context
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import itertools
import os
import re
from collections import OrderedDict
from collections.abc import Mapping
from copy import deepcopy
from typing import Any, Iterator, List, Optional

from loris import Configurable, Configurations
from loris.components import Component, ComponentException, registry


class ComponentContext(Configurable, Mapping[str, Component]):
    _components: OrderedDict[str, Component] = OrderedDict()

    def __init__(self, context, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        self._context = context
        self._load(configs)

    # noinspection PyTypeChecker, PyProtectedMember, PyUnresolvedReferences
    def _load(self, configs) -> None:
        components = {}
        component_defaults = {}
        if "components" in configs:
            components_section = configs.get_section("components")
            component_ids = [
                i for i in components_section.keys() if (isinstance(components_section[i], Mapping) and i != "data")
            ]
            component_defaults.update(components_section.pop("data", {}))

            for component_id in component_ids:
                component_file = f"{component_id}.conf"
                component_section = deepcopy(component_defaults)
                component_section.update(components_section.get(component_id))
                component_configs = Configurations.load(
                    component_file,
                    **configs.dirs.encode(),
                    **component_section,
                    require=False
                )
                component = self._new(component_configs)
                if component.is_enabled():
                    components[component.id] = component
        components.update(self._load_dir(configs.dirs.cmpt, component_defaults))

        def convert(text: str) -> int | str:
            return int(text) if text.isdigit() else text

        self._components = OrderedDict(
            sorted(components.items(), key=lambda e: [convert(t) for t in re.split("([0-9]+)", e[0])])
        )

    # noinspection PyTypeChecker, PyProtectedMember, PyUnresolvedReferences
    def _load_dir(self, configs_dir: str, config_defaults: Optional[Mapping[str, Any]]) -> Mapping[str, Component]:
        components = {}
        if os.path.isdir(configs_dir):
            config_types = tuple(itertools.chain(*[[t.type, *t.alias] for t in registry.types.values()]))
            for configs_entry in os.scandir(configs_dir):
                if (configs_entry.is_file() and configs_entry.path.endswith('.conf')
                        and not configs_entry.path.endswith('default.conf')
                        and not (configs_entry.path.endswith('results.conf')
                                 or configs_entry.path.endswith('evaluations.conf'))
                        and configs_entry.name.startswith(config_types)):

                    configs_dirs = self.configs.dirs.encode()
                    configs_dirs["conf_dir"] = os.path.dirname(configs_entry.path)
                    component_configs = Configurations.load(
                        configs_entry.name,
                        **configs_dirs,
                        **config_defaults
                    )
                    component = self._new(component_configs)
                    if component.is_enabled():
                        components[component.id] = component

        return components

    # noinspection SpellCheckingInspection
    def _new(self, configs: Configurations) -> Component:
        registration_name = os.path.splitext(configs.name)[0]
        if "id" not in configs:
            configs.set("id", registration_name)
            configs.move_to_top("id")

        registration_type = re.split(r"[^a-zA-Z0-9\s]", registration_name)[0]
        for registration in registry.types.values():
            if registration.is_alias(registration_type):
                registration_type = registration.type
                self._logger.debug(
                    f"Using alias \"{','.join(registration.alias.keys())}\" " f"for component: {registration_type}"
                )

        if registration_type not in registry.types.keys():
            raise ComponentException(f"Invalid component type: {registration_type}")

        return registry.types[registration_type].initialize(self, configs)

    def _add(self, component: Component) -> None:
        self._components[component.id] = component

    def _remove(self, component_id: str) -> None:
        del self._components[component_id]

    def __getitem__(self, component_id: str) -> Component:
        return self._components[component_id]

    def __iter__(self) -> Iterator[str]:
        return iter(self._components)

    def __len__(self) -> int:
        return len(self._components)

    def configure(self) -> None:
        super().configure()
        for component in self._components.values():
            component.configure()

    def activate(self) -> None:
        for component_id, component in self._components.items():
            component.activate()

        self.__activate__()

    def __activate__(self) -> None:
        pass

    def deactivate(self) -> None:
        for component_id, component in self._components.items():
            try:
                component.deactivate()

            except Exception as e:
                self._logger.warning(f"Error deactivating component '{component_id}': {e}")
                self._logger.exception(e)

        self.__deactivate__()

    def __deactivate__(self) -> None:
        pass

    # noinspection PyMethodMayBeStatic
    def get_types(self) -> List[str]:
        return list(registry.types.keys())

    def has_type(self, *types: str | type) -> bool:
        return len(self.get_all(*types)) > 0

    def get_all(self, *types: str | type) -> List[Component] | Component:
        return [
            component for component in self._components.values()
            if (
                any(t.startswith(component.get_type()) for t in types if isinstance(t, str))
                or any(isinstance(component, t) for t in types if isinstance(t, type))
            )
        ]
