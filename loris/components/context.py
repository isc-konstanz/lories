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
from copy import deepcopy
from typing import Any, Dict, Iterator, List, Optional

from loris import ConfigurationException, Configurations, Configurator, Context
from loris.components import Component, ComponentException, registry
from loris.data import DataAccess


class ComponentContext(Configurator, Context[Component]):
    SECTION: str = "components"

    __components: Dict[str, Component]

    def __init__(self, context: Context, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(context, configs, *args, **kwargs)
        from loris.data.context import DataContext

        if context is None or not isinstance(context, DataContext):
            raise ConfigurationException(f"Invalid data context: {None if context is None else type(context)}")
        self.__components = OrderedDict()

    def __iter__(self) -> Iterator[str]:
        return iter(self.__components)

    def __len__(self) -> int:
        return len(self.__components)

    def __contains__(self, item: str | Component) -> bool:
        if isinstance(item, str):
            return item in self.__components.keys()
        if isinstance(item, Component):
            return item in self.__components.values()
        return False

    def __repr__(self) -> str:
        return f"{type(self).__name__}({[c.uuid for c in self.__components.values()]})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(
            [f"{i} = {repr(c)}" for i, c in self.__components.items()]
        )

    # noinspection PyTypeChecker
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        def convert(text: str) -> int | str:
            return int(text) if text.isdigit() else text

        config_defaults = {}
        if configs.has_section(ComponentContext.SECTION):
            components = configs.get_section(ComponentContext.SECTION)
            config_defaults.update(components.pop(DataAccess.SECTION, {}))

            self._load_sections(components, config_defaults)

        context_dirs = [str(context.configs.dirs.conf)
                        for context in self.context.components.values() if isinstance(context, ComponentContext)]
        if configs.dirs.conf not in context_dirs:
            self._load_from_dir(configs.dirs.conf, config_defaults)
        self.__components = OrderedDict(
            sorted(self.__components.items(), key=lambda e: [convert(t) for t in re.split("([0-9]+)", e[0])])
        )

    # noinspection PyProtectedMember, PyShadowingBuiltins, PyTypeChecker, PyUnresolvedReferences
    def _load_sections(self, configs: Configurations, defaults: Optional[Mapping[str, Any]]) -> None:
        ids = [s for s in configs.sections if s != DataAccess.SECTION]
        for id in ids:
            component_file = f"{id}.conf"
            component_section = deepcopy(defaults)
            component_section.update(configs.get(id))
            component_configs = Configurations.load(
                component_file,
                **configs.dirs.encode(),
                **component_section,
                require=False
            )
            component = self._new(component_configs)
            if component.id in self:
                self._get(component.id).configs.update(component_configs)
            else:
                self._set(component.id, component)

    # noinspection PyTypeChecker, PyProtectedMember, PyUnresolvedReferences
    def _load_from_dir(self, configs_dir: str, defaults: Optional[Mapping[str, Any]]) -> None:
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
                        **defaults
                    )
                    component = self._new(component_configs)
                    if component.id in self:
                        self._get(component.id).configs.update(component_configs)
                    else:
                        self._add(component)

    def _get(self, uid: str) -> Component:
        return self.__components[uid]

    def _set(self, uid: str, component: Component) -> None:
        self.__components[uid] = component

    def _add(self, component: Component) -> None:
        self._set(component.uuid, component)

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

    def _remove(self, uid: str) -> None:
        del self.__components[uid]

    # noinspection PyMethodMayBeStatic
    def get_types(self) -> List[str]:
        return list(registry.types.keys())

    def has_type(self, *types: str | type) -> bool:
        return len(self.get_all(*types)) > 0

    def get_all(self, *types: str | type) -> List[Component]:
        return [
            component for component in self.__components.values()
            if (
                any(t.startswith(component.type) for t in types if isinstance(t, str))
                or any(isinstance(component, t) for t in types if isinstance(t, type))
            )
        ]
