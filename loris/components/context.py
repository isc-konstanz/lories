# -*- coding: utf-8 -*-
"""
    loris._components.context
    ~~~~~~~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations
from collections.abc import Mapping
from typing import List, Iterator

import os
import re
import logging

from collections import OrderedDict
from loris import Configurations, Configurable
from loris.components import ComponentRegistration, Component, ComponentException

logger = logging.getLogger(__name__)

registrations = {}


# noinspection PyShadowingBuiltins
def register(cls: type, type: str, *alias: str, factory: callable = None, replace: bool = False) -> None:
    if type in registrations and not replace:
        raise ComponentException(f"Component \"{type}\" does already exist: {registrations[type].name}")
    registrations[type] = ComponentRegistration(cls, type, *alias, factory)


class ComponentContext(Configurable, Mapping[str, Component]):

    _components: OrderedDict[str, Component] = OrderedDict()

    def __init__(self, context, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        self._context = context
        self._load_dir(configs.dirs.cmpt)

    # noinspection PyTypeChecker, PyProtectedMember, PyUnresolvedReferences
    def _load_dir(self, configs_dir: str) -> None:
        components = {}
        if os.path.isdir(configs_dir):
            for configs_entry in os.scandir(configs_dir):
                if configs_entry.is_file() and configs_entry.path.endswith('.conf') \
                        and not configs_entry.path.endswith('default.conf') \
                        and not (configs_entry.path.endswith('results.conf') or
                                 configs_entry.path.endswith('evaluations.conf')) \
                        and configs_entry.name.startswith(tuple(self.get_types())):

                    configs_dirs = self.configs.dirs.encode()
                    configs_dirs['conf_dir'] = os.path.dirname(configs_entry.path)
                    component = self._new(Configurations.load(configs_entry.name, **configs_dirs))
                    if component.is_enabled:
                        components[component.id] = component

            def convert(text: str) -> int | str:
                return int(text) if text.isdigit() else text
            self._components = OrderedDict(sorted(components.items(),
                                                  key=lambda e: [convert(t) for t in re.split('([0-9]+)', e[0])]))

    # noinspection SpellCheckingInspection
    def _new(self, configs: Configurations) -> Component:
        if 'id' not in configs:
            configs.set('id', os.path.splitext(configs.name)[0])
            configs.move_to_top('id')

        registration_type = os.path.splitext(configs.name)[0]
        for registration in registrations.values():
            if registration.is_alias(registration_type):
                registration_type = registration.type
                logger.debug(f"Using alias \"{','.join(registration.alias.keys())}\" "
                             f"for component: {registration_type}")

        if registration_type not in registrations.keys():
            raise ComponentException(f"Invalid component type: {registration_type}")

        return registrations[registration_type].initialize(self, configs)

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
            try:
                component.activate()

            except Exception as e:
                logger.warning(f"Error activating component \"{component_id}\": {e}")
                logger.exception(e)
                del self._components[component_id]

        self.__activate__()

    def __activate__(self) -> None:
        pass

    def deactivate(self) -> None:
        for component_id, component in self._components.items():
            try:
                component.deactivate()

            except Exception as e:
                logger.warning(f"Error deactivating component \"{component_id}\": {e}")
                logger.exception(e)

        self.__deactivate__()

    def __deactivate__(self) -> None:
        pass

    # noinspection PyMethodMayBeStatic
    def get_types(self) -> List[str]:
        return list(registrations.keys())

    def has_type(self, *types: str | type) -> bool:
        return len(self.get_all(*types)) > 0

    def get_all(self, *types: str | type) -> List[Component] | Component:
        return [component for component in self._components.values()
                if (any(t.startswith(component.get_type()) for t in types if isinstance(t, str)) or
                    any(isinstance(component, t) for t in types if isinstance(t, type)))]
