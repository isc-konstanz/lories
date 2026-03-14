# -*- coding: utf-8 -*-
"""
lories.components.access
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import glob
import os.path
from collections.abc import Callable
from typing import Any, Collection, Mapping, Optional, Sequence, Type

from lories._core._application import _Application  # noqa
from lories._core._component import Component, _Component, _ComponentContext  # noqa
from lories.core import Configurations, RegistratorAccess, ResourceError
from lories.util import get_context, update_recursive


# noinspection PyProtectedMember, PyShadowingBuiltins
class ComponentAccess(_ComponentContext, RegistratorAccess[Component]):
    # noinspection PyUnresolvedReferences
    def __init__(self, registrar: Component, **kwargs) -> None:
        context = get_context(registrar, _Application).components
        super().__init__(context, registrar, **kwargs)

    def _set(self, id: str, component: Component) -> None:
        if not isinstance(component, _Component):
            raise ResourceError(f"Invalid component type: {type(component)}")

        super()._set(id, component)

    def activate(self, filter: Optional[Callable[[Component], bool]] = None) -> None:
        _components = self.filter(filter)
        if len(_components) > 0:
            self.context._activate(*_components)

    # noinspection PyShadowingBuiltins
    def deactivate(self, filter: Optional[Callable[[Component], bool]] = None) -> None:
        _components = self.filter(filter)
        if len(_components) > 0:
            self.context._deactivate(*_components)

    def load(
        self,
        configs: Optional[Configurations] = None,
        configs_file: Optional[str] = None,
        configs_dir: Optional[str] = None,
        configure: bool = False,
        **kwargs: Any,
    ) -> Sequence[Component]:
        return super().load(configs, configs_file, configs_dir, configure, strict=True)

    # noinspection PyShadowingBuiltins, PyUnresolvedReferences
    def load_from_type(
        self,
        cls: Type[Component],
        configs: Configurations,
        type: str,
        key: str,
        name: Optional[str] = None,
        includes: Optional[Collection[str]] = (),
        defaults: Optional[Mapping[str, Any]] = None,
        configure: bool = False,
        sort: bool = True,
        **kwargs,
    ) -> Sequence[Component]:
        kwargs["factory"] = cls
        components = []
        if defaults is None:
            defaults = self._load_registrator_defaults(strict=True)
        if any(i in configs.members for i in includes):
            configs["key"] = key
            configs["name"] = name
        configs = configs.get_member(type, defaults={**configs.get_members(includes), **defaults})
        if any(i in configs.members for i in includes):
            components.append(self._load_from_configs(self._registrar, configs, **kwargs))

        update_recursive(defaults, _Component._build_defaults(configs, includes))

        configs_dirs = configs.dirs.copy()
        configs_members = configs.get_members([s for s in configs.members if s not in defaults])

        components.extend(self._load_from_members(self._registrar, configs_members, defaults=defaults, **kwargs))

        if "alias" in configs:
            key = configs.get("alias")
        for configs_path in glob.glob(str(configs_dirs.conf.joinpath(f"{key}*.conf"))):
            if configs.name == os.path.basename(configs_path):
                continue

            configs = Configurations.load(
                configs_path,
                **configs_dirs.to_dict(),
                **defaults,
            )
            components.append(self._load_from_configs(self._registrar, configs, **kwargs))

        if sort:
            self.sort()
        if configure:
            self.configure(components)
        return components

    def _create(
        self,
        context: _ComponentContext | _Component,
        configs: Configurations,
        factory: Optional[Callable[..., Component]] = None,
        **kwargs: Any,
    ) -> Component:
        if factory is None:
            factory = super()._create
        return factory(context, configs, **kwargs)
