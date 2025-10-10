# -*- coding: utf-8 -*-
"""
lori.components.access
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import glob
import os.path
from collections.abc import Callable
from typing import Any, Collection, Mapping, Optional, Sequence, Type

from lori._core._component import Component, _Component, _ComponentContext  # noqa
from lori._core._data import _DataManager  # noqa
from lori.core import Configurations, RegistratorAccess, ResourceError
from lori.util import get_context, update_recursive


# noinspection PyProtectedMember
class ComponentAccess(_ComponentContext, RegistratorAccess[Component]):
    # noinspection PyUnresolvedReferences
    def __init__(self, registrar: Component, **kwargs) -> None:
        context = get_context(registrar, _DataManager).components
        super().__init__(context, registrar, **kwargs)

    # noinspection PyShadowingBuiltins
    def _set(self, id: str, component: Component) -> None:
        if not isinstance(component, _Component):
            raise ResourceError(f"Invalid component type: {type(component)}")

        super()._set(id, component)

    def load(
        self,
        configs: Optional[Configurations] = None,
        configs_file: Optional[str] = None,
        configs_dir: Optional[str] = None,
        configure: bool = False,
        **kwargs: Any,
    ) -> Sequence[Component]:
        return super().load(configs, configs_file, configs_dir, configure, strict=True)

    # noinspection PyUnresolvedReferences
    def load_from_type(
        self,
        type: Type[Component],
        configs: Configurations,
        section: str,
        key: str,
        name: Optional[str] = None,
        includes: Optional[Collection[str]] = (),
        defaults: Optional[Mapping[str, Any]] = None,
        configure: bool = False,
        sort: bool = True,
        **kwargs,
    ) -> Sequence[Component]:
        kwargs["factory"] = type
        components = []
        if defaults is None:
            defaults = self._load_registrator_defaults(strict=True)
        if any(i in configs.sections for i in includes):
            configs["key"] = key
            configs["name"] = name
        configs = configs.get_section(section, defaults={**configs.get_sections(includes), **defaults})
        if any(i in configs.sections for i in includes):
            components.append(self._load_from_configs(self._registrar, configs, **kwargs))

        update_recursive(defaults, _Component._build_defaults(configs, includes))

        configs_dirs = configs.dirs.copy()
        configs_sections = configs.get_sections([s for s in configs.sections if s not in defaults])

        components.extend(self._load_from_sections(self._registrar, configs_sections, defaults=defaults, **kwargs))

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
