# -*- coding: utf-8 -*-
"""
loris.core.register.registrator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import Collection, Optional

from loris.core import ConfigurationException, Configurations, Configurator, Context, ResourceException
from loris.util import get_context, parse_id


class Registrator(Configurator):
    _registrator: Optional[Registrator] = None

    # noinspection PyPep8Naming
    @property
    @abstractmethod
    def SECTIONS(self) -> Collection[str]:
        pass

    # noinspection PyPep8Naming
    @property
    @abstractmethod
    def SECTION(self) -> str:
        pass

    # noinspection PyPep8Naming
    @property
    @abstractmethod
    def TYPE(self) -> str:
        pass

    # noinspection PyProtectedMember
    def __init__(
        self,
        context: Optional[Registrator | Context] = None,
        configs: Configurations = None,
        *args, **kwargs
    ) -> None:
        from loris.core import RegistratorContext
        from loris.data.context import DataContext
        if context is not None and not isinstance(context, (Registrator, RegistratorContext)):
            raise ResourceException(f"Invalid context: {None if context is None else type(context)}")
        super().__init__(get_context(context, Context), configs, *args, **kwargs)

        if context is not None:
            self._registrator = context if isinstance(context, Registrator) else get_context(context, DataContext)
        if configs is None:
            raise ConfigurationException("Missing configuration")
        if self.SECTION not in configs.sections:
            configs._add_section(self.SECTION, {"type": self.TYPE})
        elif "type" not in configs[self.SECTION]:
            configs[self.SECTION]["type"] = self.TYPE

        if "key" in configs:
            configs[self.SECTION]["key"] = configs.pop("key")
        if "key" not in configs[self.SECTION]:
            raise ConfigurationException(f"Invalid configuration, missing specified {self.SECTION} Key")

        self._key = parse_id(configs[self.SECTION].get("key"))
        self._id = self._key if self._registrator is None else f"{self._registrator.id}.{self._key}"

    @property
    def id(self) -> str:
        return self._id

    @property
    def key(self) -> str:
        return self._key
