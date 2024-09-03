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
        configs: Optional[Configurations] = None,
        key: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(get_context(context, Context), configs, *args, **kwargs)
        if context is not None:
            from loris.core import RegistratorContext
            from loris.data.context import DataContext

            if context is not None and not isinstance(context, (Registrator, RegistratorContext)):
                raise ResourceException(f"Invalid context: {None if context is None else type(context)}")

            self._registrator = context if isinstance(context, Registrator) else get_context(context, DataContext)

        self._key = self._assert_key(configs, key)
        self._id = self._key if self._registrator is None else f"{self._registrator.id}.{self._key}"

    def _assert_key(self, configs: Optional[Configurations], key: Optional[str]) -> str:
        if configs is not None:
            if configs.has_section(self.SECTION) and "key" in configs[self.SECTION]:
                key = configs[self.SECTION]["key"]
            elif "key" in configs:
                key = configs["key"]
            elif key is None:
                raise ConfigurationException(f"Invalid configuration, missing specified {self.SECTION}.key")
        elif key is None:
            raise ConfigurationException("Missing configuration")
        return parse_id(key)

    @property
    def id(self) -> str:
        return self._id

    @property
    def key(self) -> str:
        return self._key
