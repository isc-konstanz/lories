# -*- coding: utf-8 -*-
"""
loris.core.register.registrator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import Collection, Optional

from loris.core import ConfigurationException, Configurations, Configurator, Context, ResourceException
from loris.util import get_context, parse_key


class Registrator(Configurator):
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
        context: Optional[Context | Registrator] = None,
        configs: Optional[Configurations] = None,
        key: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(context, configs, *args, **kwargs)
        self._key = self._assert_key(context, configs, key)
        self._id = self._assert_id(context, configs, self._key)

    # noinspection PyMethodMayBeStatic
    def _assert_context(self, context: Optional[Context]) -> Optional[Context]:
        from loris.core import RegistratorContext

        if context is None:
            return None
        if not isinstance(context, (RegistratorContext, Registrator)):
            raise ResourceException(f"Invalid context: {None if context is None else type(context)}")
        return get_context(context, Context)

    # noinspection PyMethodMayBeStatic, PyUnusedLocal
    def _assert_id(self, context: Optional[Context], configs: Optional[Configurations], key: str) -> str:
        from loris.core import RegistratorContext

        if context is None:
            return key
        if not isinstance(context, (Registrator, RegistratorContext)):
            raise ResourceException(f"Invalid context: {None if context is None else type(context)}")
        if not isinstance(context, Registrator):
            # noinspection PyTypeChecker
            context = get_context(context, Registrator)
        return f"{context.id}.{key}"

    # noinspection PyUnusedLocal
    def _assert_key(self, context: Optional[Context], configs: Optional[Configurations], key: Optional[str]) -> str:
        if configs is not None:
            if configs.has_section(self.SECTION) and "key" in configs[self.SECTION]:
                key = configs[self.SECTION]["key"]
            elif "key" in configs:
                key = configs["key"]
            elif key is None:
                raise ConfigurationException(f"Invalid configuration, missing specified {self.SECTION}.key")
        elif key is None:
            raise ConfigurationException("Missing configuration")
        return parse_key(key)

    @property
    def id(self) -> str:
        return self._id

    @property
    def key(self) -> str:
        return self._key
