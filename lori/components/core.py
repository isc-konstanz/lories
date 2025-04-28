# -*- coding: utf-8 -*-
"""
lori.components.core
~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import abstractmethod
from typing import Any, Collection, Dict, List, Optional

import pandas as pd
from lori.core import Activator, Configurations, Registrator, ResourceException, ResourceUnavailableException
from lori.data import Channel, Channels, DataAccess


class _Component(Registrator, Activator):
    SECTION: str = "component"
    INCLUDES: List[str] = [DataAccess.SECTION]

    # noinspection PyProtectedMember
    @classmethod
    def _build_defaults(
        cls,
        configs: Configurations,
        includes: Optional[Collection[str]] = (),
        strict: bool = False
    ) -> Dict[str, Any]:
        defaults = super()._build_defaults(configs, includes)
        if strict and DataAccess.SECTION in defaults:
            defaults[DataAccess.SECTION][Channels.SECTION] = Channel._build_defaults(
                defaults[DataAccess.SECTION].get_section(Channels.SECTION, defaults={})
            )
        return defaults

    @property
    @abstractmethod
    def converters(self):
        pass

    @property
    @abstractmethod
    def connectors(self):
        pass

    @property
    @abstractmethod
    def components(self):
        pass

    @property
    @abstractmethod
    def data(self):
        pass

    @abstractmethod
    def get(
        self,
        start: Optional[pd.Timestamp, dt.datetime, str] = None,
        end: Optional[pd.Timestamp, dt.datetime, str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        pass


class ComponentException(ResourceException):
    """
    Raise if an error occurred accessing the connector.

    """

    # noinspection PyArgumentList
    def __init__(self, component: _Component, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.component = component


class ComponentUnavailableException(ResourceUnavailableException, ComponentException):
    """
    Raise if an accessed connector can not be found.

    """
