# -*- coding: utf-8 -*-
"""
lori.components.core
~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import abstractmethod
from typing import List, Optional

import pandas as pd
from lori.core import Activator, Registrator, ResourceException, ResourceUnavailableException
from lori.data import DataAccess


class _Component(Registrator, Activator):
    SECTION: str = "component"
    INCLUDES: List[str] = [DataAccess.SECTION]

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
