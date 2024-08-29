# -*- coding: utf-8 -*-
"""
loris.app.view.pages.page
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
import re
from abc import ABC, ABCMeta, abstractmethod
from functools import wraps
from typing import Optional, TypeVar

import dash
from dash.development.base_component import Component

import pandas as pd
from loris.app import InterfaceException
from loris.app.view.pages.layout import PageLayout

C = TypeVar("C", bound=Component)


class PageMeta(ABCMeta):
    # noinspection PyProtectedMember
    def __call__(cls, *args, **kwargs):
        page = super().__call__(*args, **kwargs)

        page._Page__create_layout = page.create_layout
        page.create_layout = page._do_create_layout

        page._Page__register = page.register
        page.register = page._do_register

        return page


# noinspection PyShadowingBuiltins
class Page(ABC, metaclass=PageMeta):
    id: str
    name: str
    title: str
    description: Optional[str]

    order: int = 100

    layout: PageLayout

    _created: bool = False
    _registered: bool = False

    def __init__(
        self,
        id: str,
        name: str,
        title: Optional[str] = None,
        description: Optional[str] = None,
        order: Optional[int] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__()
        self._logger = logging.getLogger(self.__module__)

        self.id = self._encode_id(id)
        self.name = name
        self.title = title if title is not None else name
        self.description = description

        if not pd.isna(order):
            self.order = order

    @property
    @abstractmethod
    def path(self) -> str:
        pass

    def is_created(self) -> bool:
        return self._created

    @abstractmethod
    def create_layout(self, layout: PageLayout) -> None:
        pass

    # noinspection PyUnresolvedReferences
    @wraps(create_layout, updated=())
    def _do_create_layout(self) -> PageLayout:
        if self.is_registered():
            raise PageException(f"Trying to create layout of already registered {type(self).__name__}: {self.name}")
        if self.is_created():
            self._logger.warning(f"{type(self).__name__} '{self.id}' layout already created")
        else:
            self.layout = PageLayout(id=f"{self.id}-container")
            self.__create_layout(self.layout)
            self._on_create_layout(self.layout)
            self._created = True

        return self.layout

    def _on_create_layout(self, layout: PageLayout) -> None:
        pass

    def is_registered(self) -> bool:
        return self._registered

    # noinspection PyMethodMayBeStatic
    def register(self, **kwargs) -> None:
        self._logger.debug(f"Registering '{type(self).__name__}' page: {self.id}")
        dash.register_page(
            self.id,
            path=self.path,
            name=self.name,
            title=self.title,
            description=self.description,
            layout=self.layout.container,
            **kwargs,
        )

    # noinspection PyUnresolvedReferences
    @wraps(register, updated=())
    def _do_register(self) -> None:
        if not self.is_created():
            raise PageException(f"Trying to register {type(self).__name__} without layout: {self.name}")
        if self.is_registered():
            self._logger.warning(f"{type(self).__name__} '{self.id}' already registered")
            return

        self._logger.debug(f"Registering {type(self).__name__} '{self.id}': {self.name}")

        self.__register()
        self._on_register()
        self._registered = True

    def _on_register(self) -> None:
        pass

    @staticmethod
    def _encode_id(id) -> str:
        # TODO: Implement correct URI encoding
        id = re.sub(r"[^0-9A-Za-zäöüÄÖÜß]+", "-", id).lower()
        id = id.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
        return id


class PageException(InterfaceException):
    """
    Raise if an error occurred accessing a page.

    """
