# -*- coding: utf-8 -*-
"""
lori.application.view.pages.page
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
import re
from abc import ABC, ABCMeta, abstractmethod
from functools import wraps
from typing import Any, Optional, TypeVar

import dash
from dash.development.base_component import Component

import pandas as pd
from lori.application import InterfaceException
from lori.application.view.pages.layout import PageLayout
from lori.util import validate_key

C = TypeVar("C", bound=Component)


class PageMeta(ABCMeta):
    # noinspection PyProtectedMember
    def __call__(cls, *args, **kwargs):
        page = super().__call__(*args, **kwargs)
        cls._wrap_method(page, "create_layout")
        cls._wrap_method(page, "register")

        return page

    # noinspection PyShadowingBuiltins
    @staticmethod
    def _wrap_method(object: Any, method: str) -> None:
        setattr(object, f"_run_{method}", getattr(object, method))
        setattr(object, method, getattr(object, f"_do_{method}"))


# noinspection PyShadowingBuiltins
class Page(ABC, metaclass=PageMeta):
    id: str
    key: str
    name: str
    title: str
    description: Optional[str]

    order: int = 100

    group: Optional[Page]

    _layout: PageLayout
    _created: bool = False
    _registered: bool = False

    def __init__(
        self,
        id: str,
        key: str,
        name: str,
        title: Optional[str] = None,
        description: Optional[str] = None,
        order: Optional[int] = None,
        group: Optional[Page] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__()
        self._logger = logging.getLogger(self.__module__)

        self.id = self._encode_id(id)
        self.key = validate_key(key)
        self.name = name
        self.title = title if title is not None else name
        self.description = description

        self.order = Page.order if pd.isna(order) else order
        self.group = group
        self._layout = PageLayout(id=f"{self.id}-container")

    @property
    def path(self) -> str:
        _path = f"/{self._encode_id(self.key)}"
        return _path if self.group is None else self.group.path + _path

    @property
    def layout(self) -> PageLayout:
        return self._layout

    def is_created(self) -> bool:
        return self._created

    @abstractmethod
    def create_layout(self, layout: PageLayout) -> None:
        pass

    # noinspection PyUnresolvedReferences
    @wraps(create_layout, updated=())
    def _do_create_layout(self, layout: PageLayout, *args, **kwargs) -> None:
        if self.is_registered():
            raise PageException(f"Trying to create layout of already registered {type(self).__name__}: {self.name}")
        if self.is_created():
            self._logger.warning(f"{type(self).__name__} '{self.id}' layout already created")
        else:
            self._at_create_layout(layout)
            self._run_create_layout(layout, *args, **kwargs)
            self._on_create_layout(layout)
            self._created = True

    def _at_create_layout(self, layout: PageLayout) -> None:
        pass

    def _on_create_layout(self, layout: PageLayout) -> None:
        pass

    def is_registered(self) -> bool:
        return self._registered

    # noinspection PyMethodMayBeStatic
    def register(self, **kwargs) -> None:
        self._logger.info(f"Registering '{type(self).__name__}' page: {self.id} at {self.path}")
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
    def _do_register(self, **kwargs) -> None:
        if not self.is_created():
            raise PageException(f"Trying to register {type(self).__name__} without layout: {self.name}")
        if self.is_registered():
            self._logger.warning(f"{type(self).__name__} '{self.id}' already registered")
            return

        self._logger.debug(f"Registering {type(self).__name__} '{self.id}': {self.name}")

        self._at_register(**kwargs)
        self._run_register(**kwargs)
        self._on_register(**kwargs)
        self._registered = True

    def _at_register(self, **kwargs) -> None:
        pass

    def _on_register(self, **kwargs) -> None:
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
