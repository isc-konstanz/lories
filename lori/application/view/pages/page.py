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
    name: str
    title: str
    description: Optional[str]

    order: int = 100

    group: Optional[Page]

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
        group: Optional[Page] = None,
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

        self.group = group

    @property
    @abstractmethod
    def key(self) -> str:
        pass

    @property
    def path(self) -> str:
        _path = f"/{self._encode_id(self.key)}"
        return _path if self.group is None else self.group.path + _path

    def is_created(self) -> bool:
        return self._created

    @abstractmethod
    def create_layout(self, layout: PageLayout) -> None:
        pass

    # noinspection PyUnresolvedReferences
    @wraps(create_layout, updated=())
    def _do_create_layout(self, *args, **kwargs) -> None:
        if self.is_registered():
            raise PageException(f"Trying to create layout of already registered {type(self).__name__}: {self.name}")
        if self.is_created():
            self._logger.warning(f"{type(self).__name__} '{self.id}' layout already created")
        else:
            self.layout = PageLayout(id=f"{self.id}-container")
            self._at_create_layout(self.layout)
            self._run_create_layout(self.layout, *args, **kwargs)
            self._on_create_layout(self.layout)
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
    def _do_register(self) -> None:
        if not self.is_created():
            raise PageException(f"Trying to register {type(self).__name__} without layout: {self.name}")
        if self.is_registered():
            self._logger.warning(f"{type(self).__name__} '{self.id}' already registered")
            return

        self._logger.debug(f"Registering {type(self).__name__} '{self.id}': {self.name}")

        self._run_register()
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
