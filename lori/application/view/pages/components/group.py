# -*- coding: utf-8 -*-
"""
lori.application.view.pages.components.group
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from functools import wraps
from typing import Dict, Generic, Iterable, Optional, Type, TypeVar

import dash_bootstrap_components as dbc
from dash import html

from lori import Component
from lori.application import InterfaceException
from lori.application.view.pages import PageGroup, PageLayout
from lori.application.view.pages.components import ComponentPage

ComponentType = TypeVar("ComponentType", bound=Component)
ChildrenType = TypeVar("ChildrenType", bound=Dict[str, Type[ComponentPage]])


# noinspection PyShadowingBuiltins
class ComponentGroup(PageGroup[ComponentPage], ComponentPage[ComponentType], Generic[ComponentType]):
    def __init__(self, component: ComponentType, children: Optional[ChildrenType] = None, *args, **kwargs) -> None:
        super().__init__(component=component, *args, **kwargs)
        if children is not None:
            for attr, factory in children.items():
                if not hasattr(component, attr):
                    raise InterfaceException(
                        f"Unknown child attribute '{attr}' for '{type(component).__name__}': {component.id}"
                    )
                _components = getattr(component, attr)
                if not isinstance(_components, Iterable) or not all(isinstance(c, Component) for c in _components):
                    raise InterfaceException(
                        f"Invalid child component types for '{type(component).__name__}': {type(_components)}"
                    )
                self.extend([factory(component=_component, group=self) for _component in _components])

    # noinspection PyTypeChecker
    @property
    def key(self) -> str:
        return self._component.key

    def create_layout(self, layout: PageLayout) -> None:
        super().create_layout(layout)

    def _create_data_layout(self, layout: PageLayout, title: Optional[str] = "Data") -> None:
        if len(self.data.channels) > 0:
            data = []
            if title is not None:
                data.append(html.H5(f"{title}:"))
            data.append(self._build_data())
            layout.append(dbc.Row(dbc.Col(dbc.Card(dbc.CardBody(data)))))

    # noinspection PyTypeChecker
    @wraps(create_layout, updated=())
    def _do_create_layout(self, *args, **kwargs) -> None:
        for page in self:
            page._do_create_layout(*args, **kwargs)
        super()._do_create_layout(*args, **kwargs)

    def _do_register(self) -> None:
        super()._do_register()
        for page in self:
            page._do_register()
