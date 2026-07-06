# -*- coding: utf-8 -*-
"""
lories.application.view.interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
import os
import shutil
from importlib import resources
from pathlib import Path
from typing import Optional

import dash
import dash_bootstrap_components as dbc
from dash import Dash, dcc, html
from dash_bootstrap_components import themes

from lories.application import Application
from lories.application.interface import Interface, register_interface_type
from lories.application.view import LoginPage, PageFooter, PageHeader, View
from lories.application.view.pages.docs import DocsPage
from lories.application.view.snapshot import register_snapshot_routes
from lories.application.view.stream import register_stream_routes
from lories.components.cameras._core import _Camera
from lories.core.configs.parameters import Parameter
from lories.typing import Configurations

logging.getLogger("werkzeug").setLevel(logging.WARNING)


# noinspection PyProtectedMember
@register_interface_type("dash")
class ViewInterface(Interface, Dash):
    _proxy = Parameter(key="proxy", type=str, required=False, default=None, desc="Reverse proxy URL prefix path")
    _host = Parameter(key="host", type=str, default="127.0.0.1", desc="Host address to bind to")
    _port = Parameter(key="port", type=int, default=8050, desc="TCP port number")

    _proxy: Optional[str]
    _host: str
    _port: int

    def __init__(self, context: Application, configs: Configurations) -> None:
        def get_custom_path(key: str, default: Optional[str] = None) -> str:
            if "pages" in configs:
                custom_path = configs[key]
            else:
                custom_path = default
            if custom_path is None:
                return ""
            if os.path.isabs(custom_path):
                custom_path = Path(custom_path)
            else:
                custom_path = Path(configs.dirs.data, custom_path)
            if custom_path is not None and not custom_path.exists():
                custom_path.mkdir(exist_ok=True)
            return str(custom_path)

        view_path = resources.files("lories.application.view")
        pages_path = get_custom_path("pages")

        assets_default = str(view_path.joinpath("assets"))
        assets_path = get_custom_path("assets", default=assets_default)
        if assets_path != assets_default:

            def copy_assets(src, dest):
                dest = Path(dest)
                if dest.is_dir():
                    return
                if dest.exists() or dest.suffix not in [".ico", ".png", ".jpg", ".jpeg", ".css"]:
                    return
                shutil.copy2(src, dest)

            shutil.copytree(
                assets_default,
                assets_path,
                dirs_exist_ok=True,
                copy_function=copy_assets,
            )

        super().__init__(
            name=context.name,
            title=context.name,
            context=context,
            configs=configs,
            external_stylesheets=[themes.BOOTSTRAP],
            assets_folder=assets_path,
            pages_folder=pages_path,
            use_pages=True,
            server=True,  # TODO: Replace this with local Flask server, to create custom REST API ?
        )
        theme_defaults = {
            "name": context.name,
            "logo": os.path.join(assets_path, "logo.png"),
        }
        theme = configs.get_member("theme", defaults=theme_defaults)

        header = PageHeader(**theme)
        footer = PageFooter()

        self.view = View(context.id, header, footer)

        login = configs.get_member("login", defaults={"enabled": False})
        if login.enabled:
            login_page = LoginPage(self, context, configs)
            self.view.append(login_page)
        self._login_required = bool(login.enabled)

    # noinspection PyUnresolvedReferences
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._proxy = configs.get("proxy", default=None)
        self._host = configs.get("host", default="127.0.0.1")
        self._port = configs.get_int("port", default=8050)
        self._reload = configs.get_bool("reload", default=False)

        self.view.create_pages(self.context.components)
        self.view.create_connector_pages(self.context.connectors)
        for component in self.context.components.values():
            self.view.create_connector_pages(component.connectors)
        docs_page = DocsPage()
        self.view.append(docs_page)
        self.view.create_layout(self.view.layout)
        self.view.header.menu.append(dbc.NavItem(dbc.NavLink(docs_page.title, href=docs_page.path)))
        self.view.register()
        self.layout = self.create_layout

        register_stream_routes(
            self.server,
            self._find_channel,
            require_login=self._login_required,
        )
        register_snapshot_routes(
            self.server,
            self._find_component,
            require_login=self._login_required,
        )

    def _find_channel(self, channel_id: str):
        """Resolve a fully-qualified channel id by walking the component tree.

        Channels owned by a ``_Camera`` whose ``preview`` flag is off are
        hidden from HTTP — the camera is not published to clients.
        """

        def visit(component):
            data = getattr(component, "data", None)
            if data is not None and channel_id in data:
                if isinstance(component, _Camera) and not getattr(component, "preview", False):
                    return None
                return data[channel_id]
            for sub in getattr(component, "components", {}).values():
                channel = visit(sub)
                if channel is not None:
                    return channel
            return None

        return visit(self.context)

    def _find_component(self, component_id: str):
        """Resolve a fully-qualified component id by walking the component tree."""

        def visit(component):
            if getattr(component, "id", None) == component_id:
                return component
            for sub in getattr(component, "components", {}).values():
                found = visit(sub)
                if found is not None:
                    return found
            return None

        return visit(self.context)

    def start(self) -> None:
        self.run(
            host=self._host,
            port=self._port,
            proxy=self._proxy,
            debug=self._logger.getEffectiveLevel() <= logging.DEBUG,
            use_reloader=self._reload,
        )

    # noinspection PyUnresolvedReferences
    def create_layout(self) -> html.Div:
        return html.Div(
            id=f"{self.context.id}",
            children=[
                self.view.header.navbar,
                dash.page_container,
                dcc.Interval(
                    id="view-update",
                    interval=1000,
                ),
            ],
        )
