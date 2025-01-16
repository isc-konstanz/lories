# -*- coding: utf-8 -*-
"""
lori.application.view.interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
import os
import shutil
from importlib import resources
from pathlib import Path
from typing import Optional

import dash
from dash import Dash, dcc, html
from dash_bootstrap_components import themes

from lori import Configurations
from lori.application import Application
from lori.application.interface import Interface, InterfaceMeta
from lori.application.view.pages import PageFooter, PageHeader, View

logging.getLogger("werkzeug").setLevel(logging.WARNING)


class ViewInterfaceMeta(InterfaceMeta):
    # noinspection PyProtectedMember
    def __call__(cls, context: Application, configs: Configurations) -> Interface:
        global _instance
        if _instance is None:
            _instance = super().__call__(context, configs)
        return _instance


# noinspection PyProtectedMember
class ViewInterface(Interface, Dash, metaclass=ViewInterfaceMeta):
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

        view_path = resources.files("lori.application.view")
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
        theme = configs.get_section("theme", defaults=theme_defaults)

        header = PageHeader(**theme)
        footer = PageFooter()

        self.view = View(context.id, header, footer)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._do_create_view()

    # noinspection PyUnresolvedReferences
    def create_view_layout(self) -> html.Div:
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

    # noinspection PyUnresolvedReferences, PyArgumentList
    def _do_create_view(self) -> None:
        self.view._do_create_pages(self.context.components)
        self.view._do_create_layout()
        self.layout = self.create_view_layout

    def start(self) -> None:
        self.view._do_register()
        self.run()  # debug=self._logger.isEnabledFor(logging.DEBUG))


_instance: Optional[ViewInterface] = None
