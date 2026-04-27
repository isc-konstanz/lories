# -*- coding: utf-8 -*-
"""
lories.application.view.login.page
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Optional

import dash_bootstrap_components as dbc
from dash import Dash, Input, Output, State, dcc, html
from dash_auth import public_callback
from flask_login import LoginManager

from lories.application import Application
from lories.application.view.login import Authentication, AuthenticationException
from lories.application.view.pages import Page, PageLayout
from lories.core import Configurations


# noinspection SpellCheckingInspection
class LoginPage(Page):
    __manager: LoginManager
    __auth: Authentication

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        app: Dash,
        context: Application,
        configs: Configurations,
    ) -> None:
        super().__init__(id=f"{context.id}-login", key="login", name="Login")
        self.__auth = Authentication(app, configs, self.path)
        self.__manager = LoginManager(app.server)
        self.__manager.login_view = self.path
        self.__manager.user_loader(self.__auth.get_user)
        self.__manager.request_loader(self.__auth.request_user)

    def create_layout(self, layout: PageLayout) -> None:
        layout.container.class_name = "card-container auth-container"

        username_id = f"{self.id}-username"
        username_input = html.Div(
            [
                dbc.Label("Username", html_for=username_id),
                dbc.Input(type="username", id=username_id, placeholder="Username"),
            ],
            className="mb-3",
        )

        password_id = f"{self.id}-password"
        password_input = html.Div(
            [
                dbc.Label("Password", html_for=password_id),
                dbc.Input(type="password", id=password_id, placeholder="Password"),
            ],
            className="mb-3",
        )

        notice_id = f"{self.id}-notice"
        login_id = f"{self.id}-login"
        login_button = dbc.Button("Log in", id=login_id, n_clicks=0, color="primary", style={"width": "100%"})
        layout.append(
            dbc.Row(
                dbc.Col(
                    [
                        dbc.Row(dbc.Card(dbc.CardBody(dbc.Form([username_input, password_input, login_button])))),
                        dbc.Row(id=notice_id, style={"min-height": "60px", "padding-top": "12px"}),
                    ],
                    width="auto",
                ),
                align="center",
                justify="center",
                style={"height": "100%"},
            )
        )

        @public_callback(
            Output(notice_id, "children"),
            Input(login_id, "n_clicks"),
            [
                State(username_id, "value"),
                State(password_id, "value"),
            ],
            prevent_initial_call=True,
        )
        def update_output(_, username, password) -> Optional[dbc.Alert] | dcc.Location:
            try:
                if username == "" or username is None:
                    failure = "Username is empty"
                elif password == "" or password is None:
                    failure = "Password is empty"
                elif not self.__auth.login_user(username, password):
                    failure = "Unknown Error"
                else:
                    return dcc.Location(id=f"{self.id}-success", pathname="/")

            except AuthenticationException as e:
                failure = e.reason

            return dbc.Alert(
                children=[
                    html.H5("Log in failed", className="alert-heading"),
                    html.P(failure),
                ],
                color="danger",
            )
