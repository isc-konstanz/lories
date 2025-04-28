# -*- coding: utf-8 -*-
"""
lori.application.view.pages.login
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import dash_bootstrap_components as dbc
from dash import html, callback, Input, Output, State

from lori.application.view import Authentication
from lori.application.view.pages import Page, PageLayout


class LoginPage(Page):
    __auth: Authentication

    # noinspection PyShadowingBuiltins
    def __init__(self, id: str, authentication: Authentication, *args, **kwargs) -> None:
        super().__init__(id=f"{id}-login", key="login", name="Login", *args, **kwargs)
        self.__auth = authentication

    def create_layout(self, layout: PageLayout) -> None:
        layout.container.class_name = "card-container"

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
        login_button = dbc.Button("Log in", id=login_id, color="primary", style={"width": "100%"})
        layout.append(
            dbc.Row(
                dbc.Col(
                    [
                        dbc.Row(dbc.Card(dbc.CardBody(dbc.Form([username_input, password_input, login_button])))),
                        dbc.Row(id=notice_id),
                    ],
                    width=4,
                ),
                align="center",
                justify="center",
                style={"margin-top": "66vh"},
            )
        )

        @callback(
            Output(notice_id, 'children'),
           [Input(login_id, 'n_clicks')],
            state=[
                State(username_id, 'value'),
                State(password_id, 'value'),
            ],
        )
        def update_output(_, username, password):
            if username == '' or username is None or password == '' or password is None:
                return html.Div(children='')
            return html.Div(
                children=['Incorrect Password'],
                style={'padding-left':'550px','padding-top':'40px','font-size':'16px'})
