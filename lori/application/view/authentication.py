# -*- coding: utf-8 -*-
"""
lori.application.view.authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import base64
from typing import Callable, Dict, List, Optional, Union

import flask
from dash import Dash
from dash_auth.auth import Auth

from lori.core import Configurations, Configurator

UserGroups = Dict[str, List[str]]


class Authentication(Configurator, Auth):
    SECTION: str = "auth"

    __hashes: Dict[str, str]
    __groups: Optional[Union[UserGroups, Callable[[str], UserGroups]]] = None

    def __init__(
        self,
        app: Dash,
        configs: Configurations,
        public_routes: Optional[list] = None,
        secret_key: str = None,
    ):
        super().__init__(app=app, public_routes=public_routes)
        self.configure(configs)
        if secret_key is not None:
            app.server.secret_key = secret_key

        self.__groups = None
        self.__hashes = {"admin": "admin"}

    def is_authorized(self):
        header = flask.request.headers.get("Authorization", None)
        if not header:
            return False
        username_password = base64.b64decode(header.split("Basic ")[1])
        username_password_utf8 = username_password.decode("utf-8")
        username, password = username_password_utf8.split(":", 1)
        authorized = self.__hashes.get(username) == password
        if authorized:
            try:
                flask.session["user"] = {"email": username, "groups": []}
                if callable(self.__groups):
                    flask.session["user"]["groups"] = self.__groups(username)
                elif self.__groups:
                    flask.session["user"]["groups"] = self.__groups.get(username, [])
            except RuntimeError:
                self._logger.warning("Session is not available. Have you set a secret key?")
        return authorized

    def login_request(self):
        return flask.Response(
            'Login Required',
            headers={'WWW-Authenticate': 'Basic realm="User Visible Realm"'},
            status=401,
        )
