# -*- coding: utf-8 -*-
"""
lories.application.view.login.authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import base64
import os
import secrets
from typing import Callable, Dict, List, Optional, Union

import flask
from dash import Dash
from dash_auth.auth import Auth
from flask_login import current_user, login_user
from werkzeug.wrappers import Request, Response

import pandas as pd
from lories.application.interface import InterfaceException
from lories.application.view.login.user import User
from lories.core import Configurations, Configurator

RANDOM_STRING_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

UserGroups = Dict[str, List[str]]


class Authentication(Configurator, Auth):
    TYPE: str = "auth"

    __users: Dict[str, User]
    __groups: Optional[Union[UserGroups, Callable[[str], UserGroups]]] = None

    _login_route: str

    def __init__(
        self,
        app: Dash,
        configs: Configurations,
        login_route: str = "auth",
        public_routes: Optional[list] = None,
        secret_key: str = None,
    ):
        if public_routes is None:
            public_routes = []
        if login_route not in public_routes:
            public_routes.append(login_route)
        super().__init__(app=app, public_routes=public_routes)
        self.configure(configs)
        if secret_key is None:
            secret_key = os.getenv("SECRET_KEY", get_random_string(32))
        app.server.secret_key = secret_key

        self._login_route = login_route
        self.__groups = None
        self.__users = {}

        # TODO: Read users from file and add user creation and editing via commandline
        self.add_user("admin", "admin")

    def add_user(self, username: str, password: str) -> None:
        user = User.from_password(username, password)
        self.__users[user.name] = user

    def get_user(self, username: str) -> Optional[User]:
        return self.__users.get(username, None)

    def request_user(self, request: Optional[Request]) -> Optional[User]:
        if request is None:
            request = flask.request
        header = request.headers.get("Authorization", None)
        if not header:
            return None

        username_password = base64.b64decode(header.split("Basic ")[1])
        username_password_utf8 = username_password.decode("utf-8")
        username, password = username_password_utf8.split(":", 1)
        user = self.__users.get(username, None)
        if user is not None:
            user.authenticate(password)
        return user

    def is_authorized(self) -> bool:
        authenticated = current_user.is_authenticated
        if authenticated:
            # TODO: Validate if user is authorized to view route
            try:
                flask.session["user"] = {"email": current_user.id, "groups": []}
                if callable(self.__groups):
                    flask.session["user"]["groups"] = self.__groups(current_user.id)
                elif self.__groups:
                    flask.session["user"]["groups"] = self.__groups.get(current_user.id, [])
            except RuntimeError:
                self._logger.warning("Session is not available. Have you set a secret key?")
        return authenticated

    def login_user(self, username: str, password: str) -> bool:
        user = self.get_user(username)
        if user is not None:
            if user.authenticate(password):
                # TODO: Make remember arguments configurable
                remember = True
                remember_duration = pd.Timedelta(days=7)
                return login_user(user, remember=remember, duration=remember_duration)
            else:
                raise AuthenticationException("Incorrect Password")
        raise AuthenticationException(f"Unknown user '{username}'")

    def login_request(self) -> Response:
        # return flask.redirect(flask.url_for("auth", next_page="/"))
        return self.app.server.make_response(flask.redirect("/login"))


class AuthenticationException(InterfaceException):
    """
    Raise if an error occurred authenticating a session or user.

    """

    reason: str

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def get_random_string(length, allowed_chars=RANDOM_STRING_CHARS):
    """
    Return a securely generated random string.

    The bit length of the returned value can be calculated with the formula:
        log_2(len(allowed_chars)^length)

    For example, with default `allowed_chars` (26+26+10), this gives:
      * length: 12, bit length =~ 71 bits
      * length: 22, bit length =~ 131 bits
    """
    return "".join(secrets.choice(allowed_chars) for _ in range(length))
