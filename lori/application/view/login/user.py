# -*- coding: utf-8 -*-
"""
lori.application.view.login.user
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from flask_bcrypt import check_password_hash, generate_password_hash
from flask_login import UserMixin


# noinspection PyShadowingBuiltins
class User(UserMixin):
    name: str
    hash: str

    _authenticated: bool = False

    @classmethod
    def from_password(cls, name: str, password: str) -> User:
        return cls(name, generate_password_hash(password))

    @property
    def id(self) -> str:
        return self.name

    def __init__(self, name: str, hash: str) -> None:
        self.name = name
        self.hash = hash

        self._authenticated = False

    # noinspection PyShadowingBuiltins
    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name}:{self.hash[8:]}*)"

    # noinspection PyShadowingBuiltins
    def __str__(self) -> str:
        vars = [
            f"name = {self.name}",
            f"hash = {self.hash[8:]}*",
        ]
        return f"{type(self).__name__}:\n\t" + "\n\t".join(vars)

    @property
    def is_authenticated(self) -> bool:
        return self._authenticated

    def authenticate(self, password: str) -> bool:
        self._authenticated = check_password_hash(self.hash, password)
        return self._authenticated
