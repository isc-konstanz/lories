# -*- coding: utf-8 -*-
"""
lories.io.rest
~~~~~~~~~~~~~~


"""

from typing import Optional, Union

import requests


class Rest:
    _host: str
    _port: int
    _username: str
    _password: str
    _endpoint: str

    _timeout: Optional[Union[int, float]]

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        endpoint: str,
        timeout: Union[int, float],
    ):
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._endpoint = endpoint
        self._timeout = timeout

    def get_request(self, path: str, params: Optional[dict] = None) -> str:
        response = requests.get(
            self._build_url(path), params=params, auth=(self._username, self._password), timeout=self._timeout
        )
        if response.status_code != 200:
            raise ConnectionError(f"REST request failed with status code {response.status_code}: {response.text}")
        return response.text

    def _build_url(self, path: str) -> str:
        return f"http://{self._host}:{self._port}/{self._endpoint}/{path}"
