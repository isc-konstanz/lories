# -*- coding: utf-8 -*-
"""
lories.io.jsonrpc
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import json
import uuid
from typing import Any, Optional


class JsonRpc:
    _version: str

    def __init__(self):
        self._version = "2.0"

    def _build_request(self, method: str, params: Optional[Any] = None, stringify: bool = True) -> str | dict:
        request = {"jsonrpc": self._version, "method": method, "id": self._next_id(), "params": {}}
        if params is not None:
            request["params"] = params

        if not stringify:
            return request
        return json.dumps(request)

    def build_request(self, args: list[tuple[str, dict]], stringify: bool = True) -> str:
        request = None

        for i, (method, params) in enumerate(args[-1::-1]):
            if request is None:
                request = self._build_request(method, params, stringify=False)
            else:
                request = self._build_request(method, {**params, "payload": request}, stringify=False)

        # request["jsonrpc"] = self._version
        # request["id"] = self._next_id()

        if not stringify:
            return request
        return json.dumps(request)

    @staticmethod
    def _next_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def parse_response(response: str) -> Any:
        response_data = json.loads(response)
        if "error" in response_data:
            raise Exception(f"JSON-RPC Error: {response_data['error']}")
        result = response_data.get("result")

        if "payload" in result:
            return JsonRpc.parse_response(json.dumps(result["payload"]))
        return result
