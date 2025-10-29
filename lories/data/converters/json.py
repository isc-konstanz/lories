# -*- coding: utf-8 -*-
"""
lories.data.converters.json
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import json
from typing import Any, AnyStr, Optional, Sequence

import jsonpath_ng as jsonpath

from lories.data.converters.context import register_converter_type
from lories.data.converters.converter import ConversionError, StringConverter


# noinspection PyMethodMayBeStatic
@register_converter_type("jsonpath", "json")
class JsonConverter(StringConverter):
    def convert(self, value: Any, path: str = "$.value", **kwargs) -> Optional[AnyStr | Sequence[AnyStr]]:
        if isinstance(value, str):
            try:
                json_data = json.loads(value)
                json_path = jsonpath.parse(self._fix_path(path))
                json_match = json_path.find(json_data)

                if len(json_match) == 1:
                    return json_match[0].value
                elif len(json_match) > 1:
                    return [match.value for match in json_match]

            except json.JSONDecodeError as e:
                raise ConversionError(e) from e
        return None
    
    def _fix_path(self, path: str) -> str:
        """
        Fix JSONPath to handle keys with colon (:)
        Example:
            sensor.temperature:avg.value  -> sensor.["temperature:avg"].value
        """
        if ":" not in path:
            return path
        
        # parts = path.split(".")
        # fixed_parts = []
        # for part in parts:
        #     if ":" in part:
        #         fixed_parts.append(f'["{part}"]')
        #     else:
        #         fixed_parts.append(part)
        # return ".".join(fixed_parts)
        return ".".join([f'["{part}"]' if ":" in part else part for part in path.split(".")])