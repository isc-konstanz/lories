# -*- coding: utf-8 -*-
"""
lories.data.converters.json
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import json
import jsonpath_ng as jsonpath
from typing import Any, AnyStr, Optional

from lories.data.converters.converter import StringConverter, ConversionError


# noinspection PyMethodMayBeStatic
class JsonConverter(StringConverter):
    def convert(self, value: Any, expression: str = "$.value", **kwargs) -> Optional[AnyStr]:
        if isinstance(value, str):
            try:
                json_data = json.loads(value)
                json_expression = jsonpath.parse(expression)
                json_match = json_expression.find(json_data)

                return json_match[0].value

            except json.JSONDecodeError as e:
                raise ConversionError(e) from e
        return None
