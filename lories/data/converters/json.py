# -*- coding: utf-8 -*-
"""
lories.data.converters.json
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import json
import jsonpath_ng as jsonpath
from typing import Any, AnyStr, Optional, Sequence

from lories.data.converters.converter import StringConverter, ConversionError


# noinspection PyMethodMayBeStatic
class JsonConverter(StringConverter):
    def convert(self, value: Any, path: str = "$.value", **kwargs) -> Optional[AnyStr | Sequence[AnyStr]]:
        if isinstance(value, str):
            try:
                json_data = json.loads(value)
                json_path = jsonpath.parse(path)
                json_match = json_path.find(json_data)

                if len(json_match) == 1:
                    return json_match[0].value
                else:
                    return [match.value for match in json_match]

            except json.JSONDecodeError as e:
                raise ConversionError(e) from e
        return None
