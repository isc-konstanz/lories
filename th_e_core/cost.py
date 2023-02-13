# -*- coding: utf-8 -*-
"""
    th-e-core.cost
    ~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from typing import Dict, Any

from .configs import ConfigurationException, ConfigurationUnavailableException


class Cost:

    SECTION = 'Cost'

    def __init__(self, costs: Dict[str, Any]) -> None:
        self._costs = costs

    def __repr__(self):
        return ('Cost: \n  ' + '\n  '.join(
            f'{k}: {str(v)}' for k, v in self._costs.items()))

    def __getattr__(self, attr):
        if attr in self._costs.keys():
            val = self._costs[attr]
            if val.isdigit():
                val = float(val)
                if val.is_integer():
                    val = int(val)
            return val
        try:
            # noinspection PyUnresolvedReferences
            return super().__getattr__(attr)

        except AttributeError:
            raise AttributeError("'{0}' object has no attribute '{1}'".format(type(self).__name__, attr))


class CostException(ConfigurationException):
    """
    Raise if a cost section is invalid.

    """
    pass


class CostUnavailableException(ConfigurationUnavailableException):
    """
    Raise if a cost section can not be found.

    """
    pass
