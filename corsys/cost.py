# -*- coding: utf-8 -*-
"""
    corsys.cost
    ~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from collections.abc import Mapping

import re

from .configs import ConfigurationException, ConfigurationUnavailableException


class Cost(Mapping):

    SECTION = 'Cost'

    def __init__(self, **costs) -> None:
        super().__init__()
        self._costs = costs

    def __repr__(self):
        cost = re.sub(r'(?<!^)(?=[A-Z])', ' ', self.__class__.__name__)
        return (f'{cost}: \n  ' + '\n  '.join(
            f'{k}: {str(v)}' for k, v in self._costs.items()))

    def __setattr__(self, attr, val) -> None:
        super().__setattr__(attr, val)
        if not attr.startswith('_'):
            self._costs[attr] = val

    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        costs = Cost.__getattribute__(self, '_costs')
        if attr in costs.keys():
            return costs[attr]
        try:
            # noinspection PyUnresolvedReferences
            return super().__getattr__(attr)

        except AttributeError:
            raise AttributeError("'{0}' object has no attribute '{1}'".format(type(self).__name__, attr))

    def __getitem__(self, key: str):
        return self._costs[key]

    def __iter__(self):
        return iter(self._costs)

    def __len__(self) -> int:
        return len(self._costs)


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
