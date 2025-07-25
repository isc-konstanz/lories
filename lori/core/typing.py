# -*- coding: utf-8 -*-
"""
lori.core.typing
~~~~~~~~~~~~~~~~


"""

from typing import Iterable, TypeVar

from lori.core import Resource, Resources

ResourcesType = TypeVar("ResourcesType", Resource, Resources, Iterable[Resource], Iterable[str], str)
