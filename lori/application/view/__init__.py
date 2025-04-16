# -*- coding: utf-8 -*-
"""
lori.application.view
~~~~~~~~~~~~~~~~~~~~~


"""

from . import authentication  # noqa: F401
from .authentication import Authentication  # noqa: F401

from . import pages  # noqa: F401
from .pages import (  # noqa: F401
    Page,
    PageGroup,
    PageHeader,
    PageFooter,
    View,
    ComponentPage,
    ComponentGroup,
)

from .interface import ViewInterface  # noqa: F401
