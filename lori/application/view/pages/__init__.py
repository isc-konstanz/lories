# -*- coding: utf-8 -*-
"""
lori.application.view.pages
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from . import layout  # noqa: F401
from .layout import (  # noqa: F401
    PageCard,
    PageLayout,
)

from . import page  # noqa: F401
from .page import (  # noqa: F401
    Page,
    PageException,
)

from . import group  # noqa: F401
from .group import PageGroup  # noqa: F401

from . import components  # noqa: F401
from .components import (  # noqa: F401
    ComponentPage,
    ComponentGroup,
)

from . import login
from .login import LoginPage  # noqa: F401

from .header import PageHeader  # noqa: F401
from .footer import PageFooter  # noqa: F401

from .registry import PageRegistry  # noqa: F401

from .view import (  # noqa: F401
    View,
    register_component_page,
    register_component_group,
    registry,
)

from .components import system  # noqa: F401
from .components.system import SystemPage  # noqa: F401

from .components import weather  # noqa: F401
from .components.weather import WeatherPage  # noqa: F401
