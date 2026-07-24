# -*- coding: utf-8 -*-
"""
lories.components.generic
~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from lories.components import Component, register_component_type


@register_component_type("component")
class GenericComponent(Component):
    """
    A generic component without domain-specific behavior: it bundles related data channels
    into a component of its own, configured entirely from TOML. Use it to group measurements
    that belong together — the meters of a building, a machine park, or a room zone — giving
    them a shared configuration scope, their own view page, and common channel defaults
    without writing a dedicated component class.
    """
