# -*- coding: utf-8 -*-
"""Unit tests for the generic ``type = "component"`` registration (registry only, no runtime)."""

from __future__ import annotations

from lories.components import Component, GenericComponent, registry


def test_generic_type_is_registered():
    assert registry.has_type("component")
    assert registry.from_type("component").type is GenericComponent


def test_generic_registration_targets_subclass_not_base():
    # The type must be registered on the trivial subclass, not on the base class:
    # a registry filter for "component" would otherwise match every component.
    registration = registry.from_type("component")
    assert registration.type is not Component
    assert issubclass(GenericComponent, Component)
