# -*- coding: utf-8 -*-
"""
lories.tests.test_register_registry_alias
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``Registration.is_alias`` used to prefix-match (``type.startswith(alias)``),
so a registered alias silently claimed every type name starting with it --
registering ``("openems_edge", "openems")`` then ``("openems_backend")``
failed. The duplicate-registration error path then crashed with a bare
``StopIteration`` because it searched keys only, never aliases
(lories-frictions issue 07).
"""

from __future__ import annotations

import pytest

from lories.core.register.registration import Registration, RegistrationError
from lories.core.register.registry import Registry


class _Thing:
    pass


class _Edge(_Thing):
    pass


class _Backend(_Thing):
    pass


def test_alias_matches_exactly_not_by_prefix():
    registration = Registration[_Thing](_Edge, "openems_edge", "openems")
    assert registration.is_type("openems_edge")
    assert registration.is_type("openems")
    assert not registration.is_type("openems_backend")
    assert not registration.is_alias("openems_x")


def test_sibling_type_registers_beside_a_shorter_alias():
    registry = Registry[_Thing]()
    registry.register(_Edge, "openems_edge", "openems")
    registry.register(_Backend, "openems_backend")

    assert registry.from_type("openems").type is _Edge
    assert registry.from_type("openems_backend").type is _Backend
    assert not registry.has_type("openems_x")


def test_duplicate_key_error_names_the_conflict():
    registry = Registry[_Thing]()
    registry.register(_Edge, "openems_edge", "openems")
    with pytest.raises(RegistrationError, match="'openems_edge' does already exist: _Edge"):
        registry.register(_Backend, "openems_edge")


def test_duplicate_via_alias_raises_registration_error_not_stopiteration():
    registry = Registry[_Thing]()
    registry.register(_Edge, "openems_edge", "openems")
    with pytest.raises(RegistrationError, match=r"'openems' does already exist \(as alias of 'openems_edge'\): _Edge"):
        registry.register(_Backend, "openems")


def test_replace_still_overrides_a_key():
    # Known limitation, unchanged here: replace=True over a name owned by another
    # registration's ALIAS inserts a new entry but from_type still returns the
    # first insertion-order match. Only key collisions truly override.
    registry = Registry[_Thing]()
    registry.register(_Edge, "openems_edge")
    registry.register(_Backend, "openems_edge", replace=True)
    assert registry.from_type("openems_edge").type is _Backend
