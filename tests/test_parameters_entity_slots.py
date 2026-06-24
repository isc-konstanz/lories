# -*- coding: utf-8 -*-
"""B1 — declared entity slots survive ``configure`` instead of being clobbered to None (issue 02, HIGH)."""
from __future__ import annotations

from lories.core.configs.configurator import Configurator
from lories.core.configs.parameters import ComponentParameter, Parameter, ParameterGroup


class Holder(Configurator):
    child = ComponentParameter(cls=object, desc="A nested component slot")
    label = Parameter(type=str, default="holder")
    group = ParameterGroup(
        required=False,
        children=[Parameter(key="n", type=int, default=1)],
    )


def test_entity_slot_survives_configure(write_conf):
    """Red before the fix (slot clobbered to ``None``); green after (skipped)."""
    configs = write_conf('label = "x"\n')
    holder = Holder()

    sentinel = object()
    holder.child = sentinel  # simulate ComponentAccess having loaded a child

    holder.configure(configs)

    assert holder.child is sentinel  # NOT None
    assert holder.label == "x"  # normal params still resolve


def test_parameter_group_result_is_bound(write_conf):
    """Documented B1 decision: ``ParameterGroup`` results ARE set on the instance."""
    configs = write_conf("label = \"x\"\n[group]\nn = 7\n")
    holder = Holder()
    holder.configure(configs)
    assert holder.group == {"n": 7}


def test_optional_group_absent_binds_none(write_conf):
    configs = write_conf('label = "x"\n')
    holder = Holder()
    holder.configure(configs)
    assert holder.group is None
