# -*- coding: utf-8 -*-
"""B3 — configure behaves identically at INFO and DEBUG (issue 03; the dropped proxy Heisenbug)."""

from __future__ import annotations

import logging

import pytest

from lories._core._configurations import _Configurations
from lories.core.configs.configurator import Configurator
from lories.core.configs.parameters import Parameter, ParameterGroup


class Probe(Configurator):
    host = Parameter(type=str, default="h")
    section = ParameterGroup(
        required=False,
        children=[Parameter(key="inner", type=str, default="i")],
    )

    def configure(self, configs):  # the metaclass wraps this as _run_configure
        self.seen_is_configs = isinstance(configs, _Configurations)
        self.seen_type = type(configs).__name__
        # old DEBUG proxy dropped include=/exclude= -> TypeError; exercise it to prove parity
        self.seen_member = configs.get_member("section", exclude={"absent"})


@pytest.fixture
def at_level():
    saved = logging.getLogger(Probe.__module__).level

    def _run(level, configs):
        logging.getLogger(Probe.__module__).setLevel(level)
        probe = Probe()
        probe.configure(configs)
        return probe

    yield _run
    logging.getLogger(Probe.__module__).setLevel(saved)


def test_info_debug_parity(write_conf, at_level):
    info = at_level(logging.INFO, write_conf('host = "x"\n[section]\ninner = "y"\n'))
    debug = at_level(logging.DEBUG, write_conf('host = "x"\n[section]\ninner = "y"\n'))

    assert info.seen_is_configs is True
    assert debug.seen_is_configs is True
    assert info.seen_type == debug.seen_type == "Configurations"


def test_debug_isinstance_and_get_member_kwargs(write_conf, at_level):
    debug = at_level(logging.DEBUG, write_conf('host = "x"\n[section]\ninner = "y"\n'))
    assert debug.seen_is_configs is True  # not a proxy under DEBUG
    assert isinstance(debug.seen_member, _Configurations)  # get_member(..., exclude=) worked
    assert debug.seen_member.get("inner") == "y"  # exclude= filtered correctly, content intact
