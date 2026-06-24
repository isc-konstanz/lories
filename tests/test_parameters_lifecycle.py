# -*- coding: utf-8 -*-
"""Seam 1 — config-load lifecycle: resolve, defaults, required, groups, inheritance, to_schema (issue 01)."""
from __future__ import annotations

import pytest

from lories.core.configs.configurator import Configurator
from lories.core.configs.errors import ConfigurationError
from lories.core.configs.parameters import Parameter, ParameterGroup


class _Base(Configurator):
    host = Parameter(type=str, default="localhost", desc="Broker host")
    port = Parameter(type=int, default=1883, desc="Broker port")


class SampleConfigurator(_Base):
    token = Parameter(type=str, desc="Auth token")  # required (no default)
    tls = Parameter(type=bool, default=False)
    auth = ParameterGroup(
        required=False,
        desc="Optional auth block",
        children=[
            Parameter(key="username", type=str, default="admin"),
            Parameter(key="password", type=str, default="secret"),
        ],
    )


def test_declared_params_resolve_with_types(write_conf):
    configs = write_conf('token = "abc"\nport = 5020\ntls = true\n')
    inst = SampleConfigurator()
    inst.configure(configs)

    assert inst.host == "localhost"  # default injected (absent key)
    assert inst.port == 5020 and isinstance(inst.port, int)
    assert inst.token == "abc"
    assert inst.tls is True


def test_defaults_injected_when_absent(write_conf):
    configs = write_conf('token = "x"\n')
    inst = SampleConfigurator()
    inst.configure(configs)

    assert inst.host == "localhost"
    assert inst.port == 1883
    assert inst.tls is False
    # the default is also injected into the Configurations object itself
    assert configs.get("host") == "localhost"


def test_missing_required_raises(write_conf):
    configs = write_conf("port = 1\n")  # no 'token'
    inst = SampleConfigurator()
    with pytest.raises(ConfigurationError, match="token"):
        inst.configure(configs)


def test_parameter_group_resolves_nested(write_conf):
    configs = write_conf('token = "x"\n[auth]\nusername = "alice"\npassword = "pw"\n')
    inst = SampleConfigurator()
    inst.configure(configs)
    assert inst.auth == {"username": "alice", "password": "pw"}


def test_parameter_group_children_default(write_conf):
    # section present but a child key absent -> the child's default is used
    configs = write_conf('token = "x"\n[auth]\nusername = "bob"\n')
    inst = SampleConfigurator()
    inst.configure(configs)
    assert inst.auth == {"username": "bob", "password": "secret"}


def test_optional_group_absent_is_none(write_conf):
    configs = write_conf('token = "x"\n')
    inst = SampleConfigurator()
    inst.configure(configs)
    assert inst.auth is None


def test_metaclass_collects_across_inheritance():
    params = SampleConfigurator.__config_parameters__
    assert {"host", "port", "token", "tls", "auth"} <= set(params)
    # the base sees only its own declarations
    assert set(_Base.__config_parameters__) == {"host", "port"}
    assert "token" not in _Base.__config_parameters__


def test_to_schema_shapes():
    schema = {k: p.to_schema() for k, p in SampleConfigurator.__config_parameters__.items()}

    assert schema["port"] == {
        "name": "port",
        "key": "port",
        "required": False,
        "desc": "Broker port",
        "default": 1883,
        "has_default": True,
        "choices": None,
        "min": None,
        "max": None,
        "type": "int",
    }
    assert schema["tls"] == {
        "name": "tls",
        "key": "tls",
        "required": False,
        "desc": None,
        "default": False,
        "has_default": True,
        "choices": None,
        "type": "bool",
    }
    assert schema["token"] == {
        "name": "token",
        "key": "token",
        "required": True,
        "desc": "Auth token",
        "default": None,
        "has_default": False,
        "choices": None,
        "type": "str",
    }
    assert schema["auth"]["type"] == "group"
    assert set(schema["auth"]["children"]) == {"username", "password"}
    assert schema["auth"]["children"]["username"]["type"] == "str"


def test_connector_collects_channel_parameters():
    """ChannelParameters collect into __channel_parameters__, separate from __config_parameters__."""
    connector_mod = pytest.importorskip("lories.connectors.connector")
    from lories.core.configs.parameters.channel_parameter import ChannelParameter

    class FakeConnector(connector_mod.Connector):
        address = ChannelParameter(type=int, desc="Register address")
        function = ChannelParameter(type=str, required=False, default="holding")

    channel_params = FakeConnector.__channel_parameters__
    assert {"address", "function"} <= set(channel_params)
    assert channel_params["address"].name == "address"
    # ChannelParameter is not a _Parameter, so it stays out of __config_parameters__ (which holds inherited connect)
    assert "address" not in FakeConnector.__config_parameters__
    assert "_connect" in FakeConnector.__config_parameters__
