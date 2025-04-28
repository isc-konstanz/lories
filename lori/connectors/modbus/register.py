# -*- coding: utf-8 -*-
"""
lori.connectors.modbus.register
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from pymodbus.client import ModbusBaseClient

from lori.core import ConfigurationException, Resource
from lori.util import to_int

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal

FunctionType = Literal["holding_register", "input_register", "coil"]
DataType = ModbusBaseClient.DATATYPE


class ModbusRegister:
    address: int

    function: FunctionType
    type: DataType

    @classmethod
    def from_resource(cls, resource: Resource) -> ModbusRegister:
        address = to_int(resource.get("address"))
        function = resource.get("function", default="holding_register")
        return ModbusRegister(address, function, cls._build_type(resource))

    @staticmethod
    def _build_type(resource: Resource) -> DataType:
        _type = resource.get("data_type", default=None)
        if _type is not None:
            _type = _type.lower()
        else:
            _type = resource.type
        if isinstance(_type, str):
            if _type in ["bits"]:
                return DataType.BITS
            if _type in ["uint16", "uint"]:
                return DataType.UINT16
            if _type == "uint32":
                return DataType.UINT32
            if _type == "uint64":
                return DataType.UINT64
            if _type in ["int16", "int"]:
                return DataType.INT16
            if _type == "int32":
                return DataType.INT32
            if _type == "int64":
                return DataType.INT64
            if _type in ["float32", "float"]:
                return DataType.FLOAT32
            if _type == "float64":
                return DataType.FLOAT64
            if _type in ["str", "string"]:
                return DataType.STRING
        elif isinstance(_type, type):
            if issubclass(_type, int):
                return DataType.INT16
            if issubclass(_type, float):
                return DataType.FLOAT32
            if issubclass(_type, str):
                return DataType.STRING
        raise ConfigurationException(f"Invalid data type '{_type}'")

    # noinspection PyShadowingBuiltins
    def __init__(self, address: int, function: FunctionType, type: DataType) -> None:
        super().__init__()
        self.address = address

        if function not in ["holding_register", "input_register", "coil"]:
            raise ConfigurationException(f"Invalid register function '{function}'")
        self.function = function

        if not isinstance(type, DataType):
            raise ConfigurationException(f"Invalid register data type '{type}'")
        self.type = type

    @property
    def length(self) -> int:
        return self.type.value[1]
