# -*- coding: utf-8 -*-
"""
lori.connectors.sql.columns.surrogate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lori.connectors.sql.columns import Column, ColumnType


# noinspection PyShadowingBuiltins
class SurrogateKeyColumn(Column):
    inherit_cache: bool = True

    def __init__(
        self,
        name: str,
        type: ColumnType,
        attribute: str,
        nullable: bool = False,
        primary_key: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(name, type, nullable=nullable, primary_key=primary_key, **kwargs)
        self.attribute = attribute
