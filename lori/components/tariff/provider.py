# -*- coding: utf-8 -*-
"""
lori.components.tariff.provider
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#TODO: Fix docstring
This module provides the :class:`lori.components.tariff.provider.TariffProvider`, used as
reference to calculate e.g. photovoltaic installations generated power. The provided
environmental data contains temperatures and horizontal solar irradiation, which can be used,
to calculate the effective irradiance on defined, tilted photovoltaic systems.

"""

from __future__ import annotations

from typing import Optional

from lori.components import Component
from lori.components.tariff import Tariff
from lori.core import Configurations, Context


# noinspection SpellCheckingInspection
class TariffProvider(Tariff):
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.data.add(Tariff.IMPORT, aggregate="mean", connector=None)
        self.data.add(Tariff.EXPORT, aggregate="mean", connector=None)
