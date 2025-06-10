# -*- coding: utf-8 -*-
"""
lori.components.tariff.provider
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module provides the :class:`lori.components.tariff.provider.TariffProvider`, which serves
as a reference for tariff calculations. The TariffProvider supplies import and export tariff
data, enabling the calculation of energy costs and revenues for various scenarios, such as
grid consumption and feed-in from renewable energy sources.

"""

from __future__ import annotations

from lori.components.tariff import Tariff
from lori.core import Configurations


# noinspection SpellCheckingInspection
class TariffProvider(Tariff):
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.data.add(Tariff.PRICE_IMPORT, aggregate="mean", connector=None)
        self.data.add(Tariff.PRICE_EXPORT, aggregate="mean", connector=None)
