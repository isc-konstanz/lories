# -*- coding: utf-8 -*-
"""
lori.components.tariff.provider
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


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
