# -*- coding: utf-8 -*-
"""
lories.components.tariff.provider
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lories.components.tariff import Tariff
from lories.typing import Configurations


# noinspection SpellCheckingInspection
class TariffProvider(Tariff):
    """
    Base class for tariff providers that supply actual price data to the tariff component. A provider
    registers import and export price channels without a hardware connector, expecting subclasses to
    populate them from external sources such as energy market APIs or forecasting services.
    """

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.data.add(Tariff.PRICE_IMPORT, aggregate="mean", connector=None)
        self.data.add(Tariff.PRICE_EXPORT, aggregate="mean", connector=None)
