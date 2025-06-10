# -*- coding: utf-8 -*-
"""
lori.components.tariff.entsoe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lori import Configurations, Constant
from lori.components.tariff import TariffProvider, register_tariff_type
from lori.connectors.entsoe import EntsoeConnector


# noinspection SpellCheckingInspection
@register_tariff_type("entsoe", "entso_e")
class EntsoeProvider(TariffProvider):
    PRICE_DAY_AHEAD = Constant(float, "price_day_ahead", name="Day-Ahead Price", unit="ct/kWh")

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        entsoe_connector = EntsoeConnector(self, key="entsoe", name="ENTSO-e", configs=configs)
        self.connectors.add(entsoe_connector)

        self.data.add(
            EntsoeProvider.PRICE_DAY_AHEAD,
            method=EntsoeConnector.DAY_AHEAD,
            aggregate="mean",
            connector=entsoe_connector.id,
            logger={"enabled": False},
        )
