# -*- coding: utf-8 -*-
"""
lori.components.tariff.entsoe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import pandas as pd

from lori import Configurations, Constant, Channel
from lori.components.tariff import Tariff, TariffProvider, register_tariff_type
from lori.connectors.entsoe import EntsoeConnector


# noinspection SpellCheckingInspection
@register_tariff_type("entsoe", "entso_e")
class EntsoeProvider(TariffProvider):
    TARIFF_DAY_AHEAD = Constant(float, "tariff_day_ahead", name="Day-Ahead Tariff", unit="ct/kWh")

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        entsoe_connector = EntsoeConnector(self, key="entsoe", name="ENTSO-e", configs=configs)
        self.connectors.add(entsoe_connector)

        self.data.add(
            EntsoeProvider.TARIFF_DAY_AHEAD,
            method=entsoe_connector.DAY_AHEAD,
            aggregate="mean",
            connector=entsoe_connector.id,
            logger={"enabled": False},
        )

    def activate(self) -> None:
        # add callback to update tariff data
        self.data.register(self._on_tariff_received, [EntsoeProvider.TARIFF_DAY_AHEAD], how="all", unique=False)

    def _on_tariff_received(self, data: pd.DataFrame) -> None:
        """
        Callback to handle received tariff data.
        """
        timestamp = data.index[0]
        channel_import: Channel = self.data.get(Tariff.PRICE_IMPORT)
        channel_import.set(timestamp, data[EntsoeProvider.TARIFF_DAY_AHEAD])


