# -*- coding: utf-8 -*-
"""
lori.components.tariff.static
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import pandas as pd
from lori import Channel, Configurations, Constant
from lori.components.tariff import Tariff, TariffProvider, register_tariff_type
from lori.connectors import DummyConnector


# noinspection SpellCheckingInspection
@register_tariff_type("static")
class StaticProvider(TariffProvider):
    PRICE_STATIC = Constant(float, "tariff_static", name="Static Tariff Price", unit="ct/kWh")

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        static_connector = DummyConnector(
            self, 
            key="static_connnector", 
            name="Static Tariff Connector", 
            configs=configs
        )

        self.connectors.add(static_connector)
        self.data.add(
            StaticProvider.PRICE_STATIC,
            aggregate="mean",
            connector=static_connector.id,
            logger={"enabled": False},
        )

    def activate(self) -> None:
        super().activate()
        self.data.register(self._on_tariff_received, StaticProvider.PRICE_STATIC, unique=False)

    def _on_tariff_received(self, data: pd.DataFrame) -> None:
        timestamp = data.index[0]
        import_data = data[StaticProvider.PRICE_STATIC]
        import_channel: Channel = self.data.get(Tariff.PRICE_IMPORT)
        import_channel.set(timestamp, import_data)
