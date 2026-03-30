# -*- coding: utf-8 -*-
"""
lories.components.tariff.entsoe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import pandas as pd
from lories import Constant
from lories.components.tariff import Tariff, TariffProvider, register_tariff_type
from lories.connectors.entsoe import _AVAILABLE, _IMPORT_ERROR, EntsoeConnector
from lories.core.configs.parameters import Parameter
from lories.typing import Configurations


# noinspection SpellCheckingInspection
@register_tariff_type("entsoe", "entso_e")
class EntsoeProvider(TariffProvider):
    """
    Tariff provider that retrieves day-ahead electricity prices from the ENTSO-E Transparency Platform.
    ENTSO-E publishes hourly wholesale market prices for European bidding zones, typically available by
    13:00 CET for the following day. The provider converts day-ahead prices from €/MWh to ct/kWh and
    applies a configurable offset to account for taxes, levies, or margin adjustments.
    """

    __available__ = _AVAILABLE
    __import_error__ = _IMPORT_ERROR

    PRICE_DAY_AHEAD = Constant(float, "price_day_ahead", name="Day-Ahead Tariff Price", unit="€/MWh")

    _offset = Parameter(key="offset", type=float, default=0.0, desc="Price offset added to day-ahead price in ct/kWh")

    _offset: float

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        entsoe_connector = EntsoeConnector(
            self,
            key="entsoe_connector",
            name="ENTSO-e Connector",
            configs=configs,
        )
        self.connectors.add(entsoe_connector)
        self.data.add(
            EntsoeProvider.PRICE_DAY_AHEAD,
            method=EntsoeConnector.DAY_AHEAD,
            aggregate="mean",
            connector=entsoe_connector.id,
            logger={"enabled": False},
        )

    def activate(self) -> None:
        super().activate()
        self.data.register(self._on_tariff_received, EntsoeProvider.PRICE_DAY_AHEAD, unique=False)

    def _on_tariff_received(self, data: pd.DataFrame) -> None:
        timestamp = data.index[0]
        import_data = data[EntsoeProvider.PRICE_DAY_AHEAD] / 10.0 + self._offset
        import_channel = self.data.get(Tariff.PRICE_IMPORT)
        import_channel.set(timestamp, import_data)
