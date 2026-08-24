# -*- coding: utf-8 -*-
"""
lories.components.tariff.entsoe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import pandas as pd
from lories import Constant
from lories.components.tariff import Tariff, TariffProvider, register_tariff_type
from lories.connectors.entsoe import EntsoeConnector
from lories.core.configs.parameters import Parameter
from lories.typing import Configurations


# noinspection SpellCheckingInspection
@register_tariff_type("entsoe", "entso_e")
class EntsoeProvider(TariffProvider):
    """
    Tariff provider that consumes day-ahead electricity prices published on the ENTSO-E Transparency
    Platform. ENTSO-E exposes hourly wholesale market prices for European bidding zones, typically
    available by 13:00 CET for the following day. The provider converts day-ahead prices from €/MWh to
    ct/kWh and applies a configurable offset to account for taxes, levies, or margin adjustments.

    The connector is **not** created implicitly; it must be configured by the user (e.g. via a
    ``[connectors.<id>]`` section of type ``entsoe``) and referenced by id through the ``connector``
    parameter, matching how channels in the rest of the framework bind to connectors.
    """

    PRICE_DAY_AHEAD = Constant(float, "price_day_ahead", name="Day-Ahead Tariff Price", unit="€/MWh")

    _connector = Parameter(
        key="connector", type=str, required=True, desc="ID of the ENTSO-E connector to source prices from"
    )
    _offset = Parameter(key="offset", type=float, default=0.0, desc="Constant offset added to import price (ct/kWh)")

    _connector: str
    _offset: float

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self.data.add(
            EntsoeProvider.PRICE_DAY_AHEAD,
            method=EntsoeConnector.DAY_AHEAD,
            aggregate="mean",
            connector=self._connector,
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
