# -*- coding: utf-8 -*-
"""
lori.components.tariff.entsoe_dayahead
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Collection, Dict

import pandas as pd
from lori import Configurations, Constant
from lori.components.tariff import Tariff, TariffProvider, register_tariff_type
from lori.connectors.entsoe import EntsoeDayAheadConnector

CHANNELS = [
    Tariff.IMPORT,
    Tariff.EXPORT,
]

CHANNEL_ADDRESS_ALIAS = {
#     Tariff.DAYAHEAD: "day-ahead",
#     Tariff.INTRADAY: "intraday",
}
CHANNEL_AGGREGATE_ALIAS = {}



# noinspection SpellCheckingInspection
@register_tariff_type("entsoe_tariff", "entsoe_tariff")
class EntsoeDayAhead(TariffProvider):
    DAY_AHEAD = Constant(float, "day_ahead", name="Tariff", unit="ct/kWh")

    # noinspection PyUnresolvedReferences
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        connector = EntsoeDayAheadConnector(self, key="entsoe_tariff", configs=configs)
        self.connectors.add(connector)
        self.data.add(EntsoeDayAhead.DAY_AHEAD, aggregate="mean", connector=connector.id, logger={"enabled": False})


def build_channels(**custom: Any) -> Collection[Dict[str, Any]]:
    channels = []
    for channel in CHANNELS:
        configs = channel.to_dict()
        if channel.key in CHANNEL_ADDRESS_ALIAS:
            configs["address"] = CHANNEL_ADDRESS_ALIAS[channel.key]
        else:
            configs["address"] = channel.key
        if configs["type"] == str:  # noqa: E721
            configs["length"] = 32
        if channel.key in CHANNEL_AGGREGATE_ALIAS:
            configs["aggregate"] = CHANNEL_AGGREGATE_ALIAS[channel.key]
        else:
            configs["aggregate"] = "mean"
        configs.update(custom)
        channels.append(configs)
    return channels