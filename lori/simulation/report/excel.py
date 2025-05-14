# -*- coding: utf-8 -*-
"""
lori.simulation.report.pdf
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import pandas as pd

from lori.core import CONSTANTS, Configurations
from lori.data.util import resample
from lori.simulation import Results
from lori.simulation.report import Report, register_report_type
from lori.io import excel
from lori.util import parse_freq


@register_report_type("excel", "xlsx")
class ExcelReport(Report):
    _freq: str
    _include: bool

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        data = configs.get_section("data")
        self._include = data.get_bool("include", default=True)
        self._freq = parse_freq(data.get("freq", default=None))

    # noinspection PyProtectedMember
    def write(self, results: Results) -> None:
        excel_file = str(results.dirs.data.joinpath("results.xlsx"))
        excel.write(excel_file, "Results", results.to_frame())

        if self._include:
            columns = {c.key: c.full_name(unit=True) for c in CONSTANTS}
            columns.update({r.get("column", default=r.key): r.full_name(unit=True) for r in results._resources})

            if self._freq is not None:
                resampled = []
                for method, resources in results._resources.groupby("aggregate"):
                    resample_columns = [r.get("column", default=r.key) for r in resources]
                    resample_columns = [c for c in resample_columns if c in results.data.columns]
                    if len(resample_columns) == 0:
                        continue
                    if method is None:
                        self._logger.warning(
                            "Skipping resources for missing aggregate function: "
                            + ", ".join(f"'{r.id}'" for r in resources)
                        )
                        continue
                    resampled.append(resample(results.data[resample_columns], self._freq, method))

                if len(resampled) == 0:
                    data = pd.DataFrame()
                else:
                    data = pd.concat(resampled, axis="columns")[results.data.columns]
                    data.rename(inplace=True, columns=columns)
            else:
                data = results.data.rename(columns=columns)
            excel.write(excel_file, "Timeseries", data)
