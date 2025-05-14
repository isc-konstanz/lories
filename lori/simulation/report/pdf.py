# -*- coding: utf-8 -*-
"""
lori.simulation.report.pdf
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import os

from lori.core import Configurations
from lori.simulation import Results
from lori.simulation.report import Report, ReportException, register_report_type
from lori.io.pdf import PdfWriter


@register_report_type("pdf")
class PdfReport(PdfWriter, Report):
    title: str
    project: str
    author: str

    def __init__(self, configs: Configurations) -> None:
        super().__init__(configs, "report.pdf")

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        for key in ['title', "project", 'author']:
            if key not in configs:
                raise ReportException(f"Missing PDF report configuration '{key}'")

        # TODO: Introduce default title based on report template and project on results.name
        self.title = configs.get("title")
        self.project = configs.get("project")
        self.author = configs.get("author")

    # noinspection PyShadowingBuiltins
    def write(self, results: Results, open: bool = False) -> None:
        for file in os.scandir(results.dirs.tmp):
            if not os.path.isfile(file.path):
                continue
            extension = os.path.splitext(file.name)[1]
            if extension in [".svg", ".jpg", ".jpeg", ".png"]:
                self._images[file.name.replace(extension, "")] = file.path

        self.add_cover(self.title, self.project, self.author)
        self.add_table_of_content()
        self.add_results(results)
        self.save(open)

    def add_results(self, results: Results) -> None:
        # TODO: Scan and add from generic markdown pages
        pass
