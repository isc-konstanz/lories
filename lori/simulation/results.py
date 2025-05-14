# -*- coding: utf-8 -*-
"""
lori.simulation.results
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from functools import reduce
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence

import pandas as pd
from lori.components import Component
from lori.connectors import Database
from lori.core import Configurations, Configurator, Constant, Directories, ResourceException, Resources
from lori.data.util import scale_energy, scale_power
from lori.simulation import Durations, Progress, Result
from lori.typing import TimestampType


class Results(Configurator, Sequence[Result]):
    INCLUDES: List[str] = ["report"]

    __list: List[Result]

    __database: Database
    __component: Component
    _resources: Resources

    data: pd.DataFrame

    dirs: Directories

    durations: Durations
    progress: Progress

    def __init__(
        self,
        component: Component,
        database: Database,
        configs: Configurations,
        desc: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(configs)
        self.__list = []
        self.__database = self._assert_database(database)
        self.__component = self._assert_component(component)
        self._resources = self._extract_resources(component)

        self.dirs = component.configs.dirs
        if not self.dirs.data.exists():
            os.makedirs(self.dirs.data, exist_ok=True)
        self.dirs.tmp = self.dirs.data.joinpath(".results")
        if not self.dirs.tmp.exists():
            os.makedirs(self.dirs.tmp, exist_ok=True)

        if desc is None:
            desc = component.name
        self.progress = Progress(
            desc=desc,
            file=str(self.dirs.data.joinpath("results.json")),
            **kwargs,
        )
        self.durations = Durations(self.dirs.tmp)
        self.data = pd.DataFrame()

    @classmethod
    def _assert_database(cls, database: Database) -> Database:
        if database is None or not isinstance(database, Database):
            raise ResourceException(f"Invalid '{cls.__name__}' database: {type(database)}")
        return database

    @classmethod
    def _assert_component(cls, component: Component) -> Component:
        if component is None or not isinstance(component, Component):
            raise ResourceException(f"Invalid '{cls.__name__}' component: {type(component)}")
        return component

    @staticmethod
    def _extract_resources(component: Component) -> Resources:
        resources = []

        def extend_resources(__component: Component) -> None:
            resources.extend(__component.data.channels)
            for _component in __component.components.values():
                extend_resources(_component)

        extend_resources(component)
        return Resources(resources)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(str(r.key) for r in self.__list)})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{r.key} = {repr(r)}" for r in self.__list)

    def __contains__(self, result: str | Result) -> bool:
        if isinstance(result, str):
            return any(result == r.key for r in self.__list)
        return result in self.__list

    def __getitem__(self, index: Iterable[str] | str | int):
        if isinstance(index, str):
            for result in self.__list:
                if result.key == index:
                    return result
        if isinstance(index, Iterable):
            return [r for r in self.__list if r.key == index]
        raise KeyError(index)

    def __iter__(self) -> Iterator[Result]:
        return iter(self.__list)

    def __len__(self) -> int:
        return len(self.__list)

    def __add__(self, other):
        return [*self, *other]

    def add(
        self,
        key: str | Constant,
        name: str,
        summary: Any,
        header: str = "Summary",
        **kwargs: Any,
    ) -> None:
        self.__list.append(Result(key, name, summary, header=header, **kwargs))

    def append(self, result: Result) -> None:
        self.__list.append(result)

    def extend(self, results: Iterable[Result]) -> None:
        self.__list.extend(results)

    def __enter__(self, **kwargs) -> Results:
        configs = self.configs
        self.configure(configs)
        self.open()
        return self

    # noinspection PyShadowingBuiltins
    def __exit__(self, type, value, traceback):
        self.close()

    def open(self) -> None:
        self.__database.connect(self._resources)

    def close(self) -> None:
        self.__database.disconnect()
        self.durations.stop()
        self.progress.close()

    @property
    def key(self) -> str:
        return self.__component.key

    @property
    def name(self) -> str:
        return self.__component.name

    @property
    def start(self) -> Optional[pd.Timestamp]:
        if len(self.data.index) == 0:
            return None
        return self.data.index[0]

    @property
    def end(self) -> Optional[pd.Timestamp]:
        if len(self.data.index) < 2:
            return None
        return self.data.index[-1]

    # noinspection PyShadowingBuiltins
    def filter(self, filter: Callable[[Result], bool]) -> Sequence[Result]:
        return [result for result in self.__list if filter(result)]

    def report(self) -> None:
        self.durations.complete()
        self.progress.complete(**self.to_dict())
        try:
            from lori.simulation import Report

            report = Report(self.configs.get_section("report"))
            report.write(self)

        except ImportError as e:
            self._logger.warning(f"Failed to write report: {e}")

    # noinspection PyTypeChecker
    def submit(
        self,
        function: Callable[..., pd.DataFrame],
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
        *args,
        **kwargs,
    ) -> None:
        columns = {r.get("column", default=r.key): r.id for r in self._resources}

        if self.__database.exists(self._resources, start, end):
            data = self.__database.read(self._resources, start, end)
            data.rename(columns={v: k for k, v in columns.items()}, inplace=True)
        else:
            data = function(start, end, *args, **kwargs)
            self.__database.write(data.rename(columns=columns))

        self.data = pd.concat([self.data, data], axis="index")

    def to_dict(self) -> Dict[str, Any]:
        return {r.key: r.summary for r in self.__list}

    def to_frame(self) -> pd.DataFrame:
        summary = pd.DataFrame(columns=pd.MultiIndex.from_tuples((), names=["System", ""]))
        order = list(dict.fromkeys(r.order for r in self.__list))
        order.sort()
        for result in reduce(lambda r1, r2: r1+r2, [self.filter(lambda r: r.order == o) for o in order]):
            name = result.name
            value = result.summary
            if re.search(r".*\[.*kWh.*]", name):
                name, value = scale_energy(name, value)
            elif re.search(r".*\[.*W.*]", name):
                name, value = scale_power(name, value)
            summary.loc[self.name, (result.header, name)] = value
        return summary
