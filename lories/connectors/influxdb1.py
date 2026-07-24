# -*- coding: utf-8 -*-
"""
lories.connectors.influxdb1
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import warnings
from typing import Any, Dict, Literal, Optional, Tuple

from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError, InfluxDBServerError
from urllib3.exceptions import HTTPError, NewConnectionError

import pandas as pd
from lories.connectors import ConnectionError, Database, DatabaseError, register_connector_type
from lories.core.configs.parameters import ChannelParameter, Parameter
from lories.typing import Resource, Resources, Timestamp

warnings.filterwarnings("ignore", category=DeprecationWarning, module="influxdb.*")


@register_connector_type("influxdb1", "influxdb_v1")
class InfluxDB1(Database):
    """
    InfluxDB 1.x is an open-source time-series database optimized for high-throughput ingestion and real-time
    querying of timestamped data using the InfluxQL query language. It provides built-in retention policies and
    continuous queries for automatic data management. However, InfluxDB 1.x lacks native multi-tenancy, uses a
    custom authentication model without token-based access, and its storage engine can become resource-intensive
    under high cardinality workloads.
    """

    _host = Parameter(key="host", type=str, default="localhost", desc="InfluxDB server hostname")
    _port = Parameter(key="port", type=int, default=8086, min=1, max=65535, desc="InfluxDB server TCP port")
    _user = Parameter(key="user", type=str, default="admin", desc="InfluxDB authentication username")
    _password = Parameter(key="password", type=str, default="admin", desc="InfluxDB authentication password", secret=True)
    _database = Parameter(key="database", type=str, default="lories", desc="InfluxDB database name")
    _timeout = Parameter(key="timeout", type=pd.Timedelta, default="10s", min="1s", desc="HTTP request timeout")

    # Per-channel parameters
    measurement = ChannelParameter(
        type=str, required=False, desc="InfluxDB measurement name (defaults to the channel group)"
    )
    tag = ChannelParameter(type=str, required=False, desc="Optional tag key for grouping fields within a measurement")

    host: str
    port: int

    user: str
    password: str
    database: str

    timeout: pd.Timedelta

    _client: Optional[InfluxDBClient] = None

    def connect(self, resources: Resources) -> None:
        self._logger.debug(f"Connecting to InfluxDB v1 ({self._host}:{self._port}) at {self._database}")

        self._client = InfluxDBClient(
            host=self._host,
            port=self._port,
            username=self._user,
            password=self._password,
            database=self._database,
            timeout=self._timeout.total_seconds(),
        )

        # Check if database exists and create it if not
        databases = [database["name"] for database in self._client.get_list_database()]
        if self._database not in databases:
            self._logger.info(f"Creating InfluxDB database '{self._database}'")
            self._client.create_database(self._database)

    def disconnect(self) -> None:
        # TODO: Check if "ping" is necessary
        if self._client is not None and self._client.ping():
            self._client.close()
            self._client = None

    def exists(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> bool:
        for measurement, measurement_resources in resources.groupby(lambda r: r.get("measurement", default=r.group)):
            for tag, tagged_resources in measurement_resources.groupby("tag"):
                fields = [_get_field(r) for r in tagged_resources]
                if not fields:
                    continue

                aliases = []
                for i, field in enumerate(fields):
                    alias = f"c{i}"
                    aliases.append(alias)

                select = _build_select(
                    resources=tagged_resources,
                    measurement=measurement,
                    start=start,
                    end=end,
                    tag=tag,
                )
                try:
                    result = self._client.query(select)
                    points = list(result.get_points())
                    # No points -> no data
                    if not points:
                        return False

                    row = points[0]
                    # Check every aliased count; if any is zero -> missing
                    for alias in aliases:
                        count = int(row.get(alias, 0))
                        if count == 0:
                            return False

                except (InfluxDBClientError, InfluxDBServerError, HTTPError, NewConnectionError) as e:
                    self._raise(e)
        return True

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def read(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> pd.DataFrame:
        return self._read(resources, start, end)

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def read_first(self, resources: Resources) -> pd.DataFrame:
        first = self._read_boundary(resources, "first")
        return first

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def read_last(self, resources: Resources) -> pd.DataFrame:
        last = self._read_boundary(resources, "last")
        return last

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def _read_boundary(self, resources: Resources, mode: Literal["first", "last"]) -> pd.DataFrame:
        if mode not in ["first", "last"]:
            raise ValueError(f"Invalid mode '{mode}'")
        results = []

        for measurement, measurement_resources in resources.groupby(lambda r: r.get("measurement", default=r.group)):
            if measurement is None:
                measurement = "None"

            for tag, tagged_resources in measurement_resources.groupby("tag"):
                for field in tagged_resources:
                    field_name = _get_field(field)

                    select = f'SELECT {mode}("{field_name}") AS "{field.id}", time FROM "{measurement}"'
                    if tag is not None:
                        select += f" WHERE \"tag\" = '{tag}'"
                    select += f' ORDER BY time { "ASC" if mode == "first" else "DESC" } LIMIT 1'
                    try:
                        result = self._client.query(select)
                        points = list(result.get_points())
                        if not points:
                            continue

                        data = pd.DataFrame(points)
                        if "time" in data.columns:
                            data.index = pd.to_datetime(data["time"])
                            data.drop(columns=["time"], inplace=True)
                        data.dropna(axis="index", how="all", inplace=True)

                        if not data.empty:
                            results.append(data)

                    except (InfluxDBClientError, InfluxDBServerError, HTTPError, NewConnectionError) as e:
                        self._raise(e)

        if len(results) == 0:
            return pd.DataFrame(columns=[r.id for r in resources])

        results = pd.concat(results, axis="columns")
        results.sort_index(inplace=True)
        results = results.loc[:, [r.id for r in resources if r.id in results.columns]]
        return results

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def _read(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> pd.DataFrame:
        results = []

        for measurement, measurement_resources in resources.groupby(lambda r: r.get("measurement", default=r.group)):
            if measurement is None:
                measurement = "None"

            for tag, tagged_resources in measurement_resources.groupby("tag"):
                select = _build_select(
                    resources=tagged_resources,
                    measurement=measurement,
                    start=start,
                    end=end,
                    tag=tag,
                )
                try:
                    result = self._client.query(select)
                    points = list(result.get_points())
                    if not points:
                        continue

                    data = pd.DataFrame(points)
                    if "time" in data.columns:
                        data.index = pd.to_datetime(data["time"])
                        data.drop(columns=["time"], inplace=True)
                    data.dropna(axis="index", how="all", inplace=True)

                    # Map field names to resource ids
                    data.rename(columns={_get_field(r): r.id for r in tagged_resources}, inplace=True)

                    if not data.empty:
                        results.append(data)

                except (InfluxDBClientError, InfluxDBServerError, HTTPError, NewConnectionError) as e:
                    self._raise(e)

        if len(results) == 0:
            return pd.DataFrame(columns=[r.id for r in resources])

        results = pd.concat(results, axis="columns")
        results.sort_index(inplace=True)
        results = results.loc[:, [r.id for r in resources if r.id in results.columns]]
        return results

    def write(self, data: pd.DataFrame) -> None:
        for measurement, measurement_resources in self.resources.groupby(
            lambda r: r.get("measurement", default=r.group)
        ):
            if measurement is None:
                measurement = "None"

            for tag, tagged_resources in measurement_resources.groupby("tag"):
                tagged_data = data.loc[:, [r.id for r in tagged_resources if r.id in data.columns]]
                tagged_data = tagged_data.dropna(axis="index", how="all").dropna(axis="columns", how="all")
                if tagged_data.empty:
                    continue

                points = []
                for timestamp, row in tagged_data.iterrows():
                    fields = {_get_field(r): row[r.id] for r in tagged_resources if r.id in row and pd.notna(row[r.id])}
                    if not fields:
                        continue

                    point = {
                        "measurement": measurement,
                        "time": timestamp.isoformat(),
                        "fields": fields,
                    }
                    if tag is not None:
                        point["tags"] = {"tag": tag}

                    points.append(point)
                if not points:
                    return
                try:
                    self._client.write_points(points, time_precision="s")

                except (InfluxDBClientError, InfluxDBServerError, HTTPError, NewConnectionError) as e:
                    self._raise(e)

    def delete(self, resources: Resources, start=None, end=None) -> None:
        for measurement, measurement_resources in resources.groupby(lambda r: r.get("measurement", default=r.group)):
            if measurement is None:
                measurement = "None"

            for tag, _ in measurement_resources.groupby("tag"):
                where = [f"\"_measurement\" = '{measurement}'"]  # optional
                if tag is not None:
                    where.append(f"\"tag\" = '{tag}'")

                query = f'DELETE FROM "{measurement}"'
                if where:
                    query += " WHERE " + " AND ".join(where)
                query = _append_time(query, start or "0", end or "now()")
                try:
                    self._client.query(query)

                except (InfluxDBClientError, InfluxDBServerError, HTTPError, NewConnectionError) as e:
                    self._raise(e)

    # noinspection PyProtectedMember
    def is_connected(self) -> bool:
        if self._client is None:
            return False
        try:
            return self._client.ping()
        except (InfluxDBClientError, InfluxDBServerError, HTTPError, NewConnectionError) as e:
            self._raise(e)
        return False

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        vars = super()._get_vars()
        if self.is_configured():
            vars["host"] = self._host
            vars["port"] = self._port
            vars["user"] = self._user
            vars["database"] = self._database
            vars["timeout"] = self._timeout
        return vars

    def _raise(self, e: Exception):
        if isinstance(e, (InfluxDBClientError, InfluxDBServerError)):
            raise DatabaseError(self, str(e))
        elif isinstance(e, NewConnectionError):
            raise DatabaseError(self, str(e))
        else:
            raise ConnectionError(self, str(e))


def _build_select(
    resources: Resources,
    measurement: str,
    start: Optional[Timestamp] = None,
    end: Optional[Timestamp] = None,
    tag: Optional[str] = None,
) -> str:
    fields = [_get_field(r) for r in resources]
    select_parts = [f'"{f}"' for f in fields]
    query = f'SELECT {", ".join(select_parts)} FROM "{measurement}"'
    query = _append_time(query, start, end)
    return query


def _append_time(
    query: str,
    start: Optional[Timestamp],
    end: Optional[Timestamp],
) -> str:
    parts = []
    start_iso, end_iso = _to_isoformat(start, end)
    if start is not None:
        parts.append(f"time >= '{start_iso}'")
    if end is not None:
        parts.append(f"time < '{end_iso}'")

    if parts:
        if "WHERE" in query:
            query += " AND " + " AND ".join(parts)
        else:
            query += " WHERE " + " AND ".join(parts)
    return query


# noinspection SpellCheckingInspection
def _to_isoformat(
    start: Optional[Timestamp] = None,
    end: Optional[Timestamp] = None,
) -> Tuple[str, str]:
    if start is None:
        start = "0"
    else:
        start = start.isoformat(sep="T", timespec="seconds")
    if end is None:
        end = "now()"
    else:
        end += pd.Timedelta(seconds=1)
        end = end.isoformat(sep="T", timespec="seconds")
    return start, end


def _get_field(resource: Resource) -> str:
    return resource.get("field", default=resource.get("column", default=resource.key))
