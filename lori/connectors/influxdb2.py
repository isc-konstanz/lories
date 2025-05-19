# -*- coding: utf-8 -*-
"""
lori.connectors.influx.database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import time
from typing import Any, Dict, Iterator, Mapping, Optional

import numpy as np
from influxdb_client import InfluxDBClient, BucketRetentionRules
from influxdb_client.client.write_api import SYNCHRONOUS

import pandas as pd
import pytz as tz
from lori.connectors import ConnectionException, Database, DatabaseException, register_connector_type
from lori.core import ConfigurationException, Configurations, Resources
from lori.data.util import hash_value
from lori.util import to_timezone

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


@register_connector_type("influxdb2", "influx", "influxdb")
class InfluxDB2_Database(Database, Mapping[str, np.ndarray]):
    HASH_METHODS = ["md5", "sha1", "sha256"]

    url: str
    bucket: str
    token: str
    org: str
    timeout: int
    verify_ssl: bool

    client: None | InfluxDBClient = None

    @property
    def connection(self) -> InfluxDBClient:
        if not self.is_connected():
            raise ConnectionException(self, "InfluxDB connection not open")
        return self.client
    # noinspection PyShadowingBuiltins, PyProtectedMember
    def _get_vars(self) -> Dict[str, Any]:
        vars = super()._get_vars()
        # vars.pop("_schema", None)
        if self.is_configured():
            vars["url"] = self.url
            vars["bucket"] = self.bucket
            vars["token"] = f"...{self.token[-12:]}"
            vars["org"] = self.org
            vars["timeout"] = self.timeout
            vars["verify_ssl"] = self.verify_ssl
        # vars["tables"] = f"[{', '.join(c.name for c in self.__tables.keys())}]"
        return vars

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self.url = configs.get("url")
        self.bucket = configs.get("bucket")
        self.token = configs.get("token")

        self.org = configs.get("org", default="org")
        self.timeout = configs.get_int("timeout", default=10_000)  # ms
        self.verify_ssl = configs.get_bool("verify_ssl", default=True)

    def connect(self, resources: Resources) -> None:
        self._logger.debug(f"Connecting to Influx ({self.url}) at {self.bucket}")

        # self.client = InfluxDBClient(url=f"http://{self.host}:{self.port}", token=self.token, org=self.org)
        self.client = InfluxDBClient(
            url=self.url,
            token=self.token,
            org=self.org,
            timeout=self.timeout,
            verify_ssl=self.verify_ssl,

            # debug=False,
            # enable_gzip=False
            # Here could be ssl and certification...
        )

        # Check if bucket exists and create
        buckets_api = self.client.buckets_api()
        existing_buckets = [b.name for b in buckets_api.find_buckets().buckets]
        if self.bucket not in existing_buckets:
            self._logger.info(f"InfluxDB Bucket doesn't exist. Creating Bucket {self.bucket}")
            retention = BucketRetentionRules(every_seconds=0)  # 0 means infinite retention
            buckets_api.create_bucket(bucket_name=self.bucket, retention_rules=retention, org=self.org)

    def disconnect(self) -> None:
        if self.client is not None:
            self.client.close()
            self.client = None
            self._logger.debug("Disconnected from the database")

    # TODO: fix hash
    def hash(
            self,
            resources: Resources,
            start: Optional[pd.Timestamp | dt.datetime] = None,
            end: Optional[pd.Timestamp | dt.datetime] = None,
            method: Literal["MD5", "SHA1", "SHA256", "SHA512"] = "MD5",
            encoding: str = "UTF-8",
    ) -> Optional[str]:

        end = end + dt.timedelta(seconds=1)
        start_str = start.isoformat(sep='T', timespec='seconds') if start is not None else "0"
        end_str = end.isoformat(sep='T', timespec='seconds') if end is not None else "now()"

        hashes = []
        # TODO: Add parameter
        digits = 3

        hash_method = method.lower()
        if hash_method not in InfluxDB2_Database.HASH_METHODS:
            raise ValueError(f"Invalid checksum method '{method}'")
        try:

            query_api = self.client.query_api()
            for measurement_name, field_resources in resources.groupby("table"):
                rename = {r.id: r.get("field", default=r.get("column", default=r.key))
                          for r in field_resources if "name" in r}

                field_filters = ", ".join([f'"{field}"' for field in rename.values()])

                concat_fields = "\n\t\t+ ".join(
                    [f'equalFormat(n: r.{field}, dig: {digits})' for field in rename.values()])

                query = f"""
                    import "types" // = [string, bytes, int, uint, !(float), bool, time, duration, regexp]
                    import "array"
                    import "strings"
                    import "math"
                    import "contrib/qxip/hash"

                    columns = [{field_filters}]

                    roundDigits = (n, dig) =>
                        string(v: math.round(x: float(v: n)
                        * float(v: math.pow10(n: dig)))
                        / float(v: math.pow10(n: dig)))

                    equalFormat = (n, dig) =>
                        if exists n then
                            if types.isType(v: n, type: "float") then
                                roundDigits(n: n, dig: dig) + ","
                                // if strings.containsStr(substr: ".", v: roundDigits(n: n, dig: dig)) then
                                //     roundDigits(n: n, dig: dig) + ","
                                // else
                                //     roundDigits(n: n, dig: dig) + ".000" + ","
                            else
                                string(v: n)
                        else
                            ""

                    from(bucket: "{self.bucket}")
                        |> range(start: {start_str}, stop: {end_str})
                        |> filter(fn: (r) => r["_measurement"] == "{measurement_name}")
                        |> filter(fn: (r) => contains(set: columns, value: r["_field"]))
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> map(fn: (r) => ({{
                            r with
                            row_strings: string(v: int(v: r._time) / 1000000000) + ","
                                + {concat_fields}
                            }}))
                        |> reduce(identity: {{column_string: ""}},
                            fn: (r, accumulator) => ({{
                                column_string: accumulator.column_string + r.row_strings
                            }}))
                        |> map(fn: (r) => ({{
                            r with
                            hash: r.column_string
                            //hash: strings.substring(v: r.column_string, start: 0, end: 50)
                            //hash: hash.{hash_method}(v: r.column_string)
                            }}))    
                    """

                #print(query)
                result = query_api.query_data_frame(query, org=self.org)
                if result.empty:
                    continue
                hashes.append(result["hash"].iloc[0])

        except Exception as e:
            self._raise(e)

        if len(hashes) == 0:
            hashes_joined = None
        elif len(hashes) == 1:
            hashes_joined = hashes[0]
        else:
            hashes_joined = ",".join(hashes)

        normal_hash = super().hash(resources, start, end, method, encoding)
        influx_hash = hashes_joined
        if normal_hash != influx_hash:
            self._logger.info("Hashes not equal")
            self._logger.info(f"\033[92m{normal_hash}\033[0m")
            self._logger.info(f"\033[94m{influx_hash}\033[0m")

            return normal_hash

        return hashes_joined  # hash_value(hashes_joined, method, encoding)

    def exists(
            self,
            resources: Resources,
            start: Optional[pd.Timestamp | dt.datetime] = None,
            end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> bool:
        # TODO: Test (was never called?)
        end = end + dt.timedelta(seconds=1)
        start_str = start.isoformat(sep='T', timespec='seconds') if start is not None else "0"
        end_str = end.isoformat(sep='T', timespec='seconds') if end is not None else "now()"
        try:

            query_api = self.client.query_api()
            for measurement_name, field_resources in resources.groupby(self._measurement_lambda()):
                for tag, tagged_field_resources in field_resources.groupby("tag"):
                    rename = {r.id: r.get("field", default=r.get("column", default=r.key))
                              for r in tagged_field_resources if "name" in r}

                    field_filters = ", ".join([f'"{field}"' for field in rename.values()])
                    tag_filter = f'|> filter(fn: (r) => r["_measurement"] == "{tag}")' if tag is not None else ""

                    query = f"""
                        columns = [{field_filters}]

                        from(bucket: "{self.bucket}")
                        |> range(start: {start_str}, stop: {end_str})'
                        |> filter(fn: (r) => r["_measurement"] == "{measurement_name}")
                        |> filter(fn: (r) => contains(set: columns, value: r["_field"]))
                        {tag_filter}
                        |> keep(columns: ["_field"]) 
                        |> distinct(column: "_field")  // Get unique field names
                    """
                    result = query_api.query_data_frame(query, org=self.org, data_frame_index=['_time'])

                    fetched_fields = result["_fields"]

                    if not sorted(rename.values()) == sorted(fetched_fields):
                        return False

                    # TODO: Check


        except Exception as e:
            self._raise(e)
        return True

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def read(
            self,
            resources: Resources,
            start: Optional[pd.Timestamp | dt.datetime] = None,
            end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> pd.DataFrame:
        # TODO: check again? already done previously to include next full minute? < vs <=
        end = end + dt.timedelta(seconds=1)
        start_str = start.isoformat(sep='T', timespec='seconds') if start is not None else "0"
        end_str = end.isoformat(sep='T', timespec='seconds') if end is not None else "now()"

        results = []
        query_api = self.client.query_api()

        for measurement_name, field_resources in self.resources.groupby(self._measurement_lambda()):
            for tag, tagged_field_resources in field_resources.groupby("tag"):
                rename = {r.id: r.get("field", default=r.get("column", default=r.key))
                          for r in tagged_field_resources if "name" in r}

                field_filters = ", ".join([f'"{field}"' for field in rename.values()])
                tag_filter = f'|> filter(fn: (r) => r["_measurement"] == "{tag}")' if tag is not None else ""

                query = f"""
                    columns = [{field_filters}]
                    from(bucket: "{self.bucket}")
                    |> range(start: {start_str}, stop: {end_str})
                    |> filter(fn: (r) => r["_measurement"] == "{measurement_name}")
                    |> filter(fn: (r) => contains(set: columns, value: r["_field"]))
                    {tag_filter}
                    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                """
                try:
                    result = query_api.query_data_frame(query, org=self.org, data_frame_index=['_time'])
                except Exception as e:
                    self._raise(e)
                    raise ConnectionException(self, str(e))

                # Append missing columns with NaN values
                for col in [c for c in rename.values() if c not in result.columns]:
                    result[col] = pd.NA

                # Keep only ressources
                result = result[list(rename.values())]

                # Rename
                rename_inv = {v: k for k, v in rename.items()}
                result = result.rename(columns=rename_inv)

                if len(result) > 0:
                    results.append(result)

        if len(results) == 0:
            return pd.DataFrame(columns=[r.id for r in resources])
            # TODO: always returun all resources?

        results = sorted(results, key=lambda d: min(d.index))
        return pd.concat(results, axis="columns")

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def read_first(self, resources: Resources) -> pd.DataFrame:
        return self._get_first_last_df(resources, "first")

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def read_last(self, resources: Resources) -> pd.DataFrame:
        return self._get_first_last_df(resources, "last")

    # noinspection PyTypeChecker
    def write(self, data: pd.DataFrame) -> None:
        try:
            write_api = self.client.write_api(write_options=SYNCHRONOUS)

            for measurement_name, field_resources in self.resources.groupby(self._measurement_lambda()):
                for tag, tagged_field_resources in field_resources.groupby("tag"):
                    measurement_data = data.loc[:, [r.id for r in tagged_field_resources if r.id in data.columns]]
                    measurement_data = measurement_data.dropna(axis="index", how="all")
                    measurement_data = measurement_data.dropna(axis=1, how='all')
                    if measurement_data.empty:
                        continue

                    rename = {r.id: r.get("field", default=r.get("column", default=r.key))
                              for r in tagged_field_resources if "name" in r}
                    measurement_data = measurement_data.rename(columns=rename)

                    if measurement_name is None:
                        measurement_name = "None"
                        pass

                    # TODO: turn all non strings to float?
                    write_api.write(
                        org=self.org,
                        bucket=self.bucket,
                        record=measurement_data,
                        data_frame_measurement_name=measurement_name,
                        data_frame_tag_columns=tag
                    )
                    write_api.flush()

        except Exception as e:
            self._raise(e)

    def delete(
            self,
            resources: Resources,
            start: Optional[pd.Timestamp | dt.datetime] = None,
            end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> None:
        # TODO: Test
        start_str = start.isoformat(sep='T', timespec='seconds') if start is not None else "0"
        end_str = end.isoformat(sep='T', timespec='seconds') if end is not None else "now()"
        try:
            query_api = self.client.query_api()

            for measurement_name, field_resources in self.resources.groupby(self._measurement_lambda()):
                for tag, tagged_field_resources in field_resources.groupby("tag"):
                    rename = {r.id: r.get("field", default=r.get("column", default=r.key))
                              for r in tagged_field_resources if "name" in r}

                    field_filters = ", ".join([f'"{field}"' for field in rename.values()])
                    tag_filter = f'|> filter(fn: (r) => r["_measurement"] == "{tag}")' if tag is not None else ""

                    query = f"""
                        columns = [{field_filters}]

                        from(bucket: "{self.bucket}")
                        |> range(start: {start_str}, stop: {end_str})
                        |> filter(fn: (r) => r["_measurement"] == "{measurement_name}")
                        |> filter(fn: (r) => contains(set: columns, value: r["_field"]))
                        {tag_filter}
                        |> drop(columns: columns)  // Remove specific columns
                        |> to(bucket: "{self.bucket}")
                    """
                    # print(query)
                    # result = query_api.query_data_frame(query, org=self.org, data_frame_index=['_time'])
                    query_api.query(query, org=self.org)
                    pass


        except Exception as e:
            self._raise(e)

    # noinspection PyProtectedMember
    def is_connected(self) -> bool:
        if self.client is None:
            return False
        self._connected = self.client.ping()
        return self._connected

    def _raise(self, e: Exception):
        if "syntax" in str(e).lower():
            raise DatabaseException(self, str(e))
        else:
            raise ConnectionException(self, str(e))

    def _get_first_last_df(self, resources: Resources, mode: str):
        # TODO: Test
        results = []

        # TODO better switch case
        if mode not in ["first", "last"]:
            raise ValueError(f"Invalid mode '{mode}'")

        query_api = self.client.query_api()
        for measurement_name, field_resources in self.resources.groupby("table"):
            rename = {r.id: r.get("field", default=r.get("column", default=r.key))
                      for r in field_resources if "name" in r}

            field_filters = ", ".join([f'"{field}"' for field in rename.values()])

            query_last_timestamp = f"""
                    from(bucket: "{self.bucket}")
                    |> range(start: 0, stop: now())
                    |> filter(fn: (r) => r["_measurement"] == "{measurement_name}")
                    |> keep(columns: ["_time"])
                    //|> sort(columns: ["_time"], desc: false)
                    |> {mode}(column: "_time")
                    """

            try:
                timestamp_result = query_api.query(query_last_timestamp)
            except Exception as e:
                raise ConnectionException(self, str(e))

            if not timestamp_result:
                continue

            timestamp_column = None
            for i, col in enumerate(timestamp_result[0].columns):
                if col.label == "_time":
                    timestamp_column = i
                    break

            if timestamp_column is None:
                continue

            last_timestamp = timestamp_result[0].records[0].row[timestamp_column]

            start_time = last_timestamp
            stop_time = last_timestamp + dt.timedelta(seconds=1)

            start_time_str = start_time.isoformat(sep='T', timespec='seconds')
            stop_time_str = stop_time.isoformat(sep='T', timespec='seconds')

            query = f"""
                columns = [{field_filters}]

                from(bucket: "{self.bucket}")
                |> range(start: {start_time_str}, stop: {stop_time_str})
                |> filter(fn: (r) => r["_measurement"] == "{measurement_name}")
                |> filter(fn: (r) => contains(set: columns, value: r["_field"]))
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            """

            result = query_api.query_data_frame(query, org=self.org, data_frame_index=['_time'])

            # create empty if result is empty
            if result.empty:
                continue

            # Append missing columns with NaN values
            for col in [c for c in rename.values() if c not in result.columns]:
                result[col] = np.nan

            # Keep only
            result = result[list(rename.values())]

            # Rename
            rename_inv = {v: k for k, v in rename.items()}
            result = result.rename(columns=rename_inv)

            if len(result) > 0:
                results.append(result)

        if len(results) == 0:
            return pd.DataFrame(columns=[r.id for r in resources])

        return pd.concat(results, axis="columns")

    @staticmethod
    def _measurement_lambda():
        return lambda r: r.get("measurement", default=r.get("table", default=r.group))




