# -*- coding: utf-8 -*-
"""
lori.connectors.influx
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

from influxdb_client import BucketRetentionRules, InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
from urllib3.exceptions import HTTPError, NewConnectionError

import pandas as pd
from lori.connectors import ConnectionException, Database, DatabaseException, register_connector_type
from lori.core import ConfigurationException, Configurations, Resource, Resources
from lori.data.util import hash_value
from lori.typing import TimestampType

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


@register_connector_type("influx", "influxdb")
class InfluxDatabase(Database):
    host: str
    port: int

    org: str
    bucket: str
    token: str

    ssl: bool
    ssl_verify: bool
    timeout: int

    _client: Optional[InfluxDBClient] = None

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        vars = super()._get_vars()
        if self.is_configured():
            vars["host"] = self.host
            vars["port"] = self.port
            vars["org"] = self.org
            vars["bucket"] = self.bucket
            vars["token"] = f"...{self.token[-12:]}"
            vars["ssl"] = self.ssl_verify
            vars["ssl_verify"] = self.ssl_verify
            vars["timeout"] = self.timeout
        return vars

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self.host = configs.get("host", default="localhost")
        self.port = configs.get_int("port", default=8086)

        self.org = configs.get("org")
        if self.org is None:
            raise ConfigurationException("Missing 'org' for InfluxDB connector")

        self.bucket = configs.get("bucket")
        if self.bucket is None:
            raise ConfigurationException("Missing 'bucket' for InfluxDB connector")

        self.token = configs.get("token")
        if self.token is None:
            raise ConfigurationException("Missing 'token' for InfluxDB connector")

        # In seconds
        self.timeout = int(configs.get_float("timeout", default=10) * 1000)

        # TODO: Determine SSL usage from certificate availability
        self.ssl = configs.get_bool("ssl", default=False)
        self.ssl_verify = configs.get_bool("ssl_verify", default=True)

    def connect(self, resources: Resources) -> None:
        self._logger.debug(f"Connecting to InfluxDB ({self.host}:{self.port}) at {self.bucket}")

        ssl = {
            "verify_ssl": self.ssl_verify,
        }
        url = f"{'https' if self.ssl else 'http'}://{self.host}:{self.port}"

        self._client = InfluxDBClient(
            url=url,
            org=self.org,
            token=self.token,
            timeout=self.timeout,
            debug=self._logger.getEffectiveLevel() <= logging.DEBUG,
            **ssl,
        )

        # Check if bucket exists and create
        buckets_api = self._client.buckets_api()
        try:
            existing_buckets = [b.name for b in buckets_api.find_buckets().buckets]
            if self.bucket not in existing_buckets:
                self._logger.info(f"InfluxDB Bucket doesn't exist. Creating Bucket {self.bucket}")
                # TODO: Configure retention from channel attributes
                retention = BucketRetentionRules(every_seconds=0)  # 0 means infinite retention
                buckets_api.create_bucket(bucket_name=self.bucket, retention_rules=retention)
        except (ApiException, InfluxDBError, HTTPError) as e:
            self._raise(e)

    def disconnect(self) -> None:
        # TODO: Check if "and ping" is necessary
        if self._client is not None and self._client.ping():
            self._client.close()
            self._client = None

    def hash(
        self,
        resources: Resources,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
        method: Literal["MD5", "SHA1", "SHA256", "SHA512"] = "MD5",
        encoding: str = "UTF-8",
    ) -> Optional[str]:
        if method.lower() not in ["md5", "sha1", "sha256"]:
            raise ValueError(f"Invalid checksum method '{method}'")
        hashes = []

        imports = """
        import "types" // = [string, bytes, int, uint, !(float), bool, time, duration, regexp]
        import "array"
        import "strings"
        import "math"
        import "contrib/qxip/hash"
        """

        # functions = f"""
        # roundDigits = (n, dig) =>
        #     string(v: math.round(x: float(v: n)
        #     * float(v: math.pow10(n: dig)))
        #     / float(v: math.pow10(n: dig)))
        #
        # formatValue = (n, dig) =>
        #     if exists n then
        #         if types.isType(v: n, type: "float") then
        #             roundDigits(n: n, dig: dig) + ","
        #             // if strings.containsStr(substr: ".", v: roundDigits(n: n, dig: dig)) then
        #             //     roundDigits(n: n, dig: dig) + ","
        #             // else
        #             //     roundDigits(n: n, dig: dig) + ".000" + ","
        #         else
        #             string(v: n) + ","
        #     else
        #         ""
        # """
        functions = """
        formatValue = (n) =>
            if exists n then
                string(v: n) + ","
            else
                ""
        """

        query_api = self._client.query_api()
        for measurement, measurement_resources in resources.groupby(lambda r: r.get("measurement", default=r.group)):
            for tag, tagged_resources in measurement_resources.groupby("tag"):
                # decimals = 3
                # concat = "            + ".join(
                #     f"formatValue(n: r.{_get_field(r)}, dig: {r.get('decimals', default=decimals)})"
                #     for r in tagged_resources
                # )
                concat = " + ".join(f"formatValue(n: r.{_get_field(r)})" for r in tagged_resources)

                query = f"""
                {imports}

                {functions}

                {self._build_query(tagged_resources, measurement, tag, *_to_isoformat(start, end))}
                    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                    |> map(fn: (r) => ({{
                        r with
                        row_strings: string(v: int(v: r._time) / 1000000000) + ","
                            + {concat}
                        }}))
                    |> reduce(identity: {{column_string: ""}},
                        fn: (r, accumulator) => ({{
                            column_string: accumulator.column_string + r.row_strings
                        }}))
                    |> map(fn: (r) => ({{
                        r with
                        hash: hash.{method.lower()}(v: r.column_string)
                        // hash: strings.substring(v: r.column_string, start: 0, end: 300)
                        }}))
                    |> keep(columns: ["hash"])
                """
                try:
                    result = query_api.query_data_frame(query)
                    if result.empty:
                        continue
                    hashes.append(result["hash"].iloc[0])

                except (ApiException, InfluxDBError, HTTPError) as e:
                    self._raise(e)

        if len(hashes) == 0:
            return None
        elif len(hashes) == 1:
            return hashes[0]
        else:
            return hash_value(",".join(hashes), method, encoding)

    def exists(
        self,
        resources: Resources,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> bool:
        query_api = self._client.query_api()
        for measurement, measurement_resources in resources.groupby(lambda r: r.get("measurement", default=r.group)):
            for tag, tagged_resources in measurement_resources.groupby("tag"):
                fields = [_get_field(r) for r in tagged_resources]
                query = f"""
                {self._build_query(tagged_resources, measurement, tag, *_to_isoformat(start, end))}
                    |> keep(columns: ["_field"])
                    |> distinct(column: "_field")
                """
                try:
                    data = query_api.query_data_frame(query, data_frame_index=["_time"])
                    if sorted(data["_fields"]) != sorted(fields):
                        return False

                except (ApiException, InfluxDBError, HTTPError) as e:
                    self._raise(e)
        return True

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def read(
        self,
        resources: Resources,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> pd.DataFrame:
        return self._read(resources, start, end)

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def read_first(self, resources: Resources) -> pd.DataFrame:
        first = self._read_boundaries(resources, "first")
        return first

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def read_last(self, resources: Resources) -> pd.DataFrame:
        last = self._read_boundaries(resources, "last")
        return last

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def _read_boundaries(self, resources: Resources, mode: Literal["first", "last"]) -> pd.DataFrame:
        if mode not in ["first", "last"]:
            raise ValueError(f"Invalid mode '{mode}'")
        results = []

        query_api = self._client.query_api()
        for measurement, measurement_resources in resources.groupby(lambda r: r.get("measurement", default=r.group)):
            for tag, tagged_resources in measurement_resources.groupby("tag"):
                query = f"""
                {self._build_query(tagged_resources, measurement, tag, *_to_isoformat(None, None))}
                    |> group(columns: ["_field"])
                    |> {mode}(column: "_time")
                    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                """
                try:
                    data = query_api.query_data_frame(query, data_frame_index=["_time"])
                    if not data.empty:
                        results.append(
                            data.rename(
                                columns={_get_field(r): r.id for r in tagged_resources},
                            )
                        )

                except (ApiException, InfluxDBError, HTTPError) as e:
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
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> pd.DataFrame:
        results = []

        query_api = self._client.query_api()
        for measurement, measurement_resources in resources.groupby(lambda r: r.get("measurement", default=r.group)):
            for tag, tagged_resources in measurement_resources.groupby("tag"):
                query = f"""
                    {self._build_query(tagged_resources, measurement, tag, *_to_isoformat(start, end))}
                    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                """
                try:
                    data = query_api.query_data_frame(query, data_frame_index=["_time"])

                    if not data.empty:
                        results.append(
                            data.rename(
                                columns={_get_field(r): r.id for r in tagged_resources},
                            )
                        )

                except (ApiException, InfluxDBError, HTTPError) as e:
                    self._raise(e)

        if len(results) == 0:
            return pd.DataFrame(columns=[r.id for r in resources])

        results = pd.concat(results, axis="columns")
        results.sort_index(inplace=True)
        results = results.loc[:, [r.id for r in resources if r.id in results.columns]]
        return results

    # noinspection PyTypeChecker
    def write(self, data: pd.DataFrame) -> None:
        write_api = self._client.write_api(write_options=SYNCHRONOUS)
        for measurement, group_resources in self.resources.groupby(lambda r: r.get("measurement", default=r.group)):
            if measurement is None:
                measurement = "None"
            for tag, tagged_resources in group_resources.groupby("tag"):
                tagged_data = data.loc[:, [r.id for r in tagged_resources if r.id in data.columns]]
                tagged_data = tagged_data.dropna(axis="index", how="all").dropna(axis="columns", how="all")
                if tagged_data.empty:
                    continue

                if tag is not None:
                    tagged_data["tag"] = tag

                try:
                    tagged_data = tagged_data.rename(columns={r.id: _get_field(r) for r in tagged_resources})
                    write_api.write(
                        bucket=self.bucket,
                        record=tagged_data,
                        data_frame_measurement_name=measurement,
                        data_frame_tag_columns=["tag"] if tag is not None else None,
                    )
                    write_api.flush()

                except (ApiException, InfluxDBError, HTTPError) as e:
                    self._raise(e)

    def delete(
        self,
        resources: Resources,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> None:
        start, end = _to_isoformat(start, end)
        delete_api = self._client.delete_api()
        for measurement, group_resources in resources.groupby(lambda r: r.get("measurement", default=r.group)):
            for tag, tagged_resources in group_resources.groupby("tag"):
                # Escaped because "tag" is a keyword
                predicate = f'_measurement = "{measurement}"'
                predicate += f' AND "tag" = "{tag}"' if tag is not None else ""

                try:
                    delete_api.delete(
                        start=start,
                        stop=end,
                        predicate=predicate,
                        bucket=self.bucket,
                        org=self.org,
                    )
                except (ApiException, InfluxDBError, HTTPError) as e:
                    self._raise(e)

    def _build_query(
        self,
        resources: Resources,
        measurement: str,
        tag: Optional[str],
        start: str = "0",
        end: str = "now()",
    ) -> str:
        if measurement is None:
            raise DatabaseException(self, f"Measurement is None for following resources: {resources}")
        query = f"""
            from(bucket: "{self.bucket}")
                |> range(start: {start}, stop: {end})
                |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                |> filter(fn: (r) => {" or ".join([f'r["_field"] == "{_get_field(r)}"' for r in resources])})
            """
        if tag is not None:
            query += f"""
                |> filter(fn: (r) => r["tag"] == "{tag}")
            """
        return query

    # noinspection PyProtectedMember
    def is_connected(self) -> bool:
        if self._client is None:
            return False
        try:
            return self._client.ping()
        except (ApiException, InfluxDBError, HTTPError) as e:
            self._raise(e)
        return False

    def _raise(self, e: Exception):
        if isinstance(e, ApiException):
            raise DatabaseException(self, str(e))
        elif isinstance(e, InfluxDBError):
            raise DatabaseException(self, str(e))
        elif isinstance(e, NewConnectionError):
            raise DatabaseException(self, str(e))
        else:
            raise ConnectionException(self, str(e))


# noinspection SpellCheckingInspection
def _to_isoformat(
    start: Optional[TimestampType] = None,
    end: Optional[TimestampType] = None,
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
