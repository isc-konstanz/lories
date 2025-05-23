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

import numpy as np
import pandas as pd
from lori.connectors import ConnectionException, Database, DatabaseException, register_connector_type
from lori.core import Configurations, Resource, Resources, ConfigurationException
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
    
    org:  str
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
            # enable_gzip=False
        )

        # Check if bucket exists and create
        buckets_api = self._client.buckets_api()
        try:
            existing_buckets = [b.name for b in buckets_api.find_buckets().buckets]
            if self.bucket not in existing_buckets:
                self._logger.info(f"InfluxDB Bucket doesn't exist. Creating Bucket {self.bucket}")
                #TODO: Configure retention from channel attributes
                retention = BucketRetentionRules(every_seconds=0)  # 0 means infinite retention
                buckets_api.create_bucket(bucket_name=self.bucket, retention_rules=retention)
        except Exception as e:
            self._raise(e)

    def disconnect(self) -> None:
        if self._client is not None and self._client.ping():
            self._client.close()
            self._client = None

    #TODO: Enable hash when util rounding is fixed
    def _hash(
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
                        }}))
                    |> keep(columns: ["hash"])
                """
                try:
                    result = query_api.query_data_frame(query)
                    if result.empty:
                        continue
                    hashes.append(result["hash"].iloc[0])

                except Exception as e:
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
        # TODO: Test this function
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

                except Exception as e:
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
        # TODO: Test this function
        if mode not in ["first", "last"]:
            raise ValueError(f"Invalid mode '{mode}'")
        results = []
   
        query_api = self._client.query_api()
        for measurement, measurement_resources in resources.groupby(lambda r: r.get("measurement", default=r.group)):
            for tag, tagged_resources in measurement_resources.groupby("tag"):
                query = f"""
                {self._build_query(tagged_resources, measurement, tag, *_to_isoformat(None, None))}
                    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                    |> {mode}(column: "_time")
                """
                try:
                    data = query_api.query_data_frame(query, data_frame_index=["_time"])
                    if not data.empty:
                        results.append(
                            data.rename(
                                columns={_get_field(r): r.id for r in tagged_resources},
                            )
                        )
   
                except Exception as e:
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

                except Exception as e:
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
                for write_type, write_resources in tagged_resources.groupby("type"):
                    if not (issubclass(write_type, (np.integer, int)) or issubclass(write_type, (np.floating, float))):
                        # raise DatabaseException(self, f"Unable to write '{write_type}' values into InfluxDB")
                        pass

                    write_data = data.loc[:, [r.id for r in write_resources if r.id in data.columns]]
                    write_data = write_data.dropna(axis="index", how="all").dropna(axis="columns", how="all")
                    if write_data.empty:
                        continue
                    try:
                        write_data = write_data.rename(columns={r.id: _get_field(r) for r in tagged_resources})
                        write_api.write(
                            bucket=self.bucket,
                            record=write_data,
                            data_frame_measurement_name=measurement,
                            data_frame_tag_columns=tag,
                        )
                        write_api.flush()

                    except Exception as e:
                        self._raise(e)

    def delete(
        self,
        resources: Resources,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> None:
        for measurement, group_resources in resources.groupby(lambda r: r.get("measurement", default=r.group)):
            for tag, tagged_resources in group_resources.groupby("tag"):

                query = f"""
                    {self._build_query(tagged_resources, measurement, tag, *_to_isoformat(start, end))} 
                    |> drop(columns: columns)
                    |> to(bucket: "{self.bucket}"
                """
                try:
                    self._client.query_api().query(query)

                except Exception as e:
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
            raise DatabaseException(f"Measurement is None for following resources: {resources}")
        columns = ", ".join([f'"{_get_field(r)}"' for r in resources])
        query = f"""
            columns = [{columns}]

            from(bucket: "{self.bucket}")
                |> range(start: {start}, stop: {end})
                |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                |> filter(fn: (r) => contains(set: columns, value: r["_field"]))
            """
        # TODO: Tag names can be arbitrarily configured and customized
        # To simplify: we are using a string so each resource can only have a string as tag
        if tag is not None:
            query += f"""
                |> filter(fn: (r) => r["_tag"] == "{tag}")
            """
        return query


    # noinspection PyProtectedMember
    def is_connected(self) -> bool:
        if self._client is None:
            return False
        try:
            return self._client.ping()
        except Exception as e:
            self._raise(e)

    def _raise(self, e: Exception):
        #TODO: Differentiate reasons (InfluxDBError is all?)

        print("\033[92m" + str(e) + "\033[0m")
        
        if isinstance(e, InfluxDBError):
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
