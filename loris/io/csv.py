# -*- coding: utf-8 -*-
"""
loris.io.csv
~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import glob
import os
from typing import List, Mapping, Optional

import pandas as pd
import pytz as tz
from loris.connectors import ConnectionException
from loris.util import ceil_date, floor_date, to_date, to_timedelta


# noinspection PyShadowingBuiltins
def has_range(
    path: str,
    freq: str,
    format: str,
    start: pd.Timestamp | dt.datetime | str,
    end: pd.Timestamp | dt.datetime | str,
    timezone: tz.tzinfo = tz.UTC,
):
    files = get_files(path, freq, format, start, end, timezone)
    return all([os.path.isfile(f) for f in files])


# noinspection PyShadowingBuiltins
def read_files(
    path: str,
    freq: str,
    format: str,
    start: Optional[pd.Timestamp, dt.datetime, str],
    end: Optional[pd.Timestamp, dt.datetime, str],
    timezone: Optional[tz.tzinfo] = None,
    **kwargs,
) -> pd.DataFrame:
    data = pd.DataFrame()
    start = to_date(start, timezone)
    end = to_date(end, timezone)
    end = ceil_date(end, timezone)  # TODO: Check if ceiling this is unproblematic

    for file in get_files(path, freq, format, start, end, timezone):
        if not data.empty and (end is not None and data.index[-1] > end):
            break
        if not os.path.isfile(file):
            raise IOError("File not found: " + file)

        data = read_file(file, timezone=timezone, **kwargs).combine_first(data)

    if data.empty:
        return data
    if pd.isna(start):
        data = data[data.index >= start]
    if pd.isna(end):
        data = data[data.index <= end]
    return data


def read_file(
    path: str,
    index_column: str = "Timestamp",
    index_type: str = "Timestamp",
    timezone: Optional[tz.tzinfo] = None,
    separator: str = ",",
    decimal: str = ".",
    rename: Optional[Mapping[str, str]] = None,
    encoding: str = "utf-8-sig",
) -> pd.DataFrame:
    """
    Reads the content of a specified CSV file.

    :param path:
        the full path to the CSV file.
    :type path:
        string

    :param index_column:
        the column name of the CSV file index.
    :type index_column:
        string

    :param index_type:
        the index type, either "Timestamp", "UNIX" oder "None".
    :type index_type:
        string

    :param separator:
        the separator character of the CSV file.
    :type separator:
        string

    :param decimal:
        the decimal character used for the CSV file.
    :type decimal:
        string

    :param timezone:
        the timezone for the timestamp to be converted or localized to.
    :type timezone:
        :class: `pytz.tzinfo`

    :param rename:
        the dictionary to rename columns by after reading, if not None.
    :type rename:
        dict

    :param encoding:
        the encoding to read_file the file with.
    :type encoding:
        string


    :returns:
        the retrieved columns, indexed by their timestamp
    :rtype:
        :class:`pandas.DataFrame`
    """
    data = pd.read_csv(path, sep=separator, decimal=decimal, encoding=encoding)
    if not data.empty:
        if index_column not in data.columns:
            if index_column.islower():
                index_column = index_column.title()
            else:
                index_column = index_column.lower()

        if index_type.lower() in ["timestamp", "unix"]:
            if index_type.lower() == "timestamp":
                data[index_column] = pd.to_datetime(data[index_column], unit="ms")
            elif index_type.lower() == "unix":
                data[index_column] = pd.to_datetime(data[index_column])  # , utc=True)
            else:
                raise ValueError(f"Unknown index type: {index_type}")

            data.set_index(index_column, verify_integrity=True, inplace=True)

            if not hasattr(data.index, "tzinfo"):
                data[index_column] = data.index
                data[index_column] = data[index_column].apply(lambda t: t.astimezone(tz.UTC).replace(tzinfo=None))
                data.set_index(index_column, verify_integrity=True, inplace=True)
                data.index = data.index.tz_localize(tz.UTC)

            if timezone is not None:
                if hasattr(data.index, "tzinfo") and data.index.tzinfo is not None:
                    if data.index.tzinfo != timezone:
                        data.index = data.index.tz_convert(timezone)
                else:
                    data.index = data.index.tz_localize(timezone, ambiguous="infer")

        elif index_type is None or index_type.lower() == "none":
            # Prepare the index name, to be renamed below
            data.index.name = "index"
        else:
            raise ValueError(f"Unknown index type: {index_type}")

    if rename:
        data = data.rename(columns=rename)
        data.index.name = data.index.name.lower()
    else:
        data.index.name = data.index.name.title()
    return data


# noinspection PyShadowingBuiltins
def write_files(
    data: pd.DataFrame,
    path: str,
    freq: str,
    format: str,
    timezone: Optional[tz.tzinfo] = None,
    **kwargs
) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

    index_name = data.index.name
    if index_name is None:
        index_name = "Timestamp"
    if data.index.tzinfo is None or data.index.tzinfo.utcoffset(data.index) is None:
        data.index = data.index.tz_localize(tz.UTC, ambiguous="infer")
    if timezone is None:
        timezone = data.index.tzinfo
    elif data.index.tzinfo != timezone:
        data.index = data.index.tz_convert(timezone)

    time_step = floor_date(data.index[0], freq=freq)

    def next_step() -> pd.Timestamp:
        return floor_date(time_step + to_timedelta(freq), timezone=timezone, freq=freq)

    while time_step < data.index[-1]:
        time_next = next_step()

        file = time_step.strftime(format) + ".csv"
        file_path = os.path.join(path, file)
        file_data = data[(data.index >= time_step) & (data.index < time_next)].copy()
        file_data.index.name = index_name

        write_file(file_data, file_path, timezone=timezone, **kwargs)

        time_step = time_next


def write_file(
    data: pd.DataFrame,
    path: str,
    timezone: Optional[tz.tzinfo] = None,
    separator: str = ",",
    decimal: str = ".",
    rename: Optional[Mapping[str, str]] = None,
    override: bool = False,
    encoding: str = "utf-8-sig",
):
    if data.index.tzinfo is None or data.index.tzinfo.utcoffset(data.index) is None:
        data.index = data.index.tz_localize(timezone, ambiguous="infer")
    if timezone is None:
        timezone = data.index.tzinfo
    elif data.index.tzinfo != timezone:
        data.index = data.index.tz_convert(timezone)

    if not override and os.path.isfile(path):
        index = data.index.name
        csv = read_file(
            path,
            index_column=index,
            timezone=timezone,
            separator=separator,
            decimal=decimal,
            rename={column: name for name, column in rename.items()} if not None else None,
            encoding=encoding,
        )

        if not csv.empty:
            if all(name in list(csv.columns) for name in list(data.columns)):
                data = data.combine_first(csv)
            else:
                data = pd.concat([csv, data], axis="index")

    if rename:
        data = data.rename(columns=rename)
        data.index.name = data.index.name.title()
    else:
        data.index.name = data.index.name.lower()

    data.to_csv(path, sep=separator, decimal=decimal, encoding=encoding)


# noinspection PyShadowingBuiltins
def get_files(
    path: str,
    freq: str,
    format: str,
    start: pd.Timestamp | dt.datetime | str,
    end: pd.Timestamp | dt.datetime | str,
    timezone: tz.tzinfo = tz.UTC,
) -> List[str]:
    end = to_date(end, timezone)
    start = to_date(start, timezone)
    if start is None or end is None:
        filenames = [os.path.basename(f) for f in glob.glob(os.path.join(path, "*.csv"))]
        if len(filenames) > 0:
            filenames.sort()
            if start is None and end is None:
                return [os.path.join(path, f) for f in filenames]

            if start is None:
                start_str = filenames[0].replace(".csv", "")
                start = to_date(start_str, timezone=timezone, format=format)

                if end is None:
                    end_str = filenames[-1].replace(".csv", "")
                    end = to_date(end_str, timezone=timezone, format=format)
                    end = ceil_date(end, timezone=timezone, freq=freq)

    date = floor_date(start, timezone=timezone, freq=freq)

    # noinspection PyShadowingNames
    def next_date() -> pd.Timestamp:
        next_date = floor_date(date + to_timedelta(freq), timezone=timezone, freq=freq)
        if next_date == date:
            next_date += to_timedelta(freq)
            next_offset = date.utcoffset() - next_date.utcoffset()
            if next_offset.seconds > 0:
                next_date = floor_date(next_date + next_offset, timezone=timezone, freq=freq)
            else:
                ConnectionException(f"Unable to increment date for freq '{freq}'")
        return next_date

    file = date.strftime(format) + ".csv"
    file_path = os.path.join(path, file)
    files = [file_path]
    if end is not None:
        date = next_date()
        while date <= end:
            file = date.strftime(format) + ".csv"
            file_path = os.path.join(path, file)
            files.append(file_path)
            date = next_date()

    return files
