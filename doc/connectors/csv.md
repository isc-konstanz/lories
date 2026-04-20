# CSV

Connector type: `csv`

File-based connector that reads and writes time-series data as CSV (Comma-Separated Values) files.
Its simplicity and broad tool support make it a common choice for data exchange, logging, and
archival. However, CSV lacks a standardized schema, type information, and compression, which can
lead to parsing ambiguities and inefficient storage for very large datasets.

## How it works

On **connect**, the connector creates the target directory if it does not exist. If a single `file`
is configured, it is loaded into memory immediately.

On **read**, behaviour depends on the configuration:

- **Single file mode** (`file` is set): data is read from the pre-loaded file. When no time range
  is specified, the row closest to *now* is returned.
- **Directory mode** (default): files are discovered in `dir` using the configured `freq` and
  timestamp `format`. Only files whose name matches the requested date range are read. File names
  follow the pattern `<timestamp>.csv`, e.g. `20240315.csv` for daily frequency.

On **write**, data is appended to (or overwrites) the appropriate file. When `slice = true`, the
data is split across multiple files according to `freq`; otherwise a single file is written.

Column names in the CSV can differ from channel keys via the `column` resource parameter or the
global `columns` mapping. Setting `pretty = true` uses the channel's human-readable name and unit
instead.

## Configuration

### File location

| Key | Type | Default | Description |
|---|---|---|---|
| `dir` | str | `data_dir` | Directory containing CSV files |
| `file` | str | -- | Path to a single CSV file (disables directory mode) |

### Index

| Key | Type | Default | Description |
|---|---|---|---|
| `index_column` | str | `"timestamp"` | Name of the index column in the CSV |
| `index_type` | str | `"timestamp"` | Index type: `timestamp`, `unix`, or `none` |

### File slicing

| Key | Type | Default | Description |
|---|---|---|---|
| `freq` | str | `"D"` | File frequency: `Y` (yearly), `M` (monthly), `D` (daily), or sub-daily (`h`, `min`, `s`) |
| `format` | str | *(derived)* | `strftime` pattern for file names; auto-derived from `freq` if omitted |
| `suffix` | str | -- | Optional suffix appended to the file name (before `.csv`) |
| `slice` | bool | `false` | Split written data across files according to `freq` |
| `override` | bool | `false` | Overwrite existing data on write (instead of append/merge) |

### Format

| Key | Type | Default | Description |
|---|---|---|---|
| `separator` | str | `","` | Column separator |
| `decimal` | str | `"."` | Decimal separator |
| `pretty` | bool | `false` | Use human-readable column names with units |
| `columns` | table | -- | Explicit column-name overrides (`channel_key = "Column Name"`) |

## Channel configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `column` | str | *(channel key)* | Column name in the CSV file for this channel |

## Example configuration

```toml
[connectors.csv_logger]
type = "csv"

dir       = "logs"
freq      = "D"
separator = ","
decimal   = "."

[data.channels]
connector = "csv_logger"

[data.channels.temperature]
type   = "float"
column = "temp_c"

[data.channels.humidity]
type   = "float"
column = "rh_pct"
```

### Single-file mode

```toml
[connectors.import]
type = "csv"
file = "/data/weather_station.csv"
index_column = "datetime"
separator    = ";"
decimal      = ","
```
