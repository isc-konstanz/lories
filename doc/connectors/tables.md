# HDF5 Tables

Connector type: `tables`, `hdfstore`

Binary file-based database connector using the HDF5 format via the pandas
[HDFStore](https://pandas.pydata.org/docs/reference/api/pandas.HDFStore.html) (PyTables backend).
HDF5 supports fast columnar reads, on-disk querying, and optional compression. However, HDF5
files are not human-readable, concurrent write access is limited, and the format can be
sensitive to library version mismatches.

## How it works

On **connect**, the connector opens (or creates) an HDF5 file at the configured path. Channels
are grouped by their `group` resource parameter; each group maps to a top-level key in the HDF5
store.

On **read**, data is selected from the store using optional time-range filtering
(`start` / `end`). On **write**, data is appended to the store in PyTables `table` format,
which supports subsequent on-disk queries.

On **disconnect**, the store file is closed.

## Dependencies

```
pip install tables             # PyTables (HDF5 backend for pandas)
```

## Configuration

### Store location

| Key | Type | Default | Description |
|---|---|---|---|
| `path` | str | `data_dir` | Base path for the store file (absolute or relative to `data_dir`) |
| `file` | str | `".store.h5"` | HDF5 filename |
| `mode` | str | `"a"` | File open mode: `a` (append), `r` (read), `r+` (read/write), `w` (overwrite) |

### Storage options

| Key | Type | Default | Description |
|---|---|---|---|
| `columns_unique` | bool | `false` | Use full channel IDs as column names (instead of short keys) |
| `compression_level` | int | -- | Compression level 0--9 (higher = slower but smaller) |
| `compression_lib` | str | -- | Compression library: `zlib`, `lzo`, `bzip2`, or `blosc` |

## Channel configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `group` | str | *(channel group)* | HDF5 group key for organizing channels in the store |

## Example configuration

```toml
[connectors.store]
type              = "tables"
file              = "data.h5"
compression_level = 5
compression_lib   = "zlib"

[data.channels]
connector = "store"

[data.channels.temperature]
type  = "float"
group = "sensors"

[data.channels.humidity]
type  = "float"
group = "sensors"
```
