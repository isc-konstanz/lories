# ENTSO-E

Connector type: `entsoe`

Connector for the [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/) REST API,
providing access to European electricity market data such as day-ahead prices, generation
forecasts, and cross-border flows. The API delivers standardised data across all EU member
states. Rate limits, occasional data gaps, and historical changes in bidding zone codes
(e.g. the DE/AT/LU split in 2018) require careful handling on the client side.

```{note}
This connector is **read-only** — writing data back to ENTSO-E is not supported.
```

## How it works

On **connect**, the connector creates an `EntsoePandasClient` with the configured API key.

On **read**, channels are grouped by their `method` resource parameter (currently only
`day_ahead` is supported). For each group the corresponding query is executed via the
[entsoe-py](https://github.com/EnergieID/entsoe-py) library, and results are returned as a
pandas DataFrame.

For German market data, the connector automatically selects the correct bidding zone code
(`DE_AT_LU` before 2019, `DE_LU` from 2019 onward) based on the requested date range.

## Dependencies

```
pip install entsoe-py          # ENTSO-E Transparency Platform client
```

## Configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `country_code` | str | *(required)* | Country or bidding zone code (e.g. `DE`, `AT`, `FR`) |
| `api_key` | str | *(required)* | ENTSO-E API security token ([register here](https://transparency.entsoe.eu/)) |
| `resolution` | str | `"60min"` | Data resolution: `15min`, `30min`, or `60min` |

## Channel configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `method` | str | *(required)* | Query method. Currently supported: `day_ahead` |

## Example configuration

```toml
[connectors.entsoe]
type         = "entsoe"
country_code = "DE"
api_key      = "your-api-token"
resolution   = "60min"

[data.channels]
connector = "entsoe"

[data.channels.price]
type   = "float"
method = "day_ahead"
```
