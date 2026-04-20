# Math

Connector type: `math`

Derived-channel connector that evaluates symbolic mathematical expressions at runtime using the
[SymPy](https://www.sympy.org/) computer algebra library. Free symbols in an expression are
mapped to live channel values, enabling computed channels such as unit conversions, aggregations,
or physical formulas.

```{note}
This connector is **read-only** -- writing to math channels is not supported.
```

## How it works

On **connect**, each channel's `expression` is parsed and simplified by SymPy. Free symbols are
resolved to other channels, either by name or through an explicit `mapping`. If the channel sets
a `listen` target, a callback is registered so the expression is re-evaluated automatically
whenever the target channel receives new data.

On **read**, the connector evaluates each expression by substituting the current values of the
mapped channels and returning the result.

Symbols are resolved as follows:
1. If a `mapping` entry exists for the symbol name, the mapped channel ID is used.
2. Otherwise the symbol name is treated as a sibling channel key in the same component.

## Dependencies

```
pip install sympy              # symbolic mathematics library
```

## Configuration

### Connector-level

| Key | Type | Default | Description |
|---|---|---|---|
| `mapping` | table | -- | Global symbol-to-channel-ID mapping, applied to all channels |

## Channel configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `expression` | str | *(required)* | Math expression in SymPy syntax (e.g. `a * 1000 + b`) |
| `mapping` | table | -- | Per-channel symbol-to-channel-ID overrides |
| `listen` | str | -- | Channel ID whose updates trigger re-evaluation |
| `listener` | bool | *(auto)* | Register as listener; defaults to `true` when `listen` is set |

## Example configuration

```toml
[connectors.calc]
type = "math"

[connectors.calc.mapping]
t = "sensors.temperature"

[data.channels]
connector = "calc"

[data.channels.temp_fahrenheit]
type       = "float"
expression = "t * 9/5 + 32"
listen     = "sensors.temperature"

[data.channels.power]
type       = "float"
expression = "voltage * current"
mapping    = { voltage = "meter.voltage", current = "meter.current" }
```
