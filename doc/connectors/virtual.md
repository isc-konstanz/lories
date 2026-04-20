# Virtual

Connector type: `virtual`, `random`, `dummy`

In-memory connector for testing, simulation, and soft-sensor prototyping. It supports simple
value storage with read/write semantics and optional random-walk value generation within
configurable bounds. Since all state is held in memory, data does not persist across restarts.

## How it works

On **connect**, each channel is initialised according to its `generator` setting:

- **`random`**: a random value between `min` and `max` is generated. On each subsequent read,
  the value performs a random walk (small random step clamped to the configured range).
- **`virtual`** (or no generator): the channel stores a single value, initialised from `default`.
  Values can be updated via write.

On **read**, random channels are stepped and virtual channels return their stored value.

On **write**, the latest value is stored (virtual) or clamped to bounds (random).

## Configuration

This connector has no connector-level parameters.

## Channel configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `generator` | str | -- | Generator type: `random` for random-walk values, `virtual` for simple store |
| `min` | float | -- | Minimum value (required when `generator = "random"`) |
| `max` | float | -- | Maximum value (required when `generator = "random"`) |
| `default` | float | -- | Initial stored value (used when no generator is set) |

## Example configuration

```toml
[connectors.test]
type = "virtual"

[data.channels]
connector = "test"

[data.channels.random_temp]
type      = "float"
generator = "random"
min       = 15.0
max       = 35.0

[data.channels.setpoint]
type    = "float"
default = 21.0
```
