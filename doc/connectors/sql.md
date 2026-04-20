# SQL

Connector type: `sql`

Relational database connector backed by [SQLAlchemy](https://www.sqlalchemy.org/), supporting
PostgreSQL, MySQL/MariaDB, and SQLite dialects. It provides a unified interface for reading and
writing time-series data to relational databases, with per-table configuration overrides and
automatic schema introspection. SQLAlchemy's broad dialect support enables portability across
database engines, though performance characteristics and SQL feature availability vary by backend.

## How it works

On **connect**, the connector creates a SQLAlchemy engine with the configured dialect and
credentials, opens a connection, and sets the session timezone to UTC to ensure consistent
timestamp handling. It then introspects the database schema to discover or create tables matching
the configured channels.

Channels are grouped into tables by their `table` resource parameter (defaulting to the channel's
group). Within each table, channels map to columns. The connector supports **read**, **write**,
and **delete** operations, each iterating over the table groups to build and execute the
appropriate SQL statements.

On **disconnect**, the connection is closed and resources are released.

## Dependencies

```
pip install sqlalchemy        # core ORM / query builder

# Install the driver for your database:
pip install psycopg2-binary   # PostgreSQL
pip install pymysql           # MySQL / MariaDB
```

## Configuration

### Connection

| Key | Type | Default | Description |
|---|---|---|---|
| `host` | str | -- | Database hostname or IP address |
| `port` | int | -- | Database port (e.g. 5432 for PostgreSQL, 3306 for MySQL) |
| `user` | str | -- | Database username |
| `password` | str | -- | Database password |
| `database` | str | -- | Database / schema name |
| `dialect` | str | -- | Database dialect: `postgresql`, `mysql`, `sqlite`, `mssql` |

### Tables

| Key | Type | Default | Description |
|---|---|---|---|
| `tables` | table | -- | Per-table configuration overrides (column types, index settings) |

## Channel configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `schema` | str | -- | Database schema for this channel's table (if the DB supports schemas) |
| `table` | str | *(channel group)* | Table name for this channel; channels sharing the same table are stored together |

## Example configuration

```toml
[connectors.db]
type     = "sql"
dialect  = "postgresql"
host     = "localhost"
port     = 5432
user     = "lories"
password = "secret"
database = "lories_data"

[data.channels]
connector = "db"

[data.channels.temperature]
type  = "float"
table = "sensors"

[data.channels.humidity]
type  = "float"
table = "sensors"

[data.channels.energy]
type  = "float"
table = "meters"
```

In this example, `temperature` and `humidity` share the `sensors` table, while `energy` is stored
in a separate `meters` table. All three channels use the same SQL connector.
