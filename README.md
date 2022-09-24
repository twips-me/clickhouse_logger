# ClickhouseLogger

The Elixir [Logger](https://hexdocs.pm/logger/Logger.html) backend that sends logs to
[Clickhouse](https://clickhouse.com) server.

## Installation

Add `:clickhouse_logger` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:clickhouse_logger, "~> 0.1.0"}
  ]
end
```

## Usage

Add backend configuration

```elixir
# config.exs
config :logger, ClickhouseLogger,
  base_uri: "http://localhost:8123",
  database: "logs",
  fields: [
    ts: :timestamp,
    msg: :message,
    app: {:meta, :app, :string},
    file: {:meta, :file, :string},
    line: {:meta, :line, :uint32},
  ]

config :logger,
  baclends: [:console, ClickhouseLogger]
```

See `ClickhouseLogger` module documentation for configuration options.

