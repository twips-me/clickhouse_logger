defmodule ClickhouseLogger do
  @moduledoc """
  The Elixir [Logger](https://hexdocs.pm/logger/Logger.html) backend that sends logs to
  [clickhouse](https://clickhouse.com) server.

  ## Configuration example

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

  ## Common configuration options

  ### database

  The Clickhouse database to write logs to. Default value: `"default"`.

  ### table

  The Clickhouse table to write logs to. Default value: `"logs"`.

  ### fields (required)

  A keyword list of logged fields. The keys is a Clickhouse table field names. The values can be one of:

  - `:timestamp`, `:message` atoms to log appropriate log fields.
  - `{:meta, field_name, type}` to log appropriate field from metadata. Currently supported types: `:string`,
    `:uint8/16/32/64`, `:int8/16/32/64` and `{:array, type}`.
  - `{CustomModule, :custom_func, type}` to custom encode logged value. `CustomModule.custom_func/4` will
    be called with `level`, `timestamp`, `message` and `metadata` arguments and should return logged value.

  ### buffer_size

  The number of message that will be buffered in memory before sending to Clickhouse. Default value: `1_000`.

  ### buffer_timeout

  The number of milliseconds after which all buffered messages will be sent to Clickhouse even if they
  not exceeds `buffer_size` value. Default value: `3_000`.

  ### buffer_limit

  The maximum number of messages in memory buffer. This option needed for cases when Clickhouse server
  is not available for some reasons. If this limit is reached futher log messages will be dropped.
  Default value: `10_000`.

  ## HTTP client configuration options

  ### base_uri (required)

  the HTTP endpoint of Clickhouse server URL.
  """

  require Logger

  defstruct [
    level: nil,
    meta: [],
    buffer: [],
    buffer_length: 0,
    buffer_size: nil,
    buffer_timeout: nil,
    buffer_limit: nil,
    client: nil,
    client_state: nil,
    fields: nil,
    timer: nil,
  ]

  @behaviour :gen_event

  @type level :: :debug | :info | :warn | :error | atom
  @type message :: iodata
  @type timestamp :: {
    {non_neg_integer, non_neg_integer, non_neg_integer},
    {non_neg_integer, non_neg_integer, non_neg_integer, non_neg_integer}
  }
  @type metadata :: keyword

  @default_config [
    buffer_size: 1_000,
    buffer_timeout: 3_000,
    buffer_limit: 10_000,
  ]

  @unix_seconds ~N[1970-01-01 00:00:00] |> NaiveDateTime.to_gregorian_seconds() |> elem(0)

  @impl true
  def init(__MODULE__) do
    config = Keyword.merge(@default_config, Application.get_env(:logger, __MODULE__, []))
    level = Keyword.get(config, :level, Application.get_env(:logger, :level, :debug))
    client = Keyword.get(config, :client, ClickhouseLogger.HttpClient)
    st = %__MODULE__{level: level, client: client}
    with {:ok, client_state} <- client.init(config),
         {:ok, st} <- configure(config, %{st | client_state: client_state}) do
      {:ok, st}
    else
      {:error, error} ->
        Logger.error(
          "ClickhouseLogger: Initialization error: #{inspect error}",
          ignore_clickhouse_backend: true
        )
        {:error, :ignore}
    end
  end

  @impl true
  def handle_call({:configure, config}, st) do
    with {:ok, st} <- configure(config, st) do
      {:ok, :ok, restart_timer(st)}
    end
  end

  @impl true
  def handle_event({_level, gl, _event}, st) when node(gl) != node() do
    {:ok, st}
  end
  def handle_event({level, _gl, {Logger, msg, ts, meta}}, %{level: min_level, meta: app_meta} = st) do
    ignore_clickhouse_backend = Keyword.get(meta, :ignore_clickhouse_backend, false)
    if not ignore_clickhouse_backend and right_log_level?(min_level, level) do
      level
      |> encode_message(ts, msg, Keyword.merge(app_meta, meta), st)
      |> enqueue_message(st)
    else
      {:ok, st}
    end
  rescue
    error ->
      Logger.error(
        "ClickhouseLogger: Logging failed: #{inspect error}",
        ignore_clickhouse_backend: true
      )
      {:ok, st}
  end
  def handle_event(:flush, st) do
    {:ok, st |> restart_timer() |> send_messages()}
  end
  def handle_event(_evt, st) do
    {:ok, st}
  end

  @impl true
  def handle_info({__MODULE__, :flush}, st) do
    {:ok, st |> restart_timer() |> send_messages()}
  end
  def handle_info(_other, st) do
    {:ok, st}
  end

  @impl true
  def terminate(_args, st) do
    send_messages(st)
    :ok
  end

  defp configure(config, %{fields: prev_fields, buffer_timeout: prev_timeout} = st) do
    st
    |> parse_meta(Keyword.get(config, :metadata))
    |> parse_buffer_size(Keyword.get(config, :buffer_size))
    |> parse_buffer_limit(Keyword.get(config, :buffer_limit))
    |> parse_buffer_timeout(Keyword.get(config, :buffer_timeout))
    |> parse_fields(Keyword.get(config, :fields))
    |> case do
      {:ok, %{fields: ^prev_fields} = st} ->
        configure_client(config, st)

      {:ok, %{fields: [_ | _] = fields} = st} ->
        client_fields = Enum.map(fields, fn {field, _path, type} -> {field, type} end)
        configure_client(Keyword.put(config, :fields, client_fields), st)

      other ->
        other
    end
    |> case do
      {:ok, %{buffer_timeout: ^prev_timeout} = st} -> {:ok, st}
      {:ok, st} -> {:ok, restart_timer(st)}
      other -> other
    end
  end

  defp configure_client(config, %{client: client, client_state: client_state} = st) do
    case client.configure(config, client_state) do
      {:ok, client_state} -> {:ok, %{st | client_state: client_state}}
      {:error, error} -> {:error, {:client_configuration, error}}
    end
  end

  defp right_log_level?(nil, _level), do: true
  defp right_log_level?(min_level, level), do: Logger.compare_levels(level, min_level) != :lt

  defp parse_meta(st, nil), do: {:ok, st}
  defp parse_meta(st, meta) when is_list(meta) do
    if Keyword.keyword?(meta) do
      {:ok, %{st | meta: meta}}
    else
      {:error, :invalid_meta}
    end
  end
  defp parse_meta(_st, _meta), do: {:error, :invalid_meta}

  defp parse_buffer_size({:ok, st}, nil), do: {:ok, st}
  defp parse_buffer_size({:ok, st}, size) when is_integer(size) and size >= 0, do: {:ok, %{st | buffer_size: size}}
  defp parse_buffer_size({:ok, _st}, _size), do: {:error, :invalid_buffer_size}
  defp parse_buffer_size(error, _size), do: error

  defp parse_buffer_limit({:ok, st}, nil), do: {:ok, st}
  defp parse_buffer_limit({:ok, st}, limit) when is_integer(limit) and limit >= 0 do
    {:ok, %{st | buffer_limit: limit}}
  end
  defp parse_buffer_limit({:ok, _st}, _limit), do: {:error, :invalid_buffer_limit}
  defp parse_buffer_limit(error, _limit), do: error

  defp parse_buffer_timeout({:ok, st}, nil), do: {:ok, st}
  defp parse_buffer_timeout({:ok, st}, timeout) when is_integer(timeout) and timeout >= 0 do
    {:ok, %{st | buffer_timeout: timeout}}
  end
  defp parse_buffer_timeout({:ok, _st}, _timeout), do: {:error, :invalid_buffer_timeout}
  defp parse_buffer_timeout(error, _timeout), do: error

  defp parse_fields({:ok, st}, nil), do: {:ok, st}
  defp parse_fields({:ok, st}, [_ | _] = fields) do
    with {:ok, fields} <- Enum.reduce_while(fields, {:ok, []}, &parse_field/2) do
      {:ok, %{st | fields: fields}}
    end
  end
  defp parse_fields({:ok, st}, []), do: {:ok, Map.put(st, :fields, [])}
  defp parse_fields({:ok, _st}, _fields), do: {:error, :invalid_fields}
  defp parse_fields(error, _fields), do: error

  defp parse_field({field, :message}, {:ok, acc}), do: {:cont, {:ok, [{field, [:message], :string} | acc]}}
  defp parse_field({field, :timestamp}, {:ok, acc}), do: {:cont, {:ok, [{field, [:timestamp], :uint64} | acc]}}
  defp parse_field({field, {:meta, meta_field, type}}, {:ok, acc}) do
    {:cont, {:ok, [{field, [:meta, meta_field], type} | acc]}}
  end
  defp parse_field({field, {mod, fun, type}}, {:ok, acc}) when is_atom(mod) and is_atom(fun) do
    Code.ensure_compiled!(mod)
    if function_exported?(mod, fun, 4) do
      {:cont, {:ok, [{field, Function.capture(mod, fun, 4), type} | acc]}}
    else
      {:halt, {:error, :invalid_field_converter}}
    end
  end
  defp parse_field(_other, _result), do: {:halt, {:error, :invalid_field}}

  defp enqueue_message(
    msg,
    %{
      buffer: buffer,
      buffer_length: buffer_length,
      buffer_size: buffer_size,
      buffer_limit: buffer_limit,
    } = st
  ) when buffer_length < buffer_size and buffer_length < buffer_limit do
    {:ok, %{st | buffer: [msg | buffer], buffer_length: buffer_length + 1}}
  end
  defp enqueue_message(
    msg,
    %{
      buffer: buffer,
      buffer_length: buffer_length,
      buffer_limit: buffer_limit,
    } = st
  ) when buffer_length < buffer_limit do
    {:ok, send_messages(%{st | buffer: [msg | buffer], buffer_length: buffer_length + 1})}
  end
  defp enqueue_message(_msg, %{buffer_limit: buffer_limit} = st) do
    Logger.error(
      "ClickhouseLogger: buffer limit (for #{buffer_limit} messages) was overflown",
      ignore_clickhouse_backend: true
    )
    {:ok, %{st | buffer: [], buffer_length: 0}}
  end

  defp encode_message(level, timestamp, message, meta, %{fields: fields}) do
    Enum.map(fields, fn
      {_, [:timestamp], _} -> encode_timestamp(timestamp)
      {_, [:message], _} -> message
      {_, [:meta | path], type} -> encode_field(get_in(meta, path), type)
      {_, f, _} when is_function(f, 4) -> f.(level, timestamp, message, meta)
    end)
  end

  defp encode_timestamp({date, {hh, mm, ss, ms}}) do
    case NaiveDateTime.from_erl({date, {hh, mm, ss}}) do
      {:ok, timestamp} ->
        {s, _ms} = NaiveDateTime.to_gregorian_seconds(timestamp)
        (s - @unix_seconds) * 1000 + ms

      _ ->
        0
    end
  end
  defp encode_timestamp(_), do: ""

  defp encode_field(nil, :string), do: ""
  defp encode_field(nil, :uint8), do: 0
  defp encode_field(nil, :uint16), do: 0
  defp encode_field(nil, :uint32), do: 0
  defp encode_field(nil, :uint64), do: 0
  defp encode_field(nil, :int8), do: 0
  defp encode_field(nil, :int16), do: 0
  defp encode_field(nil, :int32), do: 0
  defp encode_field(nil, :int64), do: 0
  defp encode_field(nil, {:array, _subtype}), do: []
  defp encode_field(v, :string) when is_pid(v), do: inspect(v)
  defp encode_field(v, :string), do: to_string(v)
  defp encode_field(v, _), do: v

  defp send_messages(%{buffer: [_ | _] = data, client: client, client_state: client_state} = st) do
    case client.send(data, client_state) do
      {:ok, client_state} ->
        %{st | buffer: [], buffer_length: 0, client_state: client_state}

      {:error, error, client_state} ->
        Logger.error(
          "ClickhouseLogger: Error sending logs to clickhouse server: #{inspect error}",
          ignore_clickhouse_errors: true
        )
        %{st | client_state: client_state}
    end
  end
  defp send_messages(st), do: st

  defp restart_timer(%{timer: timer} = st) when not is_nil(timer) do
    Process.cancel_timer(timer, async: true, info: false)
    restart_timer(%{st | timer: nil})
  end
  defp restart_timer(%{buffer_timeout: timeout} = st) when timeout > 0 do
    %{st | timer: Process.send_after(self(), {__MODULE__, :flush}, timeout)}
  end
  defp restart_timer(st) do
    st
  end
end
