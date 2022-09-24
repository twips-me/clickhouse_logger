defmodule ClickhouseLogger.HttpClient do
  @moduledoc """
  HTTP 1.1 client for clickhouse server
  """

  @behaviour ClickhouseLogger.Client

  import Bitwise

  @impl true
  def init(opts) do
    finch_opts =
      opts
      |> Keyword.take(~w[size count max_idle_time conn_opts pool_max_idle_time conn_max_idle_time]a)
      |> Keyword.put(:protocol, :http1)
    with {:ok, _pid} <- Finch.start_link(name: __MODULE__, pools: %{default: finch_opts}) do
      {:ok, %{base_uri: nil, fields: nil}}
    end
  end

  @impl true
  def configure(opts, st) do
    base_uri =
      opts
      |> Keyword.fetch!(:base_uri)
      |> URI.parse()
    database = Keyword.get(opts, :database, "default")
    table = Keyword.get(opts, :table, "logs")
    {field_names, types} =
      case Keyword.get(opts, :fields) do
        [_ | _] = fields -> Enum.unzip(fields)
        _ -> {[], []}
      end
    fields = "`" <> Enum.map_join(field_names, "`,`", &to_string/1) <> "`"
    {:ok, Map.merge(st, %{base_uri: base_uri, database: database, table: table, types: types, fields: fields, field_names: field_names})}
  end

  @impl true
  def send(data, %{base_uri: base_uri, database: db, table: table, fields: fields, types: [_ | _] = types} = st) do
    query = "INSERT INTO `#{db}`.`#{table}` (#{fields}) FORMAT RowBinary"
    uri = %{base_uri | query: URI.encode_query(%{query: query})} |> URI.to_string()
    encoded =
      Stream.map(data, fn row ->
        row |> Enum.zip(types) |> Enum.map(&encode_row_binary/1)
      end)
    :post
    |> Finch.build(uri, [], {:stream, encoded})
    |> Finch.request(__MODULE__)
    |> case do
      {:ok, %Finch.Response{status: 200}} -> {:ok, st}
      {:ok, %Finch.Response{status: status, body: body}} -> {:error, {:clickhouse_error, status, body}, st}
      {:error, error} -> {:error, {:connection_error, error}, st}
    end
  end
  def send(_data, st), do: {:ok, st}

  defp encode_row_binary({v, :string}), do: [encode_leb128(IO.iodata_length(v)), v]
  defp encode_row_binary({v, :uint8}), do: <<v>>
  defp encode_row_binary({v, :uint16}), do: <<v::little-size(16)>>
  defp encode_row_binary({v, :uint32}), do: <<v::little-size(32)>>
  defp encode_row_binary({v, :uint64}), do: <<v::little-size(64)>>
  defp encode_row_binary({v, :int8}), do: <<v::signed>>
  defp encode_row_binary({v, :int16}), do: <<v::signed-little-size(16)>>
  defp encode_row_binary({v, :int32}), do: <<v::signed-little-size(32)>>
  defp encode_row_binary({v, :int64}), do: <<v::signed-little-size(64)>>
  defp encode_row_binary({v, {:array, subtype}}) do
    [encode_leb128(length(v)) | Enum.map(v, & encode_row_binary({&1, subtype}))]
  end

  defp encode_leb128(v) when v < 128, do: <<v>>
  defp encode_leb128(v), do: <<1::1, v::7, encode_leb128(v >>> 7)::binary>>
end
