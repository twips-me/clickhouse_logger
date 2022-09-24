defmodule ClickhouseLogger.Client do
  @moduledoc """
  Clickhouse transport client behaviour
  """

  @type options :: keyword
  @type state :: any
  @type error :: any
  @type payload :: list

  @callback init(options) :: {:ok, state} | {:error, atom}
  @callback configure(options, state) :: {:ok, state} | {:error, atom}
  @callback send([payload], state) :: {:ok, state} | {:error, error, state}
end
