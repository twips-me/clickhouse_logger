defmodule ClickhouseLoggerTest do
  use ExUnit.Case
  doctest ClickhouseLogger

  test "greets the world" do
    assert ClickhouseLogger.hello() == :world
  end
end
