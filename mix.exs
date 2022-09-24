defmodule ClickhouseLogger.MixProject do
  use Mix.Project

  def project do
    [
      app: :clickhouse_logger,
      name: "ClickhouseLogger",
      source_url: "https://github.com/twips-me/clickhouse_logger",
      homepage_url: "https://hex.pm/packages/clickhouse_logger",
      version: "0.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      description: description(),
      package: package(),
    ]
  end

  def application do
    [
      extra_applications: ~w[logger]a,
    ]
  end

  defp deps do
    [
      # HTTP client
      {:finch, "~> 0.13"},

      # code climate
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.1", only: [:dev], runtime: false},
      {:ex_doc, "~> 0.27", only: :dev, runtime: false},
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"],
    ]
  end

  defp description do
    """
    The Elixir Logger backend that sends logs to Clickhouse server.
    """
  end

  defp package do
    [
      name: "clickhouse_logger",
      files: ~w[lib .formatter.exs mix.exs README* LICENSE*],
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/twips-me/clickhouse_logger"},
    ]
  end
end
