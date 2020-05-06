defmodule Membrane.HTTPAdaptiveStream.MixProject do
  use Mix.Project

  @version "0.1.0"
  @github_url "https://github.com/membraneframework/membrane_http_adaptive_stream"

  def project do
    [
      app: :membrane_http_adaptive_stream,
      version: @version,
      elixir: "~> 1.7",
      elixirc_paths: elixirc_paths(Mix.env()),
      description: "Membrane HTTPAdaptiveStream plugin",
      package: package(),
      name: "Membrane HTTPAdaptiveStream plugin",
      source_url: @github_url,
      docs: docs(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: []
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp docs do
    [
      main: "readme",
      extras: ["README.md"],
      source_ref: "v#{@version}",
      nest_modules_by_prefix: [Membrane.HTTPAdaptiveStream],
      groups_for_modules: [
        Elements: [~r/^Membrane.HTTPAdaptiveStream.Sink/],
        HLS: [~r/^Membrane.HTTPAdaptiveStream.HLS/],
        Playlist: [~r/^Membrane.HTTPAdaptiveStream.Playlist/],
        Storages: [~r/^Membrane.HTTPAdaptiveStream.Storage/]
      ]
    ]
  end

  defp package do
    [
      maintainers: ["Membrane Team"],
      licenses: ["Apache 2.0"],
      links: %{
        "GitHub" => @github_url,
        "Membrane Framework Homepage" => "https://membraneframework.org"
      }
    ]
  end

  defp deps do
    [
      {:membrane_core, "~> 0.5.0"},
      {:membrane_caps_http_adaptive_stream,
       git: "git@github.com:membraneframework/membrane-caps-http-adaptive-stream"},
      {:fe, "~> 0.1.3"},
      {:ex_doc, "~> 0.21", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0.0-rc.7", only: [:dev, :test], runtime: false}
    ]
  end
end
