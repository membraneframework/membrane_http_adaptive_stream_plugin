defmodule Membrane.HTTPAdaptiveStream.MixProject do
  use Mix.Project

  @version "0.5.0"
  @github_url "https://github.com/membraneframework/membrane_http_adaptive_stream_plugin"

  def project do
    [
      app: :membrane_http_adaptive_stream_plugin,
      version: @version,
      elixir: "~> 1.7",
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      # hex
      description: "Membrane plugin for adaptive streaming over HTTP",
      package: package(),
      # docs
      name: "Membrane HTTP Adaptive Stream plugin",
      source_url: @github_url,
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: []
    ]
  end

  defp elixirc_paths(:test),
    do: ["lib", "test/support"]

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
        Manifest: [~r/^Membrane.HTTPAdaptiveStream.Manifest/],
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
      {:membrane_core, "~> 0.10.0"},
      {:membrane_cmaf_format, "~> 0.6.0"},
      {:membrane_tee_plugin, "~> 0.9.0"},
      {:credo, "~> 1.6.1", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.25", only: :dev, runtime: false},
      {:dialyxir, "~> 1.1", only: :dev, runtime: false},
      # {:membrane_mp4_plugin, "~> 0.13.0"},
      {:membrane_mp4_plugin,
       github: "membraneframework/membrane_mp4_plugin", branch: "update-deps"},
      {:membrane_hackney_plugin, "~> 0.8.0", only: [:test]},
      {:membrane_h264_ffmpeg_plugin, "~> 0.20.0", only: [:test]},
      # {:membrane_h264_ffmpeg_plugin, path: "../membrane_h264_ffmpeg_plugin", only: [:test]},
      {:membrane_aac_plugin, "~> 0.12.0", only: [:test]}
    ]
  end
end
