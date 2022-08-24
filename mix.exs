defmodule Membrane.HTTPAdaptiveStream.MixProject do
  use Mix.Project

  @version "0.8.1"
  @github_url "https://github.com/membraneframework/membrane_http_adaptive_stream_plugin"

  def project do
    [
      app: :membrane_http_adaptive_stream_plugin,
      version: @version,
      elixir: "~> 1.12",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: dialyzer(),

      # hex
      description: "Membrane plugin for adaptive streaming over HTTP",
      package: package(),

      # docs
      name: "Membrane HTTP Adaptive Stream plugin",
      homepage_url: "https://membraneframework.org",
      source_url: @github_url,
      docs: docs()
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
      extras: ["README.md", "LICENSE"],
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
      licenses: ["Apache-2.0"],
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
      {:membrane_mp4_plugin,
       github: "membraneframework/membrane_mp4_plugin", branch: "partial-cmaf-segments"},
      {:credo, "~> 1.6.1", only: :dev, runtime: false},
      {:ex_doc, "~> 0.25", only: :dev, runtime: false},
      {:dialyxir, "~> 1.1", only: :dev, runtime: false},
      {:membrane_hackney_plugin, "~> 0.8.0", only: :test},
      {:membrane_h264_ffmpeg_plugin, "~> 0.21.0", only: :test},
      {:membrane_aac_plugin, "~> 0.12.0", only: :test}
    ]
  end

  defp dialyzer() do
    opts = [
      flags: [:error_handling]
    ]

    if System.get_env("CI") == "true" do
      # Store PLTs in cacheable directory for CI
      [plt_local_path: "priv/plts", plt_core_path: "priv/plts"] ++ opts
    else
      opts
    end
  end
end
