defmodule Membrane.HTTPAdaptiveStream.MixProject do
  use Mix.Project

  @version "0.18.7"
  @github_url "https://github.com/membraneframework/membrane_http_adaptive_stream_plugin"

  def project do
    [
      app: :membrane_http_adaptive_stream_plugin,
      version: @version,
      elixir: "~> 1.13",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: dialyzer(),

      # hex
      description: "Membrane plugin for adaptive streaming over HTTP",
      package: package(),

      # docs
      name: "Membrane HTTP Adaptive Stream plugin",
      homepage_url: "https://membrane.stream",
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
        "Membrane Framework Homepage" => "https://membrane.stream"
      }
    ]
  end

  defp deps do
    [
      {:membrane_core, "~> 1.2"},
      {:membrane_tee_plugin, "~> 0.12.0"},
      {:membrane_mp4_plugin, "~> 0.35.0"},
      {:membrane_aac_plugin, "~> 0.19.0"},
      {:membrane_h26x_plugin, "~> 0.10.0"},
      {:ex_hls,
       github: "membraneframework-labs/ex_hls", ref: "bf78d233b2f0a8409c8453df8e6dcb61a4e32c29"},
      {:bunch, "~> 1.6"},
      {:qex, "~> 0.5"},
      {:membrane_hackney_plugin, "~> 0.11.0", only: :test},
      {:membrane_transcoder_plugin, "~> 0.3.2", only: :test},
      # {:membrane_transcoder_plugin, path: "../membrane_transcoder_plugin", only: :test},
      {:membrane_realtimer_plugin, "~> 0.10.1", only: :test},
      {:membrane_portaudio_plugin, "~> 0.19.2", only: :test},
      {:membrane_sdl_plugin, "~> 0.18.5", only: :test},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:dialyxir, ">= 0.0.0", only: :dev, runtime: false},
      {:credo, ">= 0.0.0", only: :dev, runtime: false}
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
