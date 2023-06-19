# Membrane HTTP Adaptive Streaming Plugin

[![Hex.pm](https://img.shields.io/hexpm/v/membrane_http_adaptive_stream_plugin.svg)](https://hex.pm/packages/membrane_http_adaptive_stream_plugin)
[![API Docs](https://img.shields.io/badge/api-docs-yellow.svg?style=flat)](https://hexdocs.pm/membrane_http_adaptive_stream_plugin/)
[![CircleCI](https://circleci.com/gh/membraneframework/membrane_http_adaptive_stream_plugin.svg?style=svg)](https://circleci.com/gh/membraneframework/membrane_http_adaptive_stream_plugin)

Plugin generating manifests for HTTP adaptive streaming protocols.
Currently, only HTTP Live Streaming (HLS) is supported.
In future, the support for MPEG-DASH is planned as well

## Installation

Add the following line to your `deps` in `mix.exs`. Run `mix deps.get`.

```elixir
	{:membrane_http_adaptive_stream_plugin, "~> 0.15.0"}
```

## Usage Example

See `test/membrane_http_adaptive_stream/integration_test/sink_bin_integration_test.exs` pipeline for details on how to use HLS plugin and generate HLS playlists for example media tracks.
Master and media playlists with related multimedia content that were generated via this pipeline are stored in `test/membrane_http_adaptive_stream/integration_test/fixtures` directory.

## Copyright and License

Copyright 2019, [Software Mansion](https://swmansion.com/?utm_source=git&utm_medium=readme&utm_campaign=membrane_http_adaptive_stream_plugin)

[![Software Mansion](https://logo.swmansion.com/logo?color=white&variant=desktop&width=200&tag=membrane-github)](https://swmansion.com/?utm_source=git&utm_medium=readme&utm_campaign=membrane_http_adaptive_stream_plugin)

Licensed under the [Apache License, Version 2.0](LICENSE)
