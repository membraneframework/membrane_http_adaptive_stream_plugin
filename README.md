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
{:membrane_http_adaptive_stream_plugin, "~> 0.4.0"}
```

## Usage Example
See `examples/hls_sink.exs` pipeline for details on how to use HLS plugin. The example can be run with the following command:
```shell
elixir examples/hls_sink.exs
```
Master and media playlists with related multimedia content will be stored in `output` directory. To specify another location set environmental variable `HLS_OUTPUT_DIR`.
## Copyright and License

Copyright 2019, [Software Mansion](https://swmansion.com/?utm_source=git&utm_medium=readme&utm_campaign=membrane_http_adaptive_stream_plugin)

[![Software Mansion](https://logo.swmansion.com/logo?color=white&variant=desktop&width=200&tag=membrane-github)](https://swmansion.com/?utm_source=git&utm_medium=readme&utm_campaign=membrane_http_adaptive_stream_plugin)

Licensed under the [Apache License, Version 2.0](LICENSE)
