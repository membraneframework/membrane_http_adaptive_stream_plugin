defmodule Membrane.Element.HTTPAdaptiveStream.Storage do
  @type config_t :: struct
  @type state_t :: any
  @callback init(config_t) :: state_t
  @callback store(
              resource_name :: String.t(),
              content :: String.t(),
              content_type :: :text | :binary,
              state_t
            ) :: :ok | {:error, reason :: any}
  @callback remove(resource_name :: String.t(), state_t) :: :ok | {:error, reason :: any}
end
