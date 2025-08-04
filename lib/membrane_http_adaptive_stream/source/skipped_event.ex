defmodule Membrane.HTTPAdaptiveStream.Source.SkippedEvent do
  @derive Membrane.EventProtocol
  @enforce_keys [:how_much_skipped]
  defstruct @enforce_keys
end
