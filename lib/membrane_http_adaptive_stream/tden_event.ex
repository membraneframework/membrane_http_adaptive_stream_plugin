defmodule Membrane.HTTPAdaptiveStream.TDENEvent do
  @derive Membrane.EventProtocol

  @type timestamp :: String.t()

  defstruct [:timestamp]

  @typedoc @moduledoc
  @type t :: %__MODULE__{timestamp: timestamp()}
end
