defmodule Membrane.HTTPAdaptiveStream.TDENEvent do
  @derive Membrane.EventProtocol

  @type timestamp :: String.t()

  defstruct [:timestamp, :tden_buffer_timestamp]

  @typedoc @moduledoc
  @type t :: %__MODULE__{timestamp: timestamp(), tden_buffer_timestamp: Membrane.Time.t()}
end
