defmodule Membrane.HTTPAdaptiveStream.TDENEvent do
  @derive Membrane.EventProtocol

  @type timestamp :: String.t()

  defstruct [:timestamp, :tden_buffer_ts]

  @typedoc @moduledoc
  @type t :: %__MODULE__{timestamp: timestamp(), tden_buffer_ts: Membrane.Time.t()}
end
