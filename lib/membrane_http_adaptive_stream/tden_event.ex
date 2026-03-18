defmodule Membrane.HTTPAdaptiveStream.TDENEvent do
  @moduledoc """
  Event containing the "encoding time" - timestamp describing when
  frame was encoded - read from the ID3v2.4 `TDEN` tag.

  Contains two fields:
  * timestamp - "encoding time" of a sample, as embeded in `TDEN` tag.
  * tden_buffer_timestamp - timestmap of a buffer based on which TDEN event was generated 
  """
  @derive Membrane.EventProtocol

  defstruct [:timestamp, :tden_buffer_timestamp]

  @typedoc @moduledoc
  @type t :: %__MODULE__{timestamp: Membrane.Time.t(), tden_buffer_timestamp: Membrane.Time.t()}
end
