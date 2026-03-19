defmodule Membrane.HTTPAdaptiveStream.TDENEvent do
  @moduledoc """
  Event containing the "encoding time" - timestamp describing when
  frame was encoded - read from the ID3v2.4 `TDEN` tag.

  Contains two fields:
  * encoding_ts - "encoding time" (represented as unix time) of a sample, as embeded in `TDEN` tag.
  * buffer_ts - decoding timestmap of a buffer based on which TDEN event was generated.
  * segment_duration - duration of the first segment of the HLS playlist from  which TDEN tag was read.
  """
  @derive Membrane.EventProtocol

  defstruct [:encoding_ts, :buffer_ts, :segment_duration]

  @typedoc @moduledoc
  @type t :: %__MODULE__{
          encoding_ts: Membrane.Time.t(),
          buffer_ts: Membrane.Time.t(),
          segment_duration: Membrane.Time.t()
        }
end
