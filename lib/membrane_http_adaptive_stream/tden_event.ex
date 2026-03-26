defmodule Membrane.HTTPAdaptiveStream.TDENEvent do
  @moduledoc """
  Event containing the "encoding time" - timestamp describing when
  frame was encoded - read from the ID3v2.4 `TDEN` tag.

  Contains two fields:
  * encoding_datetime - represents the wall-clock-time of encoding of a given sample, as embeded in `TDEN` tag.
  * buffer_ts - decoding timestmap of a buffer based on which TDEN event was generated.
  * target_duration - duration read from #EXT-X-TARGETDURATION tag of an .m3u8 playlist (might be used to shift the encoding timestamp)
  """
  @derive Membrane.EventProtocol

  defstruct [:encoding_datetime, :buffer_ts, :target_duration]

  @typedoc @moduledoc
  @type t :: %__MODULE__{
          encoding_datetime: DateTime.t(),
          buffer_ts: Membrane.Time.t(),
          target_duration: Membrane.Time.t()
        }
end
