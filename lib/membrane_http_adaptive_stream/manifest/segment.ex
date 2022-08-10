defmodule Membrane.HTTPAdaptiveStream.Manifest.Segment do
  @moduledoc """
  Structure representing a single manifest segment.


  It stores the following fields:
  * `name` - the segment's name
  * `duration` - the segment's total duration
  * `byte_size` - the byte_size of the segment payload
  * `attributes` - the meta attributes associated with the segment
  * `partial?` - decides if the segment is still partial, meaning that it awaits more partial segments until
                becoming fully functional on its own
  * `parts` - the partial segments making up the full segment
  """

  alias Membrane.HTTPAdaptiveStream.Manifest.SegmentAttribute

  @enforce_keys [:name, :duration, :byte_size, :attributes]
  defstruct @enforce_keys ++ [partial?: false, parts: []]

  @type segment_duration_t :: Membrane.Time.t() | Ratio.t()
  @type partial_segment_t :: %{
          independent?: boolean(),
          duration: segment_duration_t(),
          attributes: [SegmentAttribute.t()]
        }

  @type t :: %__MODULE__{
          name: String.t(),
          duration: segment_duration_t(),
          byte_size: non_neg_integer(),
          attributes: [SegmentAttribute.t()],
          partial?: boolean(),
          parts: [partial_segment_t()]
        }
end
