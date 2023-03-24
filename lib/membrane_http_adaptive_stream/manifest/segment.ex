defmodule Membrane.HTTPAdaptiveStream.Manifest.Segment do
  @moduledoc """
  Structure representing a single manifest segment.


  It stores the following fields:
  * `name` - the segment's name
  * `duration` - the segment's total duration
  * `size` - the byte size of the segment payload
  * `attributes` - the meta attributes associated with the segment
  * `type` - decides if the structure is a full segment that can exist on its own
             or if it hosts and awaits more partial segments
  * `parts` - the partial segments making up the full segment
  """

  alias Membrane.HTTPAdaptiveStream.Manifest.SegmentAttribute

  @enforce_keys [:name, :duration, :size, :attributes]
  defstruct @enforce_keys ++ [type: :full, parts: []]

  @type segment_duration_t :: Membrane.Time.t() | Ratio.t()
  @typedoc """
  Structure of partial segment.

  Attributes representing a partial segment:
  * `independent?` - decides if a segment can be played on its own e.g. starts with a keyframe or is an audio sample
  * `duration` - the duration of the partial segment
  * `attributes` - the attributes for the particular partial segment
  """
  @type partial_segment_t :: %{
          independent?: boolean(),
          duration: segment_duration_t(),
          size: non_neg_integer(),
          payload: binary() | nil
        }

  @typedoc """
  Determines if segment is full and independent or partial
  and consists of several partial segments.
  """
  @type type_t :: :full | :partial

  @type t :: %__MODULE__{
          name: String.t(),
          duration: segment_duration_t(),
          size: non_neg_integer(),
          attributes: [SegmentAttribute.t()],
          type: type_t(),
          parts: [partial_segment_t()]
        }
end
