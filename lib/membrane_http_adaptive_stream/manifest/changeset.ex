defmodule Membrane.HTTPAdaptiveStream.Manifest.Changeset do
  @moduledoc """
  Structure representing changes that has been applied to the track. What element has been added
  and what elements are to be removed.
  """
  defmodule Segment do
    @moduledoc """
    Type used to recognize `to_add` segments in Changeset.
    """
    @type t :: %__MODULE__{
            type: :segment | :partial_segment,
            duration: Membrane.Time.t() | Ratio.t(),
            sequence_number: non_neg_integer(),
            name: String.t(),
            payload: binary(),
            independent?: boolean(),
            byte_offset: non_neg_integer() | nil
          }
    @enforce_keys [:type, :duration, :sequence_number, :name, :payload]
    defstruct @enforce_keys ++ [independent?: nil, byte_offset: nil]
  end

  @type element_type_t :: :segment | :header
  @type t :: %__MODULE__{
          to_add: [Segment.t()],
          to_remove: [{element_type_t(), name :: String.t()}]
        }
  defstruct to_add: [],
            to_remove: []

  @spec merge(t(), t()) :: t()
  def merge(%__MODULE__{to_add: to_add_a, to_remove: to_remove_a}, %__MODULE__{
        to_add: to_add_b,
        to_remove: to_remove_b
      }),
      do: %__MODULE__{to_add: to_add_a ++ to_add_b, to_remove: to_remove_a ++ to_remove_b}
end
