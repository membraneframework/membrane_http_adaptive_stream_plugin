defmodule Membrane.HTTPAdaptiveStream.Manifest.SegmentDuration do
  @moduledoc """
  Module representing a segment duration range that should appear
  in a playlist.

  The minimum and target durations are relevant if sink
  is set to work in low latency mode as the durations of partial
  segment can greatly vary.
  """

  alias Membrane.Time

  @enforce_keys [:min, :target]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          min: Time.t(),
          target: Time.t()
        }

  @doc """
  Creates a new segment duration with a minimum and target duration.
  """
  @spec new(Time.t(), Time.t()) :: t()
  def new(min, target) when min <= target,
    do: %__MODULE__{min: min, target: target}

  @doc """
  Creates a new segment duration with a target duration.

  The minimum duration is set to the target one.
  """
  @spec new(Time.t()) :: t()
  def new(target),
    do: %__MODULE__{min: target, target: target}
end
