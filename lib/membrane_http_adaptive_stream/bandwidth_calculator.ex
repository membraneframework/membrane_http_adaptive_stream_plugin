defmodule Membrane.HTTPAdaptiveStream.BandwidthCalculator do
  @moduledoc """
  Functions to calculate multimedia track bandwidth according to: https://datatracker.ietf.org/doc/html/rfc8216#section-4.3.4.2
  """

  use Ratio, comparison: true

  alias Membrane.HTTPAdaptiveStream.Manifest.Track
  alias Membrane.Time

  # Default value that is returned when track bandwidth calculation is impossible. High value is used since it is less
  # harmful to overestimate bandwidth and force HLS client to switch to track with lower bandwidth than to underestimate
  # and risk that client will use track with actual bandwidth that is beyond its capabilities.
  @default_bandwidth 2_560_000

  @spec calculate_bandwidth(Track.t()) :: integer()
  def calculate_bandwidth(track), do: @default_bandwidth
end
