defmodule Membrane.HTTPAdaptiveStream.BandwidthCalculator do
  @moduledoc """
  Functions to calculate multimedia track bandwidth according to: https://datatracker.ietf.org/doc/html/rfc8216#section-4.3.4.2

  For a single track it comes down to finding a single segment with the highest bitrate, equal to byte_size to duration ratio.
  """

  use Ratio, comparison: true

  alias Membrane.HTTPAdaptiveStream.Manifest.Track
  alias Membrane.Time

  # Default value that is returned when track bandwidth calculation is impossible. High value is used since it is less
  # harmful to overestimate bandwidth and force HLS client to switch to track with lower bandwidth than to underestimate
  # and risk that client will use track with actual bandwidth that is beyond its capabilities.
  @default_bandwidth 2_560_000

  @spec calculate_bandwidth(Track.t(), integer()) :: integer()
  def calculate_bandwidth(track, segments_number \\ 20) do
    segments =
      track.segments
      |> Enum.to_list()
      |> Enum.take(-segments_number)
      |> Enum.filter(&(&1.duration > 0))

    if Enum.empty?(segments) do
      @default_bandwidth
    else
      segments
      |> Enum.map(fn sg -> 8 * sg.byte_size / (sg.duration / Time.second()) end)
      |> Enum.max(&Ratio.>=/2)
      |> Ratio.trunc()
    end
  end
end
