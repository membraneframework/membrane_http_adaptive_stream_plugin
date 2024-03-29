defmodule Membrane.HTTPAdaptiveStream.BandwidthCalculator do
  @moduledoc false

  # Function to calculate multimedia track bandwidth
  # For a single track it comes down to finding a single segment with the highest bitrate, equal to the size in bits to duration ratio.

  use Numbers, overload_operators: true

  alias Membrane.HTTPAdaptiveStream.Manifest.Track
  alias Membrane.Time

  # Default value that is returned when track bandwidth calculation is impossible. High value is used since it is less
  # harmful to overestimate bandwidth and force HLS client to switch to track with lower bandwidth than to underestimate
  # and risk that client will use track with actual bandwidth that is beyond its capabilities.
  @default_bandwidth 2_560_000

  @spec calculate_max_bandwidth(Track.t(), integer()) :: integer()
  def calculate_max_bandwidth(track, segments_number \\ 20) do
    segments =
      track.segments
      |> Enum.to_list()
      |> Enum.take(-segments_number)
      |> Enum.filter(&Map.get(&1, :complete?, true))

    if Enum.empty?(segments) do
      @default_bandwidth
    else
      segments
      |> Enum.map(fn sg -> 8 * sg.size / (sg.duration / Time.second()) end)
      |> Enum.max(&Ratio.gte?/2)
      |> Ratio.trunc()
    end
  end

  @spec calculate_avg_bandwidth(Track.t(), integer()) :: integer()
  def calculate_avg_bandwidth(track, segments_number \\ 20) do
    segments =
      track.segments
      |> Enum.to_list()
      |> Enum.take(-segments_number)
      |> Enum.filter(&Map.get(&1, :complete?, true))

    if Enum.empty?(segments) do
      @default_bandwidth
    else
      segments
      |> Enum.reduce(Ratio.new(0), fn sg, ratio ->
        Ratio.new(8 * sg.size * Time.second())
        |> Numbers.div(sg.duration)
        |> Numbers.add(ratio)
      end)
      |> then(&(&1 / Enum.count(segments)))
      |> Ratio.trunc()
    end
  end
end
