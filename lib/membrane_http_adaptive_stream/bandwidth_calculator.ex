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
  def calculate_bandwidth(track) do
    target_duration = track.target_segment_duration / Time.second()
    segments_sequences = generate_subsequences(track.segments |> Enum.to_list())

    segments_meta =
      segments_sequences
      |> Enum.map(
        &{
          Enum.map(&1, fn sg -> 8 * sg.byte_size end) |> Enum.sum(),
          Enum.map(&1, fn sg -> sg.duration / Time.second() end)
          |> Enum.reduce(fn acc, x -> acc + x end)
        }
      )
      |> Enum.filter(fn {_bits_size, duration} -> duration != 0.0 end)

    segments_bitrates =
      segments_meta |> Enum.map(fn {bits_size, duration} -> bits_size / duration end)

    # According to HLS rfc only segment subsequences with duration between 0.5 and 1.5 of target duration should be
    # considered
    validated_segments_meta =
      segments_meta
      |> Enum.filter(fn {_bits_size, duration} ->
        duration >= 0.5 * target_duration and duration <= 1.5 * target_duration
      end)

    validated_segments_bitrates =
      validated_segments_meta |> Enum.map(fn {bits_size, duration} -> bits_size / duration end)

    cond do
      Enum.empty?(segments_meta) ->
        @default_bandwidth

      Enum.empty?(validated_segments_meta) ->
        segments_bitrates |> Enum.max(&Ratio.>=/2) |> Ratio.floor()

      true ->
        validated_segments_bitrates |> Enum.max(&Ratio.>=/2) |> Ratio.floor()
    end
  end

  # Generates all contiguous subsequences of a list. This is needed because rfc defines bandwidth
  # in HLS as the highest size to duration ratio of all contiguous subsequences of media track segments.
  defp generate_subsequences(sequence) do
    n = length(sequence)
    subsequence_ranges = for(i <- 0..(n - 1), do: for(j <- 0..i, do: {j, i})) |> List.flatten()
    Enum.map(subsequence_ranges, fn {i, j} -> Enum.slice(sequence, i..j) end)
  end
end
