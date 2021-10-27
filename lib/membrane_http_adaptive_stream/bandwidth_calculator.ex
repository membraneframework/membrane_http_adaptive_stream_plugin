defmodule Membrane.HTTPAdaptiveStream.BandwidthCalculator do
  @moduledoc """
  Functions to calculate multimedia track bandwidth according to: https://datatracker.ietf.org/doc/html/rfc8216#section-4.3.4.2
  """

  use Ratio

  alias Membrane.HTTPAdaptiveStream.Manifest.Track
  alias Membrane.Time

  @spec calculate_bandwidth(Track.t()) :: integer()
  def calculate_bandwidth(%Track{} = track) do
    target_duration = Ratio.ceil(track.target_segment_duration / Time.second()) |> trunc()
    segments_sequences = generate_subsequences(track.segments |> Enum.to_list())

    segments_meta =
      segments_sequences
      |> Enum.map(
        &{
          Enum.map(&1, fn sg -> 8 * sg.byte_size end) |> Enum.sum(),
          Enum.map(&1, fn sg -> Ratio.to_float(sg.duration / Time.second()) end) |> Enum.sum()
        }
      )

    # According to HLS rfc only segment subsequences with duration between 0.5 and 1.5 of target duration should be
    # considered
    valid_segments_meta =
      Enum.filter(segments_meta, fn {_bits_size, duration} ->
        duration >= 0.5 * target_duration and duration <= 1.5 * target_duration
      end)

    Enum.map(valid_segments_meta, fn {bits_size, duration} -> bits_size / duration end)
    |> Enum.max()
    |> trunc()
  end

  # Generates all contiguous subsequences of a list. This is needed because rfc defines bandwidth
  # in HLS as the highest size to duration ratio of all contiguous subsequences of media track segments.
  defp generate_subsequences(sequence) do
    n = length(sequence)
    subsequence_ranges = for(i <- 0..(n - 1), do: for(j <- 0..i, do: {j, i})) |> List.flatten()
    Enum.map(subsequence_ranges, fn {i, j} -> Enum.slice(sequence, i..j) end)
  end
end
