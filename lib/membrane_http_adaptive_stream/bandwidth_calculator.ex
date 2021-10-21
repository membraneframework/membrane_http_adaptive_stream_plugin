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
        &[
          Enum.map(&1, fn sg -> sg.bits end) |> Enum.sum(),
          Enum.map(&1, fn sg -> Ratio.to_float(sg.duration / Time.second()) end) |> Enum.sum()
        ]
      )

    Enum.filter(segments_meta, fn [_bits, duration] ->
      duration >= 0.5 * target_duration and duration <= 1.5 * target_duration
    end)
    |> Enum.map(&(List.first(&1) / List.last(&1)))
    |> Enum.max()
    |> trunc()
  end

  defp generate_prefixes(sequence) do
    Enum.reduce(sequence, [], fn x, acc ->
      case acc do
        [] -> [[x]]
        _ -> [[x | List.first(acc)] | acc]
      end
    end)
  end

  defp generate_subsequences(sequence) do
    Enum.reduce(sequence, [], fn x, acc ->
      case acc do
        [] -> [[x]]
        _ -> generate_prefixes([x | List.first(acc)]) ++ acc
      end
    end)
  end
end
