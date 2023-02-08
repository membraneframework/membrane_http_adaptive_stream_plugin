defmodule Membrane.HTTPAdaptiveStream.BandwidthCalculatorTest do
  use ExUnit.Case
  use Ratio, comparison: true

  alias Membrane.HTTPAdaptiveStream.BandwidthCalculator
  alias Membrane.HTTPAdaptiveStream.Manifest.Track
  alias Membrane.Time

  @default_bandwidth 2_560_000

  describe "Bandwidth calculator calculates correct bandwidth" do
    test "bandwidth equal to maximum segment bandwidth" do
      test_track = mock_track([{1, 0.8}, {2, 1}, {1, 1}, {2, 1.3}, {3, 0.25}], 5)

      assert BandwidthCalculator.calculate_bandwidth(test_track) ==
               (8 * 3 / (0.25 / Time.second())) |> Ratio.floor()
    end

    test "no segments in track" do
      test_track = mock_track([], 5)

      assert BandwidthCalculator.calculate_bandwidth(test_track) == @default_bandwidth
    end
  end

  defp mock_segment({byte_size, duration}) do
    %{
      name: "mock_segment",
      duration: duration,
      byte_size: byte_size,
      attributes: []
    }
  end

  defp mock_track(segments_meta, target_segment_duration) do
    segments = segments_meta |> Enum.map(&mock_segment(&1)) |> Qex.new()

    %Track{
      id: "mock_track",
      track_name: "mock_track",
      content_type: :video,
      header_extension: ".mp4",
      segment_extension: ".m4s",
      target_segment_duration: target_segment_duration,
      segments: segments
    }
  end
end
