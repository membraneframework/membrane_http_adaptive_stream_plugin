defmodule Membrane.HTTPAdaptiveStream.BandwidthCalculatorTest do
  use ExUnit.Case
  use Ratio, comparison: true

  alias Membrane.HTTPAdaptiveStream.BandwidthCalculator
  alias Membrane.HTTPAdaptiveStream.Manifest.Track
  alias Membrane.Time

  describe "Test bandwidth calculation for track with segment subsequences with duration " do
    test "equal to zero" do
      # All duration are 0 so bandwidth calculation is impossible.
      test_track = mock_track([{0, 0.0}, {1, 0.0}], 5)
      assert BandwidthCalculator.calculate_bandwidth(test_track) == 2_560_000
    end

    test "out of 0.5 - 1.5 of target bandwidth scope" do
      # All segment subsequences have duration out of 0.5 - 1.5 target duration range.
      test_track = mock_track([{0, 0.0}, {1, 0.0}, {1, 1}, {2, 0.5}, {3, 0.25}], 5)

      assert BandwidthCalculator.calculate_bandwidth(test_track) ==
               (8 * 3 / (0.25 / Time.second())) |> Ratio.floor()
    end

    test "both in and out of 0.5 - 1.5 of target bandwidth scope" do
      # Subsequence of one segment with size 3 and duration 0.25 would produce highest bitrate, but
      # subsequence constructed of sizes: 1, 1, 4, 2, 3 has duration of 2.75 that falls within range 0.5 - 1.5
      # of target duration.
      test_track = mock_track([{0, 0.0}, {1, 0.0}, {1, 1.0}, {4, 1.0}, {2, 0.5}, {3, 0.25}], 5)

      assert BandwidthCalculator.calculate_bandwidth(test_track) ==
               (8 * 11 / (2.75 / Time.second())) |> Ratio.floor()
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
