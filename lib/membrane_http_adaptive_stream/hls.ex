defmodule Membrane.HTTPAdaptiveStream.HLS do
  @moduledoc """
  `Membrane.HTTPAdaptiveStream.Manifest` implementation for HTTP Live Streaming.

  Currently supports up to one audio and video stream.
  """

  @behaviour Membrane.HTTPAdaptiveStream.Manifest

  alias Membrane.HTTPAdaptiveStream.Manifest
  alias Membrane.HTTPAdaptiveStream.Manifest.Track
  alias Membrane.Time

  @version 7

  @av_manifest """
  #EXTM3U
  #EXT-X-VERSION:#{@version}
  #EXT-X-INDEPENDENT-SEGMENTS
  #EXT-X-STREAM-INF:BANDWIDTH=2560000,CODECS="avc1.42e00a",AUDIO="a"
  video.m3u8
  #EXT-X-MEDIA:TYPE=AUDIO,NAME="a",GROUP-ID="a",AUTOSELECT=YES,DEFAULT=YES,URI="audio.m3u8"
  """

  defmodule SegmentAttribute do
    @moduledoc """
    Implementation of `Membrane.HTTPAdaptiveStream.Manifest.SegmentAttribute` behaviour for HTTP Live Streaming
    """
    @behaviour Membrane.HTTPAdaptiveStream.Manifest.SegmentAttribute

    import Membrane.HTTPAdaptiveStream.Manifest.SegmentAttribute

    @impl true
    def serialize(discontinuity(header_name, number)) do
      [
        "#EXT-X-DISCONTINUITY-SEQUENCE:#{number}",
        "#EXT-X-DISCONTINUITY",
        "#EXT-X-MAP:URI=#{header_name}"
      ]
    end
  end

  @doc """
  Generates EXTM3U playlist for the given manifest
  """
  @impl true
  def serialize(%Manifest{} = manifest) do
    tracks_by_content = manifest.tracks |> Map.values() |> Enum.group_by(& &1.content_type)
    main_manifest_name = "#{manifest.name}.m3u8"

    case {tracks_by_content[:audio], tracks_by_content[:video]} do
      {[audio], [video]} ->
        [
          {main_manifest_name, @av_manifest},
          {"audio.m3u8", serialize_track(audio)},
          {"video.m3u8", serialize_track(video)}
        ]

      {[audio], nil} ->
        [{main_manifest_name, serialize_track(audio)}]

      {nil, [video]} ->
        [{main_manifest_name, serialize_track(video)}]
    end
  end

  defp serialize_track(%Track{} = track) do
    use Ratio

    target_duration = Ratio.ceil(track.target_segment_duration / Time.second()) |> trunc()
    media_sequence = track.current_seq_num - Enum.count(track.segments)

    """
    #EXTM3U
    #EXT-X-VERSION:#{@version}
    #EXT-X-TARGETDURATION:#{target_duration}
    #EXT-X-MEDIA-SEQUENCE:#{media_sequence}
    #EXT-X-DISCONTINUITY-SEQUENCE:#{track.current_discontinuity_seq_num}
    #EXT-X-MAP:URI="#{track.header_name}"
    #{track.segments |> Enum.flat_map(&serialize_segment/1) |> Enum.join("\n")}
    #{if track.finished?, do: "#EXT-X-ENDLIST", else: ""}
    """
  end

  defp serialize_segment(segment) do
    use Ratio
    time = Ratio.to_float(segment.duration / Time.second())

    Enum.flat_map(segment.attributes, &SegmentAttribute.serialize/1) ++
      ["#EXTINF:#{time},", segment.name]
  end
end
