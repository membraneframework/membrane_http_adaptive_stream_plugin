defmodule Membrane.HTTPAdaptiveStream.HLS do
  @moduledoc """
  `Membrane.HTTPAdaptiveStream.Manifest` implementation for HTTP Live Streaming.

  Currently supports up to one audio and video stream.
  """
  @behaviour Membrane.HTTPAdaptiveStream.Manifest

  use Ratio

  alias Membrane.HTTPAdaptiveStream.{BandwidthCalculator, Manifest}
  alias Membrane.Time

  @version 7

  @master_playlist_header """
                          #EXTM3U
                          #EXT-X-VERSION:#{@version}
                          #EXT-X-INDEPENDENT-SEGMENTS
                          """
                          |> String.trim()

  @empty_segments Qex.new()
  @default_audio_track_id "audio_default_id"
  @default_audio_track_name "audio_default_name"

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

    if length(Map.get(tracks_by_content, :audio, [])) > 1 do
      raise ArgumentError, message: "Multiple audio tracks are not currently supported."
    end

    case {tracks_by_content[:audio], tracks_by_content[:video]} do
      {[audio], nil} ->
        [
          {main_manifest_name, build_master_playlist({audio, nil})},
          {"audio.m3u8", serialize_track(audio)}
        ]

      {nil, videos} ->
        List.flatten([
          {main_manifest_name, build_master_playlist({nil, videos})},
          videos
          |> Enum.filter(&(&1.segments != @empty_segments))
          |> Enum.map(&{build_media_playlist_path(&1), serialize_track(&1)})
        ])

      {[audio], videos} ->
        List.flatten([
          {main_manifest_name, build_master_playlist({audio, videos})},
          {"audio.m3u8", serialize_track(audio)},
          videos
          |> Enum.filter(&(&1.segments != @empty_segments))
          |> Enum.map(&{build_media_playlist_path(&1), serialize_track(&1)})
        ])
    end
  end

  defp build_media_playlist_path(%Manifest.Track{} = track) do
    [track.content_type, "_", track.track_name, ".m3u8"] |> Enum.join("")
  end

  defp build_media_playlist_tag(%Manifest.Track{} = track) do
    case track do
      %Manifest.Track{content_type: :audio} ->
        """
        #EXT-X-MEDIA:TYPE=AUDIO,NAME="#{@default_audio_track_name}",GROUP-ID="#{@default_audio_track_id}",AUTOSELECT=YES,DEFAULT=YES,URI="audio.m3u8"
        """
        |> String.trim()

      %Manifest.Track{content_type: :video} ->
        """
        #EXT-X-STREAM-INF:BANDWIDTH=#{BandwidthCalculator.calculate_bandwidth(track)},CODECS="avc1.42e00a"
        """
        |> String.trim()
    end
  end

  defp build_master_playlist(tracks) do
    case tracks do
      {audio, nil} ->
        [@master_playlist_header, build_media_playlist_tag(audio)]
        |> Enum.join("")

      {nil, videos} ->
        [
          @master_playlist_header
          | videos
            |> Enum.filter(&(&1.segments != @empty_segments))
            |> Enum.flat_map(&[build_media_playlist_tag(&1), build_media_playlist_path(&1)])
        ]
        |> Enum.join("\n")

      {audio, videos} ->
        video_tracks =
          videos
          |> Enum.filter(&(&1.segments != @empty_segments))
          |> Enum.flat_map(
            &[
              "#{build_media_playlist_tag(&1)},AUDIO=\"#{@default_audio_track_id}\"",
              build_media_playlist_path(&1)
            ]
          )

        [
          @master_playlist_header,
          build_media_playlist_tag(audio),
          video_tracks
        ]
        |> List.flatten()
        |> Enum.join("\n")
    end
  end

  defp serialize_track(%Manifest.Track{} = track) do
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
    time = Ratio.to_float(segment.duration / Time.second())

    Enum.flat_map(segment.attributes, &SegmentAttribute.serialize/1) ++
      ["#EXTINF:#{time},", segment.name]
  end
end
