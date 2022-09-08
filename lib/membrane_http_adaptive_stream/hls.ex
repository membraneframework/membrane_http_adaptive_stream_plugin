defmodule Membrane.HTTPAdaptiveStream.HLS do
  @moduledoc """
  `Membrane.HTTPAdaptiveStream.Manifest` implementation for HTTP Live Streaming.

  Currently supports up to one audio and video stream.
  """
  @behaviour Membrane.HTTPAdaptiveStream.Manifest

  use Ratio

  alias Membrane.HTTPAdaptiveStream.{BandwidthCalculator, Manifest}
  alias Membrane.HTTPAdaptiveStream.Manifest.{Segment, Track}
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

    @impl true
    def serialize({:creation_time, date_time}) do
      [
        "#EXT-X-PROGRAM-DATE-TIME:#{date_time |> DateTime.truncate(:millisecond) |> DateTime.to_iso8601()}"
      ]
    end
  end

  @doc """
  Generates EXTM3U playlist for the given manifest
  """
  @impl true
  def serialize(%Manifest{} = manifest) do
    tracks_by_content =
      manifest.tracks
      |> Map.values()
      |> Enum.group_by(& &1.content_type)

    if length(Map.get(tracks_by_content, :audio, [])) > 1 do
      raise ArgumentError, message: "Multiple audio tracks are not currently supported."
    end

    main_manifest = main_playlist_from_tracks(tracks_by_content, manifest)
    manifest_per_track = playlists_per_track(tracks_by_content)

    %{
      main_manifest: main_manifest,
      manifest_per_track: manifest_per_track
    }
  end

  defp main_playlist_from_tracks(tracks, manifest) do
    main_manifest_name = "#{manifest.name}.m3u8"

    main_playlist =
      case tracks do
        %{muxed: muxed} -> build_master_playlist({nil, muxed})
        %{audio: [audio], video: videos} -> build_master_playlist({audio, videos})
        %{audio: [audio]} -> build_master_playlist({audio, nil})
        %{video: videos} -> build_master_playlist({nil, videos})
      end

    {main_manifest_name, main_playlist}
  end

  defp playlists_per_track(tracks) do
    case tracks do
      %{muxed: tracks} ->
        serialize_tracks(tracks)

      %{audio: [audio], video: videos} ->
        videos
        |> serialize_tracks()
        |> Map.put(audio.id, {build_media_playlist_path(audio), serialize_track(audio)})

      %{audio: [audio]} ->
        %{audio.id => {build_media_playlist_path(audio), serialize_track(audio)}}

      %{video: videos} ->
        serialize_tracks(videos)
    end
  end

  defp serialize_tracks(tracks) do
    tracks
    |> Enum.filter(&(&1.segments != @empty_segments))
    |> Map.new(fn track ->
      {track.id, {build_media_playlist_path(track), serialize_track(track)}}
    end)
  end

  defp build_media_playlist_path(%Track{} = track) do
    [track.content_type, "_", track.track_name, ".m3u8"] |> Enum.join("")
  end

  defp build_media_playlist_tag(%Track{} = track) do
    case track do
      %Track{content_type: :audio} ->
        """
        #EXT-X-MEDIA:TYPE=AUDIO,NAME="#{@default_audio_track_name}",GROUP-ID="#{@default_audio_track_id}",AUTOSELECT=YES,DEFAULT=YES,URI="#{build_media_playlist_path(track)}"
        """
        |> String.trim()

      %Track{content_type: type} when type in [:video, :muxed] ->
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

  defp serialize_track(%Track{} = track) do
    target_duration = Ratio.ceil(track.target_segment_duration / Time.second()) |> trunc()
    supports_ll_hls? = Track.supports_partial_segments?(track)

    target_partial_duration =
      if supports_ll_hls? do
        Float.ceil(Ratio.to_float(track.target_partial_segment_duration / Time.second()), 3)
      else
        nil
      end

    """
    #EXTM3U
    #EXT-X-VERSION:#{@version}
    #EXT-X-TARGETDURATION:#{target_duration}
    """ <>
      serialize_ll_hls_tags(supports_ll_hls?, target_partial_duration) <>
      """
      #EXT-X-MEDIA-SEQUENCE:#{track.current_seq_num}
      #EXT-X-DISCONTINUITY-SEQUENCE:#{track.current_discontinuity_seq_num}
      #EXT-X-MAP:URI="#{track.header_name}"
      #{serialize_segments(track)}
      #{if track.finished?, do: "#EXT-X-ENDLIST", else: ""}
      """
  end

  defp serialize_segments(track) do
    supports_ll_hls? = Track.supports_partial_segments?(track)

    [track.segments, Enum.count(track.segments)..1]
    |> Enum.zip()
    |> Enum.flat_map(&do_serialize_segment(&1, supports_ll_hls?))
    |> Enum.join("\n")
  end

  defp do_serialize_segment({%Segment{} = segment, idx}, supports_ll_hls?) do
    [
      # serialize partial segments just for the last 2 live segments, otherwise just keep the regular segments
      if(supports_ll_hls? and idx <= 4,
        do: serialize_partial_segments(segment, idx == 1),
        else: []
      ),
      serialize_regular_segment(segment)
    ]
    |> List.flatten()
  end

  defp serialize_regular_segment(%Segment{type: :partial}), do: []

  defp serialize_regular_segment(segment) do
    time = Ratio.to_float(segment.duration / Time.second())

    Enum.flat_map(segment.attributes, &SegmentAttribute.serialize/1) ++
      [
        "#EXTINF:#{time},",
        segment.name
      ]
  end

  defp serialize_partial_segments(segment, _is_last) do
    segment.parts
    |> Enum.map_reduce(0, fn part, total_bytes ->
      time = Ratio.to_float(part.duration / Time.second())

      serialized =
        "#EXT-X-PART:DURATION=#{time},URI=\"#{segment.name}\",BYTERANGE=\"#{part.byte_size}@#{total_bytes}\""

      serialized = if part.independent?, do: serialized <> ",INDEPENDENT=true", else: serialized

      {serialized, part.byte_size + total_bytes}
    end)
    |> then(fn {parts, _acc} -> parts end)

    # NOTE: we may want to support the EXT-X-PRELOAD-HINT as some point to further reduce latency
    # for now hls.js (most common HLS backend for browsers) does not support those
  end

  defp serialize_ll_hls_tags(true, target_partial_duration) do
    """
    #EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK=#{2 * target_partial_duration}
    #EXT-X-PART-INF:PART-TARGET=#{target_partial_duration}
    """
  end

  defp serialize_ll_hls_tags(false, _target_partial_duration), do: ""
end
