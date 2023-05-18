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

  @keep_latest_n_segment_parts 4
  @min_segments_in_delta_playlist 6

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

    master_manifest = master_playlist_from_tracks(tracks_by_content, manifest)
    manifest_per_track = playlists_per_track(tracks_by_content)

    %{
      master_manifest: master_manifest,
      manifest_per_track: manifest_per_track
    }
  end

  defp master_playlist_from_tracks(tracks, manifest) do
    master_manifest_name = "#{manifest.name}.m3u8"

    master_playlist =
      case tracks do
        %{muxed: muxed} -> build_master_playlist({nil, muxed})
        %{audio: [audio], video: videos} -> build_master_playlist({audio, videos})
        %{audio: [audio]} -> build_master_playlist({audio, nil})
        %{video: videos} -> build_master_playlist({nil, videos})
      end

    {master_manifest_name, master_playlist}
  end

  defp playlists_per_track(tracks) do
    case tracks do
      %{muxed: tracks} ->
        serialize_tracks(tracks)

      %{audio: audios, video: videos} ->
        serialized_videos = serialize_tracks(videos)
        serialized_audios = serialize_tracks(audios)
        Map.merge(serialized_videos, serialized_audios)

      %{audio: audios} ->
        serialize_tracks(audios)

      %{video: videos} ->
        serialize_tracks(videos)
    end
  end

  defp serialize_tracks(tracks) do
    tracks
    |> Enum.filter(&(&1.segments != @empty_segments))
    |> Enum.reduce(%{}, &add_serialized_track(&2, &1))
  end

  defp add_serialized_track(tracks_map, track) do
    playlist_path = build_media_playlist_path(track)
    serialized_track = {playlist_path, serialize_track(track)}
    tracks_map = Map.put(tracks_map, track.id, serialized_track)

    should_generate_delta_playlist? =
      Track.supports_partial_segments?(track) &&
        Enum.count(track.segments) > @min_segments_in_delta_playlist

    if should_generate_delta_playlist? do
      delta_path = build_media_playlist_path(track, delta?: true)
      serialized_delta_track = {delta_path, serialize_track(track, delta?: true)}
      Map.put(tracks_map, :"#{track.id}_delta", serialized_delta_track)
    else
      tracks_map
    end
  end

  defp build_media_playlist_path(track, opts \\ [delta?: false])

  defp build_media_playlist_path(%Track{} = track, delta?: true) do
    track.track_name <> "_delta.m3u8"
  end

  defp build_media_playlist_path(%Track{} = track, delta?: false) do
    track.track_name <> ".m3u8"
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

  defp serialize_track(%Track{} = track, [delta?: delta?] \\ [delta?: false]) do
    target_duration = Ratio.ceil(track.segment_duration / Time.second()) |> trunc()
    supports_ll_hls? = Track.supports_partial_segments?(track)

    """
    #EXTM3U
    #EXT-X-VERSION:#{@version}
    #EXT-X-TARGETDURATION:#{target_duration}
    """ <>
      serialize_ll_hls_tags(track) <>
      """
      #EXT-X-MEDIA-SEQUENCE:#{track.current_seq_num}
      #EXT-X-DISCONTINUITY-SEQUENCE:#{track.current_discontinuity_seq_num}
      #EXT-X-MAP:URI="#{track.header_name}"
      #{serialize_segments(track.segments, supports_ll_hls?: supports_ll_hls?, delta?: delta?)}
      #{if track.finished?, do: "#EXT-X-ENDLIST", else: serialize_preload_hint_tag(supports_ll_hls?, track)}
      """
  end

  defp serialize_segments(segments, supports_ll_hls?: supports_ll_hls?, delta?: true) do
    segments_to_skip_count = Enum.count(segments) - @min_segments_in_delta_playlist

    prefix = """
    #EXT-X-SKIP:SKIPPED-SEGMENTS=#{segments_to_skip_count}
    """

    serialized_segments =
      segments
      |> Enum.drop(segments_to_skip_count)
      |> serialize_segments(supports_ll_hls?: supports_ll_hls?, delta?: false)

    prefix <> serialized_segments
  end

  defp serialize_segments(segments, supports_ll_hls?: supports_ll_hls?, delta?: false) do
    segments
    |> Enum.split(-@keep_latest_n_segment_parts)
    |> then(fn {regular_segments, ll_segments} ->
      regular = Enum.flat_map(regular_segments, &do_serialize_segment(&1, false))

      ll = Enum.flat_map(ll_segments, &do_serialize_segment(&1, supports_ll_hls?))

      regular ++ ll
    end)
    |> Enum.join("\n")
  end

  defp do_serialize_segment(%Segment{} = segment, supports_ll_hls?) do
    [
      # serialize partial segments just for the last 2 live segments, otherwise just keep the regular segments
      if(supports_ll_hls?,
        do: serialize_partial_segments(segment),
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

  defp serialize_partial_segments(segment) do
    segment.parts
    |> Enum.map_reduce(0, fn part, total_bytes ->
      time = Ratio.to_float(part.duration / Time.second())

      serialized =
        "#EXT-X-PART:DURATION=#{time},URI=\"#{segment.name}\",BYTERANGE=\"#{part.size}@#{total_bytes}\""

      serialized = if part.independent?, do: serialized <> ",INDEPENDENT=YES", else: serialized

      {serialized, part.size + total_bytes}
    end)
    |> then(fn {parts, _acc} -> parts end)
  end

  defp serialize_ll_hls_tags(track) do
    supports_ll_hls? = Track.supports_partial_segments?(track)

    if supports_ll_hls? do
      can_skip_segments_duration =
        track.segments
        |> Enum.drop(-@min_segments_in_delta_playlist)
        |> Enum.reduce(0, &(&1.duration + &2))
        |> then(&Ratio.to_float(&1 / Time.second()))

      target_partial_duration =
        Float.ceil(Ratio.to_float(track.partial_segment_duration / Time.second()), 3)

      """
      #EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK=#{2 * target_partial_duration}#{can_skip_until(can_skip_segments_duration)}
      #EXT-X-PART-INF:PART-TARGET=#{target_partial_duration}
      """
    else
      ""
    end
  end

  defp serialize_preload_hint_tag(true, track) do
    get_tag = fn name, bytes ->
      "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"#{name}\",BYTERANGE-START=#{bytes}"
    end

    case Qex.last(track.segments) do
      :empty ->
        ""

      {:value, %Segment{type: :partial, name: name, parts: parts}} ->
        bytes = Enum.reduce(parts, 0, fn part, acc -> acc + part.size end)
        get_tag.(name, bytes)

      {:value, %Segment{type: :full}} ->
        name = track.segment_naming_fun.(track) <> track.segment_extension
        get_tag.(name, 0)
    end
  end

  defp serialize_preload_hint_tag(false, _track), do: ""

  defp can_skip_until(duration) when duration > 0,
    do: ",CAN-SKIP-UNTIL=#{duration}"

  defp can_skip_until(_duration), do: ""
end
