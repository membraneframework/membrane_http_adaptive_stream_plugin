defmodule Membrane.HTTPAdaptiveStream.HLS.Playlist do
  alias Membrane.HTTPAdaptiveStream.Playlist
  alias Membrane.HTTPAdaptiveStream.Playlist.Track
  alias Membrane.Time

  @behaviour Playlist

  @version 7

  @av_playlist """
  #EXTM3U
  #EXT-X-VERSION:#{@version}
  #EXT-X-INDEPENDENT-SEGMENTS
  #EXT-X-STREAM-INF:BANDWIDTH=2560000,CODECS="avc1.42e00a",AUDIO="a"
  video.m3u8
  #EXT-X-MEDIA:TYPE=AUDIO,NAME="a",GROUP-ID="a",AUTOSELECT=YES,DEFAULT=YES,URI="audio.m3u8"
  """

  @impl true
  def serialize(%Playlist{} = playlist) do
    tracks_by_content = playlist.tracks |> Map.values() |> Enum.group_by(& &1.content_type)
    main_playlist_name = "#{playlist.name}.m3u8"

    case {tracks_by_content[:audio], tracks_by_content[:video]} do
      {[audio], [video]} ->
        [
          {main_playlist_name, @av_playlist},
          {"audio.m3u8", serialize_track(audio)},
          {"video.m3u8", serialize_track(video)}
        ]

      {[audio], nil} ->
        [{main_playlist_name, serialize_track(audio)}]

      {nil, [video]} ->
        [{main_playlist_name, serialize_track(video)}]
    end
  end

  defp serialize_track(%Track{} = track) do
    use Ratio

    target_duration = Ratio.ceil(track.target_fragment_duration / Time.second(1)) |> trunc
    media_sequence = track.current_seq_num - Enum.count(track.fragments)

    """
    #EXTM3U
    #EXT-X-VERSION:#{@version}
    #EXT-X-TARGETDURATION:#{target_duration}
    #EXT-X-MEDIA-SEQUENCE:#{media_sequence}
    #EXT-X-MAP:URI="#{track.init_name}"
    #{
      track.fragments
      |> Enum.flat_map(&["#EXTINF:#{Ratio.to_float(&1.duration / Time.second(1))},", &1.name])
      |> Enum.join("\n")
    }
    #{if track.finished?, do: "#EXT-X-ENDLIST", else: ""}
    """
  end
end
