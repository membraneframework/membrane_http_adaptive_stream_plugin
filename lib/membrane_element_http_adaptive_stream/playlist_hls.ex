defmodule Membrane.Element.HTTPAdaptiveStream.Playlist.HLS do
  alias FE.Maybe
  alias Membrane.Time
  @version 7

  defmodule Config do
    @enforce_keys [:init_name, :fragment_prefix, :fragment_extension]
    defstruct @enforce_keys ++
                [
                  max_duration: 0,
                  max_size: 5
                ]
  end

  @config_keys Config.__struct__() |> Map.from_struct() |> Map.keys()
  defstruct @config_keys ++ [current_seq_num: 0, fragments: Qex.new(), finished?: false]

  def playlist_extension, do: ".m3u8"

  def new(%Config{} = config) do
    Map.merge(%__MODULE__{}, Map.from_struct(config))
  end

  def put(%__MODULE__{finished?: false} = playlist, duration) do
    use Ratio
    name = "#{playlist.fragment_prefix}#{playlist.current_seq_num}#{playlist.fragment_extension}"

    playlist =
      playlist
      |> Map.update!(:fragments, &Qex.push(&1, %{name: name, duration: duration}))
      |> Map.update!(:current_seq_num, &(&1 + 1))
      |> Map.update!(:max_duration, &max(&1, duration))

    {to_remove_name, playlist} =
      if playlist.current_seq_num > playlist.max_size do
        {fragment, playlist} = playlist |> Map.get_and_update!(:fragments, &Qex.pop!/1)
        {Maybe.just(fragment.name), playlist}
      else
        {Maybe.nothing(), playlist}
      end

    {{name, to_remove_name}, playlist}
  end

  def finish(playlist) do
    %__MODULE__{playlist | finished?: true}
  end

  def serialize(%__MODULE__{} = playlist) do
    use Ratio

    target_duration = Ratio.ceil(playlist.max_duration / Time.second(1)) |> trunc
    media_sequence = max(0, playlist.current_seq_num - playlist.max_size)

    """
    #EXTM3U
    #EXT-X-VERSION:#{@version}
    #EXT-X-TARGETDURATION:#{target_duration}
    #EXT-X-MEDIA-SEQUENCE:#{media_sequence}
    #EXT-X-MAP:URI="#{playlist.init_name}"
    #{
      playlist.fragments
      |> Enum.flat_map(&["#EXTINF:#{Ratio.to_float(&1.duration / Time.second(1))},", &1.name])
      |> Enum.join("\n")
    }
    #{if playlist.finished?, do: "#EXT-X-ENDLIST", else: ""}
    """
  end
end
