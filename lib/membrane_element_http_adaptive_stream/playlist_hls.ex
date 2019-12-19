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

  def new(%Config{} = config) do
    Map.merge(%__MODULE__{}, Map.from_struct(config))
  end

  def put(%__MODULE__{} = playlist, duration) do
    use Ratio
    name = "#{playlist.fragment_prefix}#{playlist.current_seq_num}#{playlist.fragment_extension}"

    playlist =
      playlist
      |> Map.update!(:fragments, &Qex.push(&1, {name, duration}))
      |> Map.update!(:current_seq_num, &(&1 + 1))
      |> Map.update!(:max_duration, &max(&1, duration))

    {rem, playlist} =
      if playlist.current_seq_num > playlist.max_size do
        playlist |> Map.get_and_update!(:fragments, &Qex.pop!/1)
      else
        {nil, playlist}
      end

    {{name, rem |> Maybe.new() |> Maybe.map(&elem(&1, 0))}, playlist}
  end

  def finish(playlist) do
    %__MODULE__{playlist | finished?: true}
  end

  def serialize(%__MODULE__{} = playlist) do
    use Ratio

    """
    #EXTM3U
    #EXT-X-VERSION:#{@version}
    #EXT-X-TARGETDURATION:#{Ratio.ceil(playlist.max_duration / Time.second(1)) |> trunc}
    #EXT-X-MEDIA-SEQUENCE:#{max(0, playlist.current_seq_num - playlist.max_size)}
    #EXT-X-MAP:URI="#{playlist.init_name}"
    #{
      playlist.fragments
      |> Enum.flat_map(fn {name, duration} ->
        ["#EXTINF:#{Ratio.to_float(duration / Time.second(1))},", name]
      end)
      |> Enum.join("\n")
    }
    #{if playlist.finished?, do: "#EXT-X-ENDLIST", else: ""}
    """
  end
end
