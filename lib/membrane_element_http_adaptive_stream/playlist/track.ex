defmodule Membrane.Element.HTTPAdaptiveStream.Playlist.Track do
  alias FE.Maybe

  defmodule Config do
    @enforce_keys [:id, :content_type, :init_extension, :fragment_extension, :max_size]
    defstruct @enforce_keys ++
                [
                  max_fragment_duration: 0
                ]
  end

  @config_keys Config.__struct__() |> Map.from_struct() |> Map.keys()
  defstruct @config_keys ++
              [:id_string, :init_name, current_seq_num: 0, fragments: Qex.new(), finished?: false]

  def new(%Config{} = config) do
    id_string = config.id |> :erlang.term_to_binary() |> Base.url_encode64(padding: false)

    %__MODULE__{
      init_name: "#{config.content_type}_init_#{id_string}#{config.init_extension}",
      id_string: id_string
    }
    |> Map.merge(Map.from_struct(config))
  end

  def add_fragment(%__MODULE__{finished?: false} = track, duration) do
    use Ratio, comparison: true

    name =
      "#{track.content_type}_fragment_#{track.current_seq_num}_#{track.id_string}" <>
        "#{track.fragment_extension}"

    track =
      track
      |> Map.update!(:fragments, &Qex.push(&1, %{name: name, duration: duration}))
      |> Map.update!(:current_seq_num, &(&1 + 1))
      |> Map.update!(:max_fragment_duration, &if(&1 > duration, do: &1, else: duration))

    {to_remove_name, track} =
      if track.max_size != :infinity and track.current_seq_num > track.max_size do
        {fragment, track} = track |> Map.get_and_update!(:fragments, &Qex.pop!/1)
        {Maybe.just(fragment.name), track}
      else
        {Maybe.nothing(), track}
      end

    {{name, to_remove_name}, track}
  end

  def finish(track) do
    %__MODULE__{track | finished?: true}
  end
end
