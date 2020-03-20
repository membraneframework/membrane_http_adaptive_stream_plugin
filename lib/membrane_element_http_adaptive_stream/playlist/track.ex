defmodule Membrane.Element.HTTPAdaptiveStream.Playlist.Track do
  defmodule Config do
    @enforce_keys [
      :id,
      :content_type,
      :init_extension,
      :fragment_extension,
      :target_window_duration
    ]
    defstruct @enforce_keys ++
                [
                  target_fragment_duration: 0
                ]
  end

  @config_keys Config.__struct__() |> Map.from_struct() |> Map.keys()
  defstruct @config_keys ++
              [
                :id_string,
                :init_name,
                current_seq_num: 0,
                fragments: Qex.new(),
                finished?: false,
                window_duration: 0
              ]

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

    {to_remove_names, track} =
      track
      |> Map.update!(:fragments, &Qex.push(&1, %{name: name, duration: duration}))
      |> Map.update!(:current_seq_num, &(&1 + 1))
      |> Map.update!(:window_duration, &(&1 + duration))
      |> Map.update!(:target_fragment_duration, &if(&1 > duration, do: &1, else: duration))
      |> remove_stale_fragments()

    {{name, to_remove_names}, track}
  end

  def finish(track) do
    %__MODULE__{track | finished?: true}
  end

  defp remove_stale_fragments(%__MODULE__{target_window_duration: :infinity} = track) do
    {[], track}
  end

  defp remove_stale_fragments(track) do
    %__MODULE__{
      fragments: fragments,
      window_duration: window_duration,
      target_window_duration: target_window_duration
    } = track

    {to_remove, fragments, window_duration} =
      do_remove_stale_fragments(fragments, window_duration, target_window_duration, [])

    track = %__MODULE__{track | fragments: fragments, window_duration: window_duration}
    {to_remove, track}
  end

  defp do_remove_stale_fragments(fragments, window_duration, target_window_duration, acc) do
    use Ratio, comparison: true
    {fragment, new_fragments} = Qex.pop!(fragments)
    new_window_duration = window_duration - fragment.duration

    if new_window_duration > target_window_duration do
      do_remove_stale_fragments(
        new_fragments,
        new_window_duration,
        target_window_duration,
        [fragment.name | acc]
      )
    else
      {Enum.reverse(acc), fragments, window_duration}
    end
  end
end
