defmodule Membrane.HTTPAdaptiveStream.Playlist.Track do
  @moduledoc """
  Struct representing a state of a single playlist track and functions to operate
  on it.
  """
  defmodule Config do
    @moduledoc """
    Track configuration.
    """

    alias Membrane.HTTPAdaptiveStream.Playlist.Track

    @enforce_keys [
      :id,
      :content_type,
      :init_extension,
      :fragment_extension,
      :target_fragment_duration
    ]
    defstruct @enforce_keys ++
                [
                  target_window_duration: nil,
                  persist?: false
                ]

    @type t :: %__MODULE__{
            id: Track.id_t(),
            content_type: :audio | :video,
            init_extension: String.t(),
            fragment_extension: String.t(),
            target_fragment_duration: Membrane.Time.t() | Ratio.t(),
            target_window_duration: Membrane.Time.t() | Ratio.t(),
            persist?: boolean
          }
  end

  @config_keys Config.__struct__() |> Map.from_struct() |> Map.keys()
  defstruct @config_keys ++
              [
                :id_string,
                :init_name,
                current_seq_num: 0,
                fragments: Qex.new(),
                stale_fragments: Qex.new(),
                finished?: false,
                window_duration: 0
              ]

  @type t :: %__MODULE__{
          id: id_t,
          content_type: :audio | :video,
          init_extension: String.t(),
          fragment_extension: String.t(),
          target_fragment_duration: fragment_duration_t,
          target_window_duration: Membrane.Time.t() | Ratio.t(),
          persist?: boolean,
          id_string: String.t(),
          init_name: String.t(),
          current_seq_num: non_neg_integer,
          fragments: fragments_t,
          stale_fragments: fragments_t,
          finished?: boolean,
          window_duration: non_neg_integer
        }

  @type id_t :: any
  @type fragments_t :: Qex.t({name :: String.t(), fragment_duration_t})
  @type fragment_duration_t :: Membrane.Time.t() | Ratio.t()

  @spec new(Config.t()) :: t
  def new(%Config{} = config) do
    id_string = config.id |> :erlang.term_to_binary() |> Base.url_encode64(padding: false)

    %__MODULE__{
      init_name: "#{config.content_type}_init_#{id_string}#{config.init_extension}",
      id_string: id_string
    }
    |> Map.merge(Map.from_struct(config))
  end

  @spec add_fragment(t, fragment_duration_t) ::
          {{to_add_name :: String.t(), to_remove_names :: [String.t()]}, t}
  def add_fragment(%__MODULE__{finished?: false} = track, duration) do
    use Ratio, comparison: true

    name =
      "#{track.content_type}_fragment_#{track.current_seq_num}_#{track.id_string}" <>
        "#{track.fragment_extension}"

    {stale_fragments, track} =
      track
      |> Map.update!(:fragments, &Qex.push(&1, %{name: name, duration: duration}))
      |> Map.update!(:current_seq_num, &(&1 + 1))
      |> Map.update!(:window_duration, &(&1 + duration))
      |> Map.update!(:target_fragment_duration, &if(&1 > duration, do: &1, else: duration))
      |> pop_stale_fragments()

    {to_remove_names, stale_fragments} =
      if track.persist? do
        {[], Qex.join(track.stale_fragments, Qex.new(stale_fragments))}
      else
        {Enum.map(stale_fragments, & &1.name), track.stale_fragments}
      end

    {{name, to_remove_names}, %__MODULE__{track | stale_fragments: stale_fragments}}
  end

  @spec finish(t) :: t
  def finish(track) do
    %__MODULE__{track | finished?: true}
  end

  @spec from_beginning(t) :: t
  def from_beginning(%__MODULE__{persist?: true} = track) do
    %__MODULE__{
      track
      | fragments: Qex.join(track.stale_fragments, track.fragments),
        current_seq_num: 0
    }
  end

  @spec all_fragments(t) :: [fragment_name :: String.t()]
  def all_fragments(%__MODULE__{} = track) do
    Qex.join(track.stale_fragments, track.fragments) |> Enum.map(& &1.name)
  end

  defp pop_stale_fragments(%__MODULE__{target_window_duration: :infinity} = track) do
    {[], track}
  end

  defp pop_stale_fragments(track) do
    %__MODULE__{
      fragments: fragments,
      window_duration: window_duration,
      target_window_duration: target_window_duration
    } = track

    {to_remove, fragments, window_duration} =
      do_pop_stale_fragments(fragments, window_duration, target_window_duration, [])

    track = %__MODULE__{track | fragments: fragments, window_duration: window_duration}
    {to_remove, track}
  end

  defp do_pop_stale_fragments(fragments, window_duration, target_window_duration, acc) do
    use Ratio, comparison: true
    {fragment, new_fragments} = Qex.pop!(fragments)
    new_window_duration = window_duration - fragment.duration

    if new_window_duration >= target_window_duration and new_window_duration > 0 do
      do_pop_stale_fragments(
        new_fragments,
        new_window_duration,
        target_window_duration,
        [fragment | acc]
      )
    else
      {Enum.reverse(acc), fragments, window_duration}
    end
  end
end
