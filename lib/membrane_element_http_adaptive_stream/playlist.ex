defmodule Membrane.Element.HTTPAdaptiveStream.Playlist do
  use Bunch.Access
  alias __MODULE__.Track

  @callback serialize(t) :: [{playlist_name :: String.t(), playlist_content :: String.t()}]

  @type t :: %__MODULE__{
          name: String.t(),
          module: module,
          tracks: %{(id :: any) => Track.t()}
        }

  @enforce_keys [:name, :module]
  defstruct @enforce_keys ++ [tracks: %{}]

  def add_track(playlist, %Track.Config{} = config) do
    track = Track.new(config)
    playlist = %__MODULE__{playlist | tracks: Map.put(playlist.tracks, config.id, track)}
    {track.init_name, playlist}
  end

  def add_fragment(%__MODULE__{} = playlist, track_id, duration) do
    get_and_update_in(
      playlist,
      [:tracks, track_id],
      &Track.add_fragment(&1, duration)
    )
  end

  def serialize(%__MODULE__{module: module} = playlist) do
    module.serialize(playlist)
  end

  def finish(%__MODULE__{} = playlist, track_id) do
    update_in(playlist, [:tracks, track_id], &Track.finish/1)
  end

  def from_beginning(%__MODULE__{} = playlist) do
    tracks = Bunch.Map.map_values(playlist.tracks, &Track.from_beginning/1)
    %__MODULE__{playlist | tracks: tracks}
  end

  def all_fragments(%__MODULE__{} = playlist) do
    playlist.tracks |> Map.values() |> Enum.flat_map(&Track.all_fragments/1)
  end
end
