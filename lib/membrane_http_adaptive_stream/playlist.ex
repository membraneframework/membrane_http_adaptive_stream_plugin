defmodule Membrane.HTTPAdaptiveStream.Playlist do
  @moduledoc """
  Behaviour for playlist serialization.
  """
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

  @doc """
  Adds a track to the playlist.

  Returns the name under which the init file should be stored.
  """
  @spec add_track(t, Track.Config.t()) :: {init_name :: String.t(), t}
  def add_track(playlist, %Track.Config{} = config) do
    track = Track.new(config)
    playlist = %__MODULE__{playlist | tracks: Map.put(playlist.tracks, config.id, track)}
    {track.init_name, playlist}
  end

  @spec add_fragment(t, track_id :: Track.id_t(), Track.fragment_duration_t()) ::
          {{to_add_name :: String.t(), to_remove_names :: [String.t()]}, t}
  def add_fragment(%__MODULE__{} = playlist, track_id, duration) do
    get_and_update_in(
      playlist,
      [:tracks, track_id],
      &Track.add_fragment(&1, duration)
    )
  end

  @spec serialize(t) :: [{name :: String.t(), playlist :: String.t()}]
  def serialize(%__MODULE__{module: module} = playlist) do
    module.serialize(playlist)
  end

  @spec finish(t, Track.id_t()) :: t
  def finish(%__MODULE__{} = playlist, track_id) do
    update_in(playlist, [:tracks, track_id], &Track.finish/1)
  end

  @doc """
  Restores all the stale chunks in all tracks.

  All the tracks must be configured to be 'permanent'.
  """
  def from_beginning(%__MODULE__{} = playlist) do
    tracks = Bunch.Map.map_values(playlist.tracks, &Track.from_beginning/1)
    %__MODULE__{playlist | tracks: tracks}
  end

  @doc """
  Returns stale and current fragments' names from all tracks
  """
  def all_fragments(%__MODULE__{} = playlist) do
    playlist.tracks |> Map.values() |> Enum.flat_map(&Track.all_fragments/1)
  end
end
