defmodule Membrane.Element.HTTPAdaptiveStream.Playlist do
  alias __MODULE__.Track

  @callback serialize(t) :: [{playlist_name :: String.t(), playlist_content :: String.t()}]

  @type t :: %__MODULE__{
          name: String.t(),
          tracks: %{(id :: any) => Track.t()}
        }

  @enforce_keys [:name]
  defstruct @enforce_keys ++ [tracks: %{}]

  def add_track(playlist, %Track.Config{} = config) do
    track = Track.new(config)
    playlist = %__MODULE__{playlist | tracks: Map.put(playlist.tracks, config.id, track)}
    {track.init_name, playlist}
  end

  def add_fragment(%__MODULE__{} = playlist, track_id, duration) do
    Bunch.Struct.get_and_update_in(
      playlist,
      [:tracks, track_id],
      &Track.add_fragment(&1, duration)
    )
  end

  def finish(playlist) do
    %__MODULE__{playlist | tracks: Map.new(playlist.tracks, &Track.finish/1)}
  end
end
