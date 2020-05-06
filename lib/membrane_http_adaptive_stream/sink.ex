defmodule Membrane.HTTPAdaptiveStream.Sink do
  @moduledoc """
  Sink for generating HTTP streaming playlists.

  Uses `Membrane.HTTPAdaptiveStream.Playlist` for playlist serialization
  and `Membrane.HTTPAdaptiveStream.Storage` for saving files.

  ## Examples

  The following configuration:

      %#{inspect(__MODULE__)}{
        playlist_name: "playlist",
        playlist_module: Membrane.HTTPAdaptiveStream.HLS.Playlist,
        storage: %Membrane.HTTPAdaptiveStream.Storages.FileStorage{directory: "output"}
      }

  will generate a HLS playlist in the `output` directory, playable from
  `output/playlist.m3u8` file.
  """

  use Bunch
  use Membrane.Sink
  alias Membrane.HTTPAdaptiveStream.{Playlist, Storage}

  def_input_pad :input,
    availability: :on_request,
    demand_unit: :buffers,
    caps: Membrane.Caps.HTTPAdaptiveStream.Track

  def_options playlist_name: [
                type: :string,
                spec: String.t(),
                default: "index",
                description: "Name of the main playlist file"
              ],
              playlist_module: [
                type: :atom,
                spec: module,
                description: """
                Implementation of the `Membrane.HTTPAdaptiveStream.Playlist`
                behaviour.
                """
              ],
              storage: [
                type: :struct,
                spec: Storage.config_t(),
                description: """
                Implementation of the `Membrane.HTTPAdaptiveStream.Storage`
                behaviour.
                """
              ],
              target_window_duration: [
                spec: pos_integer | :infinity,
                default: Membrane.Time.seconds(5),
                description: """
                Playlist duration is keept above that time, while the oldest chunks
                are removed whenever possible.
                """
              ],
              store_permanent?: [
                type: :bool,
                default: false,
                description: """
                If true, stale chunks are removed from the playlist only. Once
                playback finishes, they are put back into the playlist.
                """
              ],
              target_fragment_duration: [
                type: :time,
                default: 0,
                description: """
                Expected length of each chunk. Setting it is not necessary, but
                may help players achieve better UX.
                """
              ]

  @impl true
  def handle_init(options) do
    options
    |> Map.from_struct()
    |> Map.delete(:playlist_name)
    |> Map.delete(:playlist_module)
    |> Map.merge(%{
      storage: Storage.new(options.storage),
      playlist: %Playlist{name: options.playlist_name, module: options.playlist_module},
      to_notify: MapSet.new()
    })
    ~> {:ok, &1}
  end

  @impl true
  def handle_caps(Pad.ref(:input, id), caps, _ctx, state) do
    {init_name, playlist} =
      Playlist.add_track(
        state.playlist,
        %Playlist.Track.Config{
          id: id,
          content_type: caps.content_type,
          init_extension: caps.init_extension,
          fragment_extension: caps.fragment_extension,
          target_window_duration: state.target_window_duration,
          target_fragment_duration: state.target_fragment_duration,
          permanent?: state.store_permanent?
        }
      )

    state = %{state | playlist: playlist}
    {result, storage} = Storage.store_init(state.storage, init_name, caps.init)
    {result, %{state | storage: storage}}
  end

  @impl true
  def handle_start_of_stream(Pad.ref(:input, id) = pad, _ctx, state) do
    to_notify = MapSet.put(state.to_notify, id)
    {{:ok, demand: pad}, %{state | to_notify: to_notify}}
  end

  @impl true
  def handle_write(Pad.ref(:input, id) = pad, buffer, _ctx, state) do
    %{storage: storage, playlist: playlist} = state
    duration = buffer.metadata.duration
    {changeset, playlist} = Playlist.add_fragment(playlist, id, duration)
    state = %{state | playlist: playlist}

    with {:ok, storage} <- Storage.apply_chunk_changeset(storage, changeset, buffer.payload),
         {:ok, storage} <- serialize_and_store_playlist(playlist, storage) do
      {notify, state} = maybe_notify_playable(id, state)
      {{:ok, notify ++ [demand: pad]}, %{state | storage: storage}}
    else
      {error, storage} -> {error, %{state | storage: storage}}
    end
  end

  @impl true
  def handle_end_of_stream(Pad.ref(:input, id), _ctx, state) do
    %{playlist: playlist, storage: storage} = state
    playlist = Playlist.finish(playlist, id)
    {store_result, storage} = serialize_and_store_playlist(playlist, storage)
    storage = Storage.clear_cache(storage)
    state = %{state | storage: storage, playlist: playlist}
    {store_result, state}
  end

  @impl true
  def handle_playing_to_prepared(_ctx, state) do
    %{
      playlist: playlist,
      storage: storage,
      store_permanent?: store_permanent?
    } = state

    to_remove = Playlist.all_fragments(playlist)

    cleanup = fn ->
      {result, _storage} = Storage.cleanup(storage, to_remove)
      result
    end

    result =
      if store_permanent? do
        {result, storage} =
          playlist |> Playlist.from_beginning() |> serialize_and_store_playlist(storage)

        {result, %{state | storage: storage}}
      else
        {:ok, state}
      end

    with {:ok, state} <- result do
      {{:ok, notify: {:cleanup, cleanup}}, state}
    end
  end

  defp maybe_notify_playable(id, %{to_notify: to_notify} = state) do
    if MapSet.member?(to_notify, id) do
      {[notify: {:stream_playable, id}], %{state | to_notify: MapSet.delete(to_notify, id)}}
    else
      {[], state}
    end
  end

  defp serialize_and_store_playlist(playlist, storage) do
    playlist_files = Playlist.serialize(playlist)
    Storage.store_playlists(storage, playlist_files)
  end
end
