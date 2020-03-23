defmodule Membrane.Element.HTTPAdaptiveStream.Sink do
  use Bunch
  use Membrane.Sink
  alias Membrane.Element.HTTPAdaptiveStream.{Playlist, Storage}

  def_input_pad :input,
    availability: :on_request,
    demand_unit: :buffers,
    caps: Membrane.Caps.HTTPAdaptiveStream.Track

  def_options playlist_name: [
                type: :string,
                spec: String.t(),
                default: "index"
              ],
              playlist_module: [
                type: :atom,
                spec: module
              ],
              storage: [
                type: :struct,
                spec: Storage.config_t()
              ],
              windowed?: [
                type: :bool,
                default: true
              ],
              store_permanent?: [
                type: :bool,
                default: false
              ],
              target_window_duration: [
                spec: pos_integer | :infinity | nil,
                default: nil
              ],
              target_fragment_duration: [
                type: :time,
                default: 0
              ]

  @impl true
  def handle_init(options) do
    options
    |> Map.from_struct()
    |> Map.delete(:playlist_name)
    |> Map.merge(%{
      storage: Storage.new(options.storage),
      playlist: %Playlist{name: options.playlist_name},
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
          windowed?: state.windowed,
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
    %{storage: storage, playlist: playlist, playlist_module: playlist_module} = state
    duration = buffer.metadata.duration
    {changeset, playlist} = Playlist.add_fragment(playlist, id, duration)
    playlists = playlist_module.serialize(playlist)
    state = %{state | playlist: playlist}

    with {:ok, storage} <- Storage.apply_chunk_changeset(storage, changeset, buffer.payload),
         {:ok, storage} <- Storage.store_playlists(storage, playlists) do
      {notify, state} = maybe_notify_playable(id, state)
      {{:ok, notify ++ [demand: pad]}, %{state | storage: storage}}
    else
      {error, storage} -> {error, %{state | storage: storage}}
    end
  end

  @impl true
  def handle_end_of_stream(Pad.ref(:input, id), _ctx, state) do
    %{playlist: playlist, playlist_module: playlist_module, storage: storage} = state
    playlist = Playlist.finish(playlist, id)

    {store_result, storage} =
      Storage.store_playlists(storage, playlist_module.serialize(playlist))

    storage = Storage.clear_cache(storage)
    state = %{state | storage: storage, playlist: playlist}
    {store_result, state}
  end

  @impl true
  def handle_playing_to_prepared(_ctx, %{store_permanent?: true} = state) do
    playlists = state.playlist |> Playlist.from_beginning() |> state.playlist_module.serialize()
    {result, storage} = Storage.store_playlists(state.storage, playlists)
    {result, %{state | storage: storage}}
  end

  @impl true
  def handle_playing_to_prepared(_ctx, state) do
    {:ok, state}
  end

  defp maybe_notify_playable(id, %{to_notify: to_notify} = state) do
    if MapSet.member?(to_notify, id) do
      {[notify: {:stream_playable, id}], %{state | to_notify: MapSet.delete(to_notify, id)}}
    else
      {[], state}
    end
  end
end
