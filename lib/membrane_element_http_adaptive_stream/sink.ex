defmodule Membrane.Element.HTTPAdaptiveStream.Sink do
  use Membrane.Sink
  alias FE.Maybe
  alias Membrane.Element.HTTPAdaptiveStream.Storage
  alias Membrane.Element.HTTPAdaptiveStream.Playlist.HLS, as: Playlist

  def_input_pad :input, demand_unit: :buffers, caps: Membrane.Caps.HTTPAdaptiveStream.Channel

  def_options playlist_name: [
                type: :string,
                spec: String.t(),
                default: "index"
              ],
              storage: [
                type: :struct,
                spec: Storage.config_t()
              ]

  @impl true
  def handle_init(options) do
    %__MODULE__{playlist_name: playlist_name, storage: %storage{} = storage_config} = options

    {:ok,
     %{
       storage: storage,
       storage_state: storage.init(storage_config),
       playlist_name: "#{playlist_name}#{Playlist.playlist_extension()}",
       playlist: nil
     }}
  end

  @impl true
  def handle_prepared_to_playing(_ctx, state) do
    {{:ok, demand: :input}, state}
  end

  @impl true
  def handle_write(:input, buffer, _ctx, state) do
    duration = buffer.metadata.duration
    {{to_add, to_remove}, playlist} = Playlist.put(state.playlist, duration)
    state = %{state | playlist: playlist}
    %{storage: storage, storage_state: storage_state} = state

    with :ok <-
           to_remove |> Maybe.map(&storage.remove(&1, storage_state)) |> Maybe.unwrap_or(:ok),
         :ok <- storage.store(to_add, buffer.payload, :binary, storage_state),
         :ok <- store_playlist(playlist, state.playlist_name, storage, storage_state) do
      {{:ok, demand: :input}, state}
    else
      error -> {error, state}
    end
  end

  @impl true
  def handle_caps(:input, caps, _ctx, state) do
    playlist =
      Playlist.new(%Playlist.Config{
        init_name: caps.init_name,
        fragment_prefix: caps.fragment_prefix,
        fragment_extension: caps.fragment_extension
      })

    state = %{state | playlist: playlist}
    result = state.storage.store(caps.init_name, caps.init, :binary, state.storage_state)
    {result, state}
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    {playlist, state} = Bunch.Map.get_updated!(state, :playlist, &Playlist.finish/1)
    result = store_playlist(playlist, state.playlist_name, state.storage, state.storage_state)
    {result, state}
  end

  defp store_playlist(playlist, playlist_name, storage, storage_state) do
    storage.store(playlist_name, Playlist.serialize(playlist), :text, storage_state)
  end
end
