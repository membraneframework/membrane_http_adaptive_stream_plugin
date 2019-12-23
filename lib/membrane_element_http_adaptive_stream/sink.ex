defmodule Membrane.Element.HTTPAdaptiveStream.Sink do
  use Membrane.Sink
  alias FE.Maybe
  alias Membrane.Element.HTTPAdaptiveStream.Storage
  alias Membrane.Element.HTTPAdaptiveStream.Playlist.HLS, as: Playlist

  def_input_pad :input, demand_unit: :buffers, caps: Membrane.Caps.HTTPAdaptiveStream.Channel

  @impl true
  def handle_init(_) do
    storage_config = %storage{} = %Storage.File{location: "../hls/mp4/mbout"}

    {:ok,
     %{
       storage: storage,
       storage_config: storage_config,
       playlist_name: "index.m3u8",
       playlist: nil
     }}
  end

  @impl true
  def handle_prepared_to_playing(_ctx, state) do
    {{:ok, demand: :input}, state}
  end

  @impl true
  def handle_start_of_stream(:input, _ctx, state) do
    {{:ok, start_timer: {:timer, :wait}}, state}
  end

  @impl true
  def handle_write(:input, buffer, _ctx, state) do
    duration = buffer.metadata.duration
    {{add, rem}, playlist} = Playlist.put(state.playlist, duration)
    state = %{state | playlist: playlist}
    %{storage: storage, storage_config: storage_config} = state

    with :ok <- rem |> Maybe.map(&storage.remove(&1, storage_config)) |> Maybe.unwrap_or(:ok),
         :ok <- storage.store(add, buffer.payload, :binary, storage_config),
         :ok <- store_playlist(playlist, state.playlist_name, storage, storage_config) do
      {{:ok, timer_interval: {:timer, duration}}, state}
    else
      error -> {error, state}
    end
  end

  @impl true
  def handle_tick(:timer, _ctx, state) do
    {{:ok, timer_interval: {:timer, :wait}, demand: :input}, state}
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
    result = state.storage.store(caps.init_name, caps.init, :binary, state.storage_config)
    {result, state}
  end

  @impl true
  def handle_event(pad, event, ctx, state) do
    super(pad, event, ctx, state)
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    {playlist, state} = Bunch.Map.get_updated!(state, :playlist, &Playlist.finish/1)
    result = store_playlist(playlist, state.playlist_name, state.storage, state.storage_config)
    {result, state}
  end

  defp store_playlist(playlist, playlist_name, storage, storage_config) do
    storage.store(playlist_name, Playlist.serialize(playlist), :text, storage_config)
  end
end
