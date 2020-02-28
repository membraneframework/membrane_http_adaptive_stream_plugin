defmodule Membrane.Element.HTTPAdaptiveStream.Sink do
  use Membrane.Sink
  alias FE.Maybe
  alias Membrane.Element.HTTPAdaptiveStream.HLS
  alias Membrane.Element.HTTPAdaptiveStream.Playlist
  alias Membrane.Element.HTTPAdaptiveStream.Storage

  def_input_pad :input,
    availability: :on_request,
    demand_unit: :buffers,
    caps: Membrane.Caps.HTTPAdaptiveStream.Track

  def_options playlist_name: [
                type: :string,
                spec: String.t(),
                default: "index"
              ],
              storage: [
                type: :struct,
                spec: Storage.config_t()
              ],
              max_fragments: [
                type: :integer,
                spec: pos_integer | :infinity,
                default: :infinity
              ],
              target_duration: [
                type: :time,
                default: 0
              ]

  @impl true
  def handle_init(options) do
    %__MODULE__{storage: %storage{} = storage_config} = options

    {:ok,
     %{
       storage: storage,
       storage_state: storage.init(storage_config),
       playlist: %Playlist{name: options.playlist_name},
       max_fragments: options.max_fragments,
       target_duration: options.target_duration
     }}
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
          max_size: state.max_fragments,
          max_fragment_duration: state.target_duration
        }
      )

    state = %{state | playlist: playlist}
    result = state.storage.store(init_name, caps.init, :binary, state.storage_state)
    {result, state}
  end

  @impl true
  def handle_start_of_stream(Pad.ref(:input, _) = pad, _ctx, state) do
    {{:ok, demand: pad}, state}
  end

  @impl true
  def handle_write(Pad.ref(:input, id) = pad, buffer, _ctx, state) do
    duration = buffer.metadata.duration
    {{to_add, to_remove}, playlist} = Playlist.add_fragment(state.playlist, id, duration)
    state = %{state | playlist: playlist}
    %{storage: storage, storage_state: storage_state} = state

    with :ok <-
           to_remove |> Maybe.map(&storage.remove(&1, storage_state)) |> Maybe.unwrap_or(:ok),
         :ok <- storage.store(to_add, buffer.payload, :binary, storage_state),
         :ok <- store_playlists(playlist, storage, storage_state) do
      {{:ok, demand: pad}, state}
    else
      error -> {error, state}
    end
  end

  @impl true
  def handle_end_of_stream(Pad.ref(:input, id), _ctx, state) do
    {playlist, state} = Bunch.Map.get_updated!(state, :playlist, &Playlist.finish(&1, id))
    result = store_playlists(playlist, state.storage, state.storage_state)
    {result, state}
  end

  defp store_playlists(playlist, storage, storage_state) do
    playlist
    |> HLS.Playlist.serialize()
    |> Bunch.Enum.try_each(fn {name, playlist} ->
      storage.store(name, playlist, :text, storage_state)
    end)
  end
end
