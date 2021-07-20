defmodule Membrane.HTTPAdaptiveStream.Sink do
  @moduledoc """
  Sink for generating HTTP streaming manifests.

  Uses `Membrane.HTTPAdaptiveStream.Manifest` for manifest serialization
  and `Membrane.HTTPAdaptiveStream.Storage` for saving files.

  ## Notifications

  - `{:track_playable, input_pad_id}` - sent when the first segment of a track is
    stored, and thus the track is ready to be played
  - `{:cleanup, cleanup_function :: (()-> :ok)}` - sent when playback changes
    from playing to prepared. Invoking `cleanup_function` lambda results in removing
    all the files that remain after the streaming

  ## Examples

  The following configuration:

      %#{inspect(__MODULE__)}{
        manifest_name: "manifest",
        manifest_module: Membrane.HTTPAdaptiveStream.HLS,
        storage: %Membrane.HTTPAdaptiveStream.Storages.FileStorage{directory: "output"}
      }

  will generate a HLS manifest in the `output` directory, playable from
  `output/manifest.m3u8` file.
  """

  use Bunch
  use Membrane.Sink
  alias Membrane.CMAF
  alias Membrane.HTTPAdaptiveStream.{Manifest, Storage}

  def_input_pad :input,
    availability: :on_request,
    demand_unit: :buffers,
    caps: CMAF.Track

  def_options manifest_name: [
                type: :string,
                spec: String.t(),
                default: "index",
                description: "Name of the main manifest file"
              ],
              manifest_module: [
                type: :atom,
                spec: module,
                description: """
                Implementation of the `Membrane.HTTPAdaptiveStream.Manifest`
                behaviour.
                """
              ],
              storage: [
                type: :struct,
                spec: Storage.config_t(),
                description: """
                Storage configuration. May be one of `Membrane.HTTPAdaptiveStream.Storages.*`.
                See `Membrane.HTTPAdaptiveStream.Storage` behaviour.
                """
              ],
              target_window_duration: [
                spec: pos_integer | :infinity,
                type: :time,
                default: Membrane.Time.seconds(40),
                description: """
                Manifest duration is keept above that time, while the oldest segments
                are removed whenever possible.
                """
              ],
              persist?: [
                type: :bool,
                default: false,
                description: """
                If true, stale segments are removed from the manifest only. Once
                playback finishes, they are put back into the manifest.
                """
              ],
              target_segment_duration: [
                type: :time,
                default: 0,
                description: """
                Expected length of each segment. Setting it is not necessary, but
                may help players achieve better UX.
                """
              ]

  @impl true
  def handle_init(options) do
    options
    |> Map.from_struct()
    |> Map.delete(:manifest_name)
    |> Map.delete(:manifest_module)
    |> Map.merge(%{
      storage: Storage.new(options.storage),
      manifest: %Manifest{name: options.manifest_name, module: options.manifest_module},
      awaiting_first_segment: MapSet.new(),
      awaiting_discontinuities: %{}
    })
    ~> {:ok, &1}
  end

  @impl true
  def handle_caps(Pad.ref(:input, track_id), %CMAF.Track{} = caps, _ctx, state) do
    {header_name, manifest, awaiting_discontinuity} =
      if Manifest.has_track?(state.manifest, track_id) do
        {{header_name, discontinuity_seq}, manifest} =
          Manifest.discontinue_track(state.manifest, track_id)

        discontinuity = {:discontinuity, header_name, discontinuity_seq}

        {header_name, manifest, discontinuity}
      else
        {header_name, manifest} =
          Manifest.add_track(
            state.manifest,
            %Manifest.Track.Config{
              id: track_id,
              content_type: caps.content_type,
              header_extension: ".mp4",
              segment_extension: ".m4s",
              target_window_duration: state.target_window_duration,
              target_segment_duration: state.target_segment_duration,
              persist?: state.persist?
            }
          )

        {header_name, manifest, nil}
      end

    {result, storage} = Storage.store_header(state.storage, header_name, caps.header)

    {result,
     %{
       state
       | storage: storage,
         manifest: manifest,
         awaiting_discontinuities:
           Map.put(state.awaiting_discontinuities, track_id, awaiting_discontinuity)
     }}
  end

  @impl true
  def handle_prepared_to_playing(ctx, state) do
    demands = ctx.pads |> Map.keys() |> Enum.map(&{:demand, &1})
    {{:ok, demands}, state}
  end

  @impl true
  def handle_pad_added(pad, %{playback_state: :playing}, state) do
    {{:ok, demand: pad}, state}
  end

  @impl true
  def handle_pad_added(_pad, _ctx, state) do
    {:ok, state}
  end

  @impl true
  def handle_start_of_stream(Pad.ref(:input, id), _ctx, state) do
    awaiting_first_segment = MapSet.put(state.awaiting_first_segment, id)
    {:ok, %{state | awaiting_first_segment: awaiting_first_segment}}
  end

  @impl true
  def handle_write(Pad.ref(:input, id) = pad, buffer, _ctx, state) do
    %{storage: storage, manifest: manifest} = state
    duration = buffer.metadata.duration

    segment_attributes =
      case Map.get(state.awaiting_discontinuities, id) do
        nil ->
          []

        discontinuity ->
          [discontinuity]
      end

    {changeset, manifest} = Manifest.add_segment(manifest, id, duration, segment_attributes)

    state = %{
      state
      | manifest: manifest,
        awaiting_discontinuities: Map.delete(state.awaiting_discontinuities, id)
    }

    with {:ok, storage} <- Storage.apply_segment_changeset(storage, changeset, buffer.payload),
         {:ok, storage} <- serialize_and_store_manifest(manifest, storage) do
      {notify, state} = maybe_notify_playable(id, state)
      {{:ok, notify ++ [demand: pad]}, %{state | storage: storage}}
    else
      {error, storage} -> {error, %{state | storage: storage}}
    end
  end

  @impl true
  def handle_end_of_stream(Pad.ref(:input, id), _ctx, state) do
    %{manifest: manifest, storage: storage} = state
    manifest = Manifest.finish(manifest, id)
    {store_result, storage} = serialize_and_store_manifest(manifest, storage)
    storage = Storage.clear_cache(storage)
    state = %{state | storage: storage, manifest: manifest}
    {store_result, state}
  end

  @impl true
  def handle_playing_to_prepared(_ctx, state) do
    %{
      manifest: manifest,
      storage: storage,
      persist?: persist?
    } = state

    to_remove = Manifest.all_segments(manifest)

    cleanup = fn ->
      {result, _storage} = Storage.cleanup(storage, to_remove)
      result
    end

    result =
      if persist? do
        {result, storage} =
          manifest |> Manifest.from_beginning() |> serialize_and_store_manifest(storage)

        {result, %{state | storage: storage}}
      else
        {:ok, state}
      end

    with {:ok, state} <- result do
      {{:ok, notify: {:cleanup, cleanup}}, state}
    end
  end

  defp maybe_notify_playable(id, %{awaiting_first_segment: awaiting_first_segment} = state) do
    if MapSet.member?(awaiting_first_segment, id) do
      {[notify: {:track_playable, id}],
       %{state | awaiting_first_segment: MapSet.delete(awaiting_first_segment, id)}}
    else
      {[], state}
    end
  end

  defp serialize_and_store_manifest(manifest, storage) do
    manifest_files = Manifest.serialize(manifest)
    Storage.store_manifests(storage, manifest_files)
  end
end
