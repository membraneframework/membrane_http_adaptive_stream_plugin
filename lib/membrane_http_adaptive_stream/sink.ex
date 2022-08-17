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

  require Membrane.HTTPAdaptiveStream.Manifest.SegmentAttribute

  alias Membrane.CMAF
  alias Membrane.HTTPAdaptiveStream.Manifest
  alias Membrane.HTTPAdaptiveStream.Storage

  def_input_pad :input,
    availability: :on_request,
    demand_unit: :buffers,
    caps: CMAF.Track,
    options: [
      track_name: [
        spec: String.t() | nil,
        default: nil,
        description: """
        Name that will be used to name the media playlist for the given track, as well as its header and segments files.
        It must not contain any URI reserved characters
        """
      ],
      supports_partial_segments?: [
        spec: boolean(),
        default: false,
        description: """
        Decides if the incoming track will produce partial segments or not. If so then the playlist will be adjusted accordingly.
        """
      ]
    ]

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
              mode: [
                spec: :live | :vod,
                default: :vod,
                description: """
                Tells if the session is live or a VOD type of broadcast. It can influence type of metadata
                inserted into the playlist's manifest.
                """
              ],
              target_segment_duration: [
                type: :time,
                default: 0,
                description: """
                Expected length of each segment. Setting it is not necessary, but
                may help players achieve better UX.
                """
              ],
              target_partial_segment_duration: [
                spec: Membrane.Time.t() | nil,
                default: nil,
                description: """
                Expected length of each partial segment. When set to `nil` then no
                partial segments get emitted.
                """
              ],
              segment_naming_fun: [
                type: :function,
                spec: (Manifest.Track.t() -> String.t()),
                default: &Manifest.Track.default_segment_naming_fun/1,
                description:
                  "A function that generates consequent segment names for a given track"
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
      # used to keep track of partial segments that should get merged
      track_to_partial_segments: %{}
    })
    ~> {:ok, &1}
  end

  @impl true
  def handle_caps(Pad.ref(:input, track_id) = pad_ref, %CMAF.Track{} = caps, ctx, state) do
    {header_name, manifest} =
      if Manifest.has_track?(state.manifest, track_id) do
        # Arrival of new caps for an already existing track indicate that stream parameters have changed.
        # According to section 4.3.2.3 of RFC 8216, discontinuity needs to be signaled and new header supplied.
        Manifest.discontinue_track(state.manifest, track_id)
      else
        track_name = parse_track_name(ctx.pads[pad_ref].options[:track_name] || track_id)

        Manifest.add_track(
          state.manifest,
          %Manifest.Track.Config{
            id: track_id,
            track_name: track_name,
            content_type: caps.content_type,
            header_extension: ".mp4",
            segment_extension: ".m4s",
            segment_naming_fun: state.segment_naming_fun,
            target_window_duration: state.target_window_duration,
            target_segment_duration: state.target_segment_duration,
            target_partial_segment_duration: state.target_partial_segment_duration,
            persist?: state.persist?
          }
        )
      end

    {result, storage} = Storage.store_header(state.storage, header_name, caps.header)

    {result, %{state | storage: storage, manifest: manifest}}
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
  def handle_write(Pad.ref(:input, track_id) = pad, buffer, ctx, state) do
    supports_partial_segments? = ctx.pads[pad].options[:supports_partial_segments?]

    {changesets, buffers, state} =
      handle_buffer(buffer, track_id, supports_partial_segments?, state)

    %{storage: storage, manifest: manifest} = state

    with {:ok, storage} <-
           [changesets, buffers]
           |> Enum.zip()
           |> Enum.reduce_while({:ok, storage}, fn {changeset, buffer}, {:ok, storage} ->
             case Storage.apply_segment_changeset(storage, changeset, buffer) do
               {:ok, storage} -> {:cont, {:ok, storage}}
               other -> {:halt, other}
             end
           end),
         {:ok, storage} <- serialize_and_store_manifest(manifest, storage) do
      {notify, state} = maybe_notify_playable(track_id, state)
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
  def handle_playing_to_prepared(ctx, state) do
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

    track_ids =
      ctx.pads
      |> Map.keys()
      |> Enum.map(fn
        Pad.ref(:input, track_id) -> track_id
      end)

    # prevent storing empty manifest, such situation can happen
    # when the sink goes from prepared -> playing -> prepared -> stopped
    # and in the meantime no media has flown through input pads
    any_track_persisted? = Enum.any?(track_ids, &Manifest.has_track?(manifest, &1))

    result =
      if persist? and any_track_persisted? do
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

  # we are operating on partial segments which may require
  # assembling previous partial segments and creating regular one
  # before processing the current segment
  defp handle_buffer(
         buffer,
         track_id,
         true = _supports_partial_segments?,
         %{mode: :live} = state
       ) do
    %{
      duration: duration,
      independent?: independent?
    } = buffer.metadata

    partial_segments = get_in(state, [:track_to_partial_segments, track_id]) || []
    partial_segments_duration = Enum.reduce(partial_segments, 0, &(&1.metadata.duration + &2))

    {full_segment, changeset, manifest, partial_segments} =
      if independent? and duration + partial_segments_duration >= state.target_segment_duration and
           partial_segments != [] do
        payload = %Membrane.Buffer{
          payload:
            partial_segments |> Enum.map(& &1.payload) |> Enum.reverse() |> IO.iodata_to_binary(),
          metadata: %{
            duration: partial_segments_duration
          }
        }

        {changeset, manifest} = Manifest.finalize_last_segment(state.manifest, track_id)

        {payload, changeset, manifest, []}
      else
        {nil, nil, state.manifest, partial_segments}
      end

    new_segment_necessary? = Enum.empty?(partial_segments)

    bytes_offset = Enum.reduce(partial_segments, 0, &(byte_size(&1.payload) + &2))
    partial_segments = [buffer | partial_segments]

    state = put_in(state, [:track_to_partial_segments, track_id], partial_segments)

    manifest =
      if new_segment_necessary? do
        {_changeset, manifest} =
          Manifest.add_segment(
            manifest,
            track_id,
            0,
            0,
            [partial?: true] ++ creation_time(state.mode)
          )

        manifest
      else
        manifest
      end

    byte_range = {byte_size(buffer.payload), bytes_offset}

    buffer = %Membrane.Buffer{
      buffer
      | metadata: Map.put(buffer.metadata, :byte_range, byte_range)
    }

    manifest
    |> Manifest.add_partial_segment(
      track_id,
      independent?,
      duration,
      byte_range: byte_range,
      independent: independent?
    )
    |> then(fn {new_changeset, manifest} ->
      {reject_nils([changeset, new_changeset]), reject_nils([full_segment, buffer]),
       %{state | manifest: manifest}}
    end)
  end

  # we are operating on regular segments
  defp handle_buffer(
         buffer,
         track_id,
         false = _supports_partial_segments?,
         state
       ) do
    duration = buffer.metadata.duration

    {changeset, manifest} =
      Manifest.add_segment(
        state.manifest,
        track_id,
        duration,
        byte_size(buffer.payload),
        creation_time(state.mode)
      )

    {[changeset], [buffer], %{state | manifest: manifest}}
  end

  defp parse_track_name(track_id) when is_binary(track_id) do
    valid_filename_regex = ~r/^[^\/:*?"<>|]+$/

    if String.match?(track_id, valid_filename_regex) do
      track_id
    else
      raise ArgumentError,
        message: "Manually defined track identifiers should be valid file names."
    end
  end

  defp parse_track_name(track_id) do
    track_id |> :erlang.term_to_binary() |> Base.url_encode64(padding: false)
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

  defp creation_time(:live), do: [{:creation_time, DateTime.utc_now()}]
  defp creation_time(:vod), do: []

  defp reject_nils(enumerable), do: Enum.reject(enumerable, &is_nil/1)
end
