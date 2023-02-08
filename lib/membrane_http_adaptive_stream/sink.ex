defmodule Membrane.HTTPAdaptiveStream.Sink do
  @moduledoc """
  Sink for generating HTTP streaming manifests.

  Uses `Membrane.HTTPAdaptiveStream.Manifest` for manifest serialization
  and `Membrane.HTTPAdaptiveStream.Storage` for saving files.

  ## Notifications

  - `{:track_playable, input_pad_id}` - sent when the first segment of a track is
    stored, and thus the track is ready to be played

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

  use Membrane.Sink

  require Membrane.HTTPAdaptiveStream.Manifest.SegmentAttribute

  alias Membrane.CMAF
  alias Membrane.HTTPAdaptiveStream.Manifest
  alias Membrane.HTTPAdaptiveStream.Storage

  defmodule SegmentDuration do
    @moduledoc """
    Module representing a segment duration range that should appear
    in a playlist.

    The minimum and target durations are relevant if sink
    is set to work in low latency mode as the durations of partial
    segment can greatly vary.
    """

    alias Membrane.Time

    @enforce_keys [:min, :target]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            min: Time.t(),
            target: Time.t()
          }

    @doc """
    Creates a new segment duration with a minimum and target duration.
    """
    @spec new(Time.t(), Time.t()) :: t()
    def new(min, target) when min <= target,
      do: %__MODULE__{min: min, target: target}

    @doc """
    Creates a new segment duration with a target duration.

    The minimum duration is set to the target one.
    """
    @spec new(Time.t()) :: t()
    def new(target),
      do: %__MODULE__{min: target, target: target}
  end

  def_input_pad :input,
    availability: :on_request,
    demand_unit: :buffers,
    accepted_format: CMAF.Track,
    options: [
      track_name: [
        spec: String.t() | nil,
        default: nil,
        description: """
        Name that will be used to name the media playlist for the given track, as well as its header and segments files.
        It must not contain any URI reserved characters.
        """
      ],
      segment_duration: [
        spec: SegmentDuration.t(),
        description: """
        The expected minimum and target duration of media segments produced by this particular track.

        In case of regular paced streams the parameter may not have any impact, but when
        partial segments gets used it may decide when regular segments gets finalized and new gets started.
        """
      ],
      target_partial_segment_duration: [
        spec: Membrane.Time.t() | nil,
        default: nil,
        description: """
        The target duration of partial segments.

        When set to nil then the track is not supposed to emit partial segments.
        """
      ]
    ]

  def_options manifest_name: [
                spec: String.t(),
                default: "index",
                description: "Name of the main manifest file."
              ],
              manifest_module: [
                spec: module(),
                description:
                  "Implementation of the `Membrane.HTTPAdaptiveStream.Manifest` behaviour."
              ],
              storage: [
                spec: Storage.config_t(),
                description: """
                Storage configuration. May be one of `Membrane.HTTPAdaptiveStream.Storages.*`.
                See `Membrane.HTTPAdaptiveStream.Storage` behaviour.
                """
              ],
              target_window_duration: [
                spec: Membrane.Time.t() | :infinity,
                default: Membrane.Time.seconds(40),
                description: """
                Manifest duration is keept above that time, while the oldest segments
                are removed whenever possible.
                """
              ],
              persist?: [
                spec: boolean(),
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
              header_naming_fun: [
                spec: (Manifest.Track.t(), counter :: non_neg_integer -> String.t()),
                default: &Manifest.Track.default_header_naming_fun/2,
                description:
                  "A function that generates consequent header names for a given track."
              ],
              segment_naming_fun: [
                spec: (Manifest.Track.t() -> String.t()),
                default: &Manifest.Track.default_segment_naming_fun/1,
                description:
                  "A function that generates consequent segment names for a given track."
              ],
              cleanup_after: [
                spec: nil | Membrane.Time.t(),
                default: nil,
                description: """
                If not `nil`, time after a storage cleanup function should run.

                The function will remove all manifests and segments stored during the stream.
                """
              ]

  @impl true
  def handle_init(_ctx, options) do
    state =
      options
      |> Map.from_struct()
      |> Map.drop([:manifest_name, :manifest_module])
      |> Map.merge(%{
        storage: Storage.new(options.storage),
        manifest: %Manifest{name: options.manifest_name, module: options.manifest_module},
        awaiting_first_segment: MapSet.new(),
        # used to keep track of partial segments that should get merged
        track_to_partial_segments: %{}
      })

    {[], state}
  end

  @impl true
  def handle_stream_format(
        Pad.ref(:input, track_id) = pad_ref,
        %CMAF.Track{} = stream_format,
        ctx,
        state
      ) do
    {header_name, manifest} =
      if Manifest.has_track?(state.manifest, track_id) do
        # Arrival of new stream format for an already existing track indicate that stream parameters have changed.
        # According to section 4.3.2.3 of RFC 8216, discontinuity needs to be signaled and new header supplied.
        Manifest.discontinue_track(state.manifest, track_id)
      else
        track_options = ctx.pads[pad_ref].options
        track_name = serialize_track_name(track_options[:track_name] || track_id)

        Manifest.add_track(
          state.manifest,
          %Manifest.Track.Config{
            id: track_id,
            track_name: track_name,
            content_type: stream_format.content_type,
            header_extension: ".mp4",
            segment_extension: ".m4s",
            header_naming_fun: state.header_naming_fun,
            segment_naming_fun: state.segment_naming_fun,
            target_window_duration: state.target_window_duration,
            target_segment_duration: track_options.segment_duration.target,
            target_partial_segment_duration: track_options.target_partial_segment_duration,
            persist?: state.persist?
          }
        )
      end

    case Storage.store_header(state.storage, track_id, header_name, stream_format.header) do
      {:ok, storage} ->
        {[], %{state | storage: storage, manifest: manifest}}

      {{:error, reason}, _storage} ->
        raise "Failed to store the header for track #{inspect(track_id)} due to #{inspect(reason)}"
    end
  end

  @impl true
  def handle_playing(ctx, state) do
    demands = ctx.pads |> Map.keys() |> Enum.map(&{:demand, &1})
    {demands, state}
  end

  @impl true
  def handle_pad_added(pad, %{playback_state: :playing}, state) do
    {[demand: pad], state}
  end

  @impl true
  def handle_pad_added(_pad, _ctx, state) do
    {[], state}
  end

  @impl true
  def handle_start_of_stream(Pad.ref(:input, id), _ctx, state) do
    awaiting_first_segment = MapSet.put(state.awaiting_first_segment, id)
    {[], %{state | awaiting_first_segment: awaiting_first_segment}}
  end

  @impl true
  def handle_write(Pad.ref(:input, track_id) = pad, buffer, ctx, state) do
    %{
      target_partial_segment_duration: target_partial_segment_duration,
      segment_duration: segment_duration
    } = ctx.pads[pad].options

    supports_partial_segments? = target_partial_segment_duration != nil

    {changesets, buffers, state} =
      if supports_partial_segments? do
        handle_partial_segment(buffer, track_id, segment_duration, state)
      else
        handle_segment(buffer, track_id, state)
      end

    %{storage: storage, manifest: manifest} = state

    with {:ok, storage} <-
           [changesets, buffers]
           |> Enum.zip()
           |> Bunch.Enum.try_reduce(storage, fn {changeset, buffer}, storage ->
             Storage.apply_segment_changeset(storage, track_id, changeset, buffer)
           end),
         {:ok, storage} <- serialize_and_store_manifest(manifest, storage) do
      {notify, state} = maybe_notify_playable(track_id, state)
      {notify ++ [demand: pad], %{state | storage: storage}}
    else
      {{:error, reason}, _storage} ->
        raise "Failed to store a buffer for track #{inspect(track_id)} due to #{inspect(reason)}"
    end
  end

  @impl true
  def handle_end_of_stream(Pad.ref(:input, track_id), _ctx, state) do
    {storage, manifest} = maybe_finalize_segment_on_eos(track_id, state)

    manifest = Manifest.finish(manifest, track_id)

    case serialize_and_store_manifest(manifest, storage) do
      {:ok, storage} ->
        storage = Storage.clear_cache(storage)
        {[], %{state | storage: storage, manifest: manifest}}

      {{:error, reason}, _storage} ->
        raise "Failed to store the finalized manifest for track #{inspect(track_id)} due to #{inspect(reason)}"
    end
  end

  @impl true
  def handle_terminate_request(ctx, state) do
    %{
      manifest: manifest,
      storage: storage,
      persist?: persist?
    } = state

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
          manifest
          |> Manifest.from_beginning()
          |> serialize_and_store_manifest(storage)

        {result, %{state | storage: storage}}
      else
        {:ok, state}
      end

    case result do
      {:ok, state} ->
        :ok = maybe_schedule_cleanup_task(state)

        {[terminate: :normal], state}

      {{:error, reason}, _state} ->
        raise "Failed to persist the manifest due to #{inspect(reason)}"
    end
  end

  # we are operating on partial segments which may require
  # assembling previous partial segments and creating regular one
  # before processing the current segment
  defp handle_partial_segment(
         buffer,
         track_id,
         segment_duration,
         %{mode: :live} = state
       ) do
    {partials, partials_duration} = partial_segments_for_track(track_id, state)

    {full_segment, changeset, manifest, partials} =
      if should_finalize_segment?(buffer, partials, partials_duration, segment_duration) do
        {segment, changeset, manifest} =
          finalize_segment_for_track(track_id, partials, partials_duration, state.manifest)

        {segment, changeset, manifest, []}
      else
        {nil, nil, state.manifest, partials}
      end

    new_segment_necessary? = Enum.empty?(partials)

    state = put_in(state, [:track_to_partial_segments, track_id], [buffer | partials])

    manifest =
      if new_segment_necessary? do
        {_changeset, manifest} =
          Manifest.add_segment(
            manifest,
            track_id,
            [
              complete?: false,
              duration: buffer.metadata.duration,
              byte_size: byte_size(buffer.payload)
            ],
            creation_time(state.mode)
          )

        manifest
      else
        manifest
      end

    manifest
    |> Manifest.add_partial_segment(
      track_id,
      buffer.metadata.independent?,
      buffer.metadata.duration,
      byte_size(buffer.payload)
    )
    |> then(fn {new_changeset, manifest} ->
      changesets = if is_nil(changeset), do: [new_changeset], else: [changeset, new_changeset]
      segments = if is_nil(full_segment), do: [buffer], else: [full_segment, buffer]

      {changesets, segments, %{state | manifest: manifest}}
    end)
  end

  # we are operating on regular segments
  defp handle_segment(
         buffer,
         track_id,
         state
       ) do
    duration = buffer.metadata.duration

    {changeset, manifest} =
      Manifest.add_segment(
        state.manifest,
        track_id,
        [duration: duration, byte_size: byte_size(buffer.payload)],
        creation_time(state.mode)
      )

    {[changeset], [buffer], %{state | manifest: manifest}}
  end

  defp serialize_track_name(track_id) when is_binary(track_id) do
    valid_filename_regex = ~r/^[^\/:*?"<>|]+$/

    if String.match?(track_id, valid_filename_regex) do
      track_id
    else
      raise "The provided track identifier #{inspect(track_id)} is not a valid filename"
    end
  end

  defp serialize_track_name(track_id) do
    track_id |> :erlang.term_to_binary() |> Base.url_encode64(padding: false)
  end

  defp maybe_notify_playable(id, %{awaiting_first_segment: awaiting_first_segment} = state) do
    if MapSet.member?(awaiting_first_segment, id) do
      {[notify_parent: {:track_playable, id}],
       %{state | awaiting_first_segment: MapSet.delete(awaiting_first_segment, id)}}
    else
      {[], state}
    end
  end

  defp serialize_and_store_manifest(manifest, storage) do
    %{master_manifest: master_manifest, manifest_per_track: manifest_per_track} =
      Manifest.serialize(manifest)

    manifest_files = [{:master, master_manifest} | Map.to_list(manifest_per_track)]

    Storage.store_manifests(storage, manifest_files)
  end

  defp total_partial_segments_duration(partials),
    do: Enum.reduce(partials, 0, &(&1.metadata.duration + &2))

  defp partial_segments_for_track(track_id, state) do
    partial_segments = Map.get(state.track_to_partial_segments, track_id, [])

    {partial_segments, total_partial_segments_duration(partial_segments)}
  end

  defp should_finalize_segment?(partial_buffer, partials, partials_duration, segment_duration) do
    %{independent?: independent?, duration: duration} = partial_buffer.metadata

    partials != [] and independent? and duration + partials_duration > segment_duration.min
  end

  defp finalize_segment_for_track(track_id, partials, partials_duration, manifest) do
    payload =
      partials
      |> Enum.map(& &1.payload)
      |> Enum.reverse()
      |> IO.iodata_to_binary()

    segment = %Membrane.Buffer{
      payload: payload,
      metadata: %{duration: partials_duration}
    }

    {changeset, manifest} = Manifest.finalize_current_segment(manifest, track_id)

    {segment, changeset, manifest}
  end

  defp maybe_finalize_segment_on_eos(track_id, state) do
    %{manifest: manifest, storage: storage} = state

    {partials, partials_duration} = partial_segments_for_track(track_id, state)

    if partials == [] do
      {storage, manifest}
    else
      {segment, changeset, manifest} =
        finalize_segment_for_track(track_id, partials, partials_duration, manifest)

      case Storage.apply_segment_changeset(storage, track_id, changeset, segment) do
        {:ok, storage} ->
          {storage, manifest}

        {{:error, reason}, _storage} ->
          raise "Failed to apply segment changeset for track #{inspect(track_id)} due to #{inspect(reason)}"
      end
    end
  end

  defp creation_time(:live), do: [{:creation_time, DateTime.utc_now()}]
  defp creation_time(:vod), do: []

  defp maybe_schedule_cleanup_task(state)
  defp maybe_schedule_cleanup_task(%{cleanup_after: nil}), do: :ok

  defp maybe_schedule_cleanup_task(%{
         manifest: manifest,
         storage: storage,
         cleanup_after: cleanup_after
       }) do
    {:ok, _pid} =
      Task.start(fn ->
        to_remove = Manifest.all_segments_per_track(manifest)
        timeout = Membrane.Time.as_milliseconds(cleanup_after)

        Process.sleep(timeout)

        # cleanup all data of the secondary playlist and the master one
        with {:ok, storage} <- Storage.clean_all_track_segments(storage, to_remove),
             {:ok, _storage} <- Storage.cleanup(storage, :master, []) do
          :ok
        else
          {{:error, reason}, _storage} ->
            raise "Failed to cleanup the storage due to #{inspect(reason)}"
        end
      end)

    :ok
  end
end
