defmodule Membrane.HTTPAdaptiveStream.Source do
  @moduledoc """
  A Membrane Source element that fetches and demuxes HLS streams.

  It uses the ExHLS library to handle the HLS protocol.

  It is recommended to plug `Membrane.H264.Parser` and `Membrane.AAC.Parser`
  after this element to parse the video and audio streams respectively,
  because the stream formats returned by this element can differ depending
  on the type of the HLS stream (MPEG-TS or fMP4).
  """

  use Membrane.Source
  require Membrane.Pad, as: Pad

  alias __MODULE__.ClientGenServer

  alias Membrane.{
    AAC,
    Buffer,
    H264,
    RemoteStream
  }

  def_output_pad :video_output,
    accepted_format: any_of(H264, %RemoteStream{content_format: H264}),
    flow_control: :manual,
    demand_unit: :buffers,
    availability: :on_request,
    max_instances: 1

  def_output_pad :audio_output,
    accepted_format: any_of(AAC, %RemoteStream{content_format: AAC}),
    flow_control: :manual,
    demand_unit: :buffers,
    availability: :on_request,
    max_instances: 1

  @variant_selection_policy_description """
  The policy used to select a variant from the list of available variants.

  The policy can be one of the predefined ones or a custom function that takes a map of
  variant IDs to their descriptions and returns the ID of the selected variant.

  The predefined policies are:
  - `:lowest_resolution` - selects the variant with the lowest value of video width * height.
  - `:highest_resolution` - selects the variant with the highest value of video width * height.
  - `:lowest_bandwidth` - selects the variant with the lowest bandwidth.
  - `:highest_bandwidth` - selects the variant with the highest bandwidth.
  """

  @typedoc @variant_selection_policy_description
  @type variant_selection_policy() ::
          :lowest_resolution
          | :highest_resolution
          | :lowest_bandwidth
          | :highest_bandwidth
          | (variants_map :: %{integer() => ExHLS.Client.variant_description()} ->
               variant_id :: integer())

  @typedoc """
  Notification sent by #{inspect(__MODULE__)} to its parent when the element figures out what
  tracks are present in the HLS stream.

  Contains pads that should be linked to the element and stream formats that will be sent via
  those pads.

  If pads are linked before the element enters the `:playing` playback, the notification will
  not be sent, but the pads will have to match the tracks in the HLS stream.
  """
  @type new_tracks_notification() ::
          {:new_tracks,
           [
             {:audio_output, RemoteStream.t() | AAC.t()}
             | {:video_output, RemoteStream.t() | H264.t()}
           ]}

  def_options url: [
                spec: String.t(),
                description: "URL of the HLS playlist manifest"
              ],
              buffered_stream_time: [
                spec: Membrane.Time.t(),
                default: Membrane.Time.seconds(5),
                inspector: &Membrane.Time.inspect/1,
                description: """
                Amount of time of stream, that will be buffered by #{inspect(__MODULE__)}.

                Defaults to 5 seconds.

                Due to implementation details, the amount of the buffered stream might
                be slightly different than specified value.
                """
              ],
              variant_selection_policy: [
                spec: variant_selection_policy(),
                default: :highest_resolution,
                description: """
                #{@variant_selection_policy_description}

                Defaults to `:highest_resolution`.
                """
              ],
              how_much_to_skip: [
                spec: Membrane.Time.t(),
                default: Membrane.Time.seconds(0),
                description: """
                Specifies how much time should be discarded from each of the tracks.

                Please note that an actual discarded part of the stream might will be at most of that length
                because it needs to be aligned with HLS segments distribution.
                The source will send an `Membrane.Event.Discontinuity` event with `:duration` field
                representing duration of the discarded part of the stream.
                """,
                inspector: &Membrane.Time.inspect/1
              ]

  @impl true
  def handle_init(_ctx, opts) do
    state =
      Map.from_struct(opts)
      |> Map.merge(%{
        # status will be either :initialized, :waiting_on_pads or :streaming
        status: :initialized,
        client_genserver: nil,
        stream: nil,
        new_tracks_notification: nil,
        pad_refs: %{video_output: nil, audio_output: nil},
        waiting_on_client_genserver_response?: false,
        initial_discontinuity_event_sent?: false
      })

    {[], state}
  end

  @impl true
  def handle_pad_added(Pad.ref(pad_name, _id) = pad_ref, ctx, state)
      when ctx.playback == :stopped do
    state = state |> put_in([:pad_refs, pad_name], pad_ref)
    {[], state}
  end

  @impl true
  def handle_pad_added(Pad.ref(pad_name, _id) = pad_ref, ctx, state)
      when ctx.playback == :playing and state.status == :waiting_on_pads do
    state = state |> put_in([:pad_refs, pad_name], pad_ref)

    if map_size(ctx.pads) == length(state.new_tracks_notification),
      do: state |> start_streaming(),
      else: {[], state}
  end

  @impl true
  def handle_pad_added(Pad.ref(pad_name, _id), ctx, state)
      when ctx.playback == :playing and state.status == :streaming do
    raise """
    Tried to link pad #{inspect(pad_name)}, but all pads are already linked.

    You must follow one of two scenarios:
    1. Link all pads before an element enters the `:playing` playback.
    2. Don't link any pads before an element enters the `:playing` playback,
        wait until it generates `{:new_tracks, tracks}` parent notification
        and then link pads accodrding to the notification.
    """
  end

  @impl true
  def handle_playing(ctx, state) do
    # both start_streaming/1 and generate_new_tracks_notification/1 functions
    # call ClientGenServer.get_tracks_info/1 that triggers downloading first
    # segments of the HLS stream

    state = create_client_gen_server(ctx, state)

    if Map.values(state.pad_refs) != [nil, nil] do
      state |> start_streaming()
    else
      state |> generate_new_tracks_notification()
    end
  end

  defp create_client_gen_server(ctx, state) do
    start_link_arg = %{
      url: state.url,
      variant_selection_policy: state.variant_selection_policy,
      source: self(),
      how_much_to_skip: state.how_much_to_skip
    }

    Membrane.UtilitySupervisor.start_link_child(
      ctx.utility_supervisor,
      {__MODULE__.ClientGenServer, start_link_arg}
    )

    client_genserver =
      receive do
        {:client_genserver, client_genserver} -> client_genserver
      after
        5_000 ->
          raise "Timeout waiting for #{inspect(__MODULE__)}.ClientGenServer initialization"
      end

    %{state | client_genserver: client_genserver}
  end

  defp generate_new_tracks_notification(%{status: :initialized} = state) do
    tracks_info = ClientGenServer.get_tracks_info(state.client_genserver)

    new_tracks =
      tracks_info
      |> Enum.map(fn {_id, stream_format} ->
        pad_name =
          if audio_stream_format?(stream_format),
            do: :audio_output,
            else: :video_output

        {pad_name, stream_format}
      end)

    state =
      %{
        state
        | status: :waiting_on_pads,
          new_tracks_notification: new_tracks
      }

    {[notify_parent: {:new_tracks, new_tracks}], state}
  end

  defp start_streaming(%{status: status} = state)
       when status in [:initialized, :waiting_on_pads] do
    actions = get_stream_formats(state) ++ get_redemands(state)

    state = %{
      state
      | status: :streaming
        # stream: Client.generate_stream(state.client_genserver)
    }

    {actions, state}
  end

  defp get_stream_formats(state) do
    stream_formats =
      ClientGenServer.get_tracks_info(state.client_genserver)
      |> Map.values()

    :ok = ensure_pads_match_stream_formats!(stream_formats, state)

    stream_formats
    |> Enum.flat_map(fn stream_format ->
      pad_ref =
        if audio_stream_format?(stream_format),
          do: state.pad_refs.audio_output,
          else: state.pad_refs.video_output

      [stream_format: {pad_ref, stream_format}]
    end)
  end

  defp get_redemands(state) do
    get_pads(state)
    |> Enum.flat_map(fn pad_ref -> [redemand: pad_ref] end)
  end

  defp ensure_pads_match_stream_formats!(stream_formats, state) do
    audio_format_occurs? =
      Enum.any?(stream_formats, &audio_stream_format?/1)

    video_format_occurs? =
      Enum.any?(stream_formats, &(not audio_stream_format?(&1)))

    if audio_format_occurs? and state.pad_refs.audio_output == nil do
      raise_missing_pad_error(:audio_output, stream_formats, state)
    end

    if video_format_occurs? and state.pad_refs.video_output == nil do
      raise_missing_pad_error(:video_output, stream_formats, state)
    end

    if not audio_format_occurs? and state.pad_refs.audio_output != nil do
      raise_redundant_pad_error(:audio_output, stream_formats, state)
    end

    if not video_format_occurs? and state.pad_refs.video_output != nil do
      raise_redundant_pad_error(:video_output, stream_formats, state)
    end

    :ok
  end

  @spec raise_missing_pad_error(atom(), list(), map()) :: no_return()
  defp raise_missing_pad_error(pad_name, stream_formats, state) do
    raise """
    Pad #{inspect(pad_name)} is not linked, but the HLS stream contains \
    #{pad_name_to_media_type(pad_name)} stream format.

    Pads: #{get_pads(state) |> inspect()}
    Stream formats of tracks in HLS playlist: #{inspect(stream_formats)}
    """
  end

  @spec raise_redundant_pad_error(atom(), list(), map()) :: no_return()
  defp raise_redundant_pad_error(pad_name, stream_formats, state) do
    raise """
    Pad #{inspect(pad_name)} is linked, but the HLS stream doesn't contain \
    #{pad_name_to_media_type(pad_name)} stream format.

    Pads: #{get_pads(state) |> inspect()}
    Stream formats of tracks in HLS playlist: #{inspect(stream_formats)}
    """
  end

  defp get_pads(state) do
    state.pad_refs |> Map.values() |> Enum.reject(&(&1 == nil))
  end

  defp audio_stream_format?(stream_format) do
    case stream_format do
      %RemoteStream{content_format: AAC} -> true
      %RemoteStream{content_format: H264} -> false
      %AAC{} -> true
      %H264{} -> false
    end
  end

  defp get_discontinuity_events(%{initial_discontinuity_event_sent?: false} = state) do
    skipped_segments_cumulative_duration =
      ClientGenServer.get_skipped_segments_cumulative_duration_ms(state.client_genserver)

    event = %Membrane.Event.Discontinuity{duration: skipped_segments_cumulative_duration}

    get_pads(state)
    |> Enum.flat_map(&[event: {&1, event}])
  end

  defp get_discontinuity_events(_state), do: []

  @impl true
  def handle_demand(_pad_ref, _demand, :buffers, _ctx, state)
      when state.status == :streaming and not state.waiting_on_client_genserver_response? do
    ClientGenServer.request_chunk_or_eos(state.client_genserver)
    {[], %{state | waiting_on_client_genserver_response?: true}}
  end

  @impl true
  def handle_demand(_pad_ref, _demand, :buffers, _ctx, state)
      when state.waiting_on_client_genserver_response? do
    {[], state}
  end

  @impl true
  def handle_demand(pad_ref, demand, :buffers, _ctx, state)
      when state.status == :waiting_on_pads do
    Membrane.Logger.debug("""
    Ignoring demand (#{inspect(demand)} buffers) on pad #{inspect(pad_ref)} because this \
    element is still waiting for other pads to be linked.
    """)

    {[], state}
  end

  @impl true
  def handle_info({:chunk, %ExHLS.Chunk{} = chunk}, _ctx, state) do
    buffer =
      %Buffer{
        payload: chunk.payload,
        pts: chunk.pts_ms |> Membrane.Time.milliseconds(),
        dts: chunk.dts_ms |> Membrane.Time.milliseconds(),
        metadata: chunk.metadata
      }

    buffer_pad_ref =
      case chunk.media_type do
        :audio -> state.pad_refs.audio_output
        :video -> state.pad_refs.video_output
      end

    actions =
      get_discontinuity_events(state) ++
        [buffer: {buffer_pad_ref, buffer}] ++ get_redemands(state)

    state = %{
      state
      | waiting_on_client_genserver_response?: false,
        initial_discontinuity_event_sent?: true
    }

    {actions, state}
  end

  @impl true
  def handle_info(:end_of_stream, _ctx, state) do
    actions = get_pads(state) |> Enum.flat_map(&[end_of_stream: &1])
    {actions, state}
  end

  defp pad_name_to_media_type(:audio_output), do: :audio
  defp pad_name_to_media_type(:video_output), do: :video
end
