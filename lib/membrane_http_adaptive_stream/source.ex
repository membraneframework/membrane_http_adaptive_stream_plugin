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

  # The boundary on how many chunks of one stream will be requested
  # from Membrane.HTTPAdaptiveStream.Source.ClientGenServer at once.
  @requested_chunks_boundary 5

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
              ]

  @impl true
  def handle_init(_ctx, opts) do
    initial_pad_state = %{
      ref: nil,
      requested: 0,
      qex: Qex.new(),
      qex_size: 0,
      oldest_buffer_dts: nil,
      eos_received?: false
    }

    state =
      Map.from_struct(opts)
      |> Map.merge(%{
        # status will be either :initialized, :waiting_on_pads or :running
        status: :initialized,
        client_genserver: nil,
        new_tracks_notification: nil,
        audio_output: initial_pad_state,
        video_output: initial_pad_state
      })

    {[], state}
  end

  @impl true
  def handle_setup(_ctx, state) do
    {:ok, client_genserver} =
      ClientGenServer.start_link(state.url, state.variant_selection_policy)

    {[], %{state | client_genserver: client_genserver}}
  end

  @impl true
  def handle_pad_added(Pad.ref(pad_name, _id) = pad_ref, ctx, state)
      when ctx.playback == :stopped do
    state = state |> put_in([pad_name, :ref], pad_ref)
    {[], state}
  end

  @impl true
  def handle_pad_added(Pad.ref(pad_name, _id) = pad_ref, ctx, state)
      when ctx.playback == :playing and state.status == :waiting_on_pads do
    state = state |> put_in([pad_name, :ref], pad_ref)

    if map_size(ctx.pads) == length(state.new_tracks_notification),
      do: state |> start_running(),
      else: {[], state}
  end

  @impl true
  def handle_pad_added(Pad.ref(pad_name, _id), ctx, state)
      when ctx.playback == :playing and state.status == :running do
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
  def handle_playing(_ctx, state) do
    # both start_running/1 and generate_new_tracks_notification/1 functions
    # call ClientGenServer.get_tracks_info/1 that triggers downloading first
    # segments of the HLS stream

    if state.audio_output.ref != nil or state.video_output.ref != nil do
      state |> start_running()
    else
      state |> generate_new_tracks_notification()
    end
  end

  defp generate_new_tracks_notification(%{status: :initialized} = state) do
    new_tracks =
      ClientGenServer.get_tracks_info(state.client_genserver)
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

  defp start_running(%{status: status} = state)
       when status in [:initialized, :waiting_on_pads] do
    actions = get_stream_formats(state) ++ get_redemands(state)

    state =
      %{state | status: :running}
      |> request_media_chunks()

    {actions, state}
  end

  defp get_stream_formats(state) do
    stream_formats =
      ClientGenServer.get_tracks_info(state.client_genserver)
      |> Map.values()

    :ok = ensure_pads_match_stream_formats!(stream_formats, state)

    stream_formats
    |> Enum.map(fn stream_format ->
      pad_ref =
        if audio_stream_format?(stream_format),
          do: state.audio_output.ref,
          else: state.video_output.ref

      {:stream_format, {pad_ref, stream_format}}
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

    if audio_format_occurs? and state.audio_output.ref == nil do
      raise_missing_pad_error(:audio_output, stream_formats, state)
    end

    if video_format_occurs? and state.video_output.ref == nil do
      raise_missing_pad_error(:video_output, stream_formats, state)
    end

    if not audio_format_occurs? and state.audio_output.ref != nil do
      raise_redundant_pad_error(:audio_output, stream_formats, state)
    end

    if not video_format_occurs? and state.video_output.ref != nil do
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
    [:audio_output, :video_output]
    |> Enum.flat_map(fn pad_name ->
      case state[pad_name].ref do
        nil -> []
        pad_ref -> [pad_ref]
      end
    end)
  end

  defp audio_stream_format?(stream_format) do
    case stream_format do
      %RemoteStream{content_format: AAC} -> true
      %RemoteStream{content_format: H264} -> false
      %AAC{} -> true
      %H264{} -> false
    end
  end

  @impl true
  def handle_demand(pad_ref, demand, :buffers, _ctx, state)
      when state.status == :running do
    {actions, state} = pop_buffers(pad_ref, demand, state)
    state = request_media_chunks(state)
    {actions, state}
  end

  @impl true
  def handle_demand(pad_ref, demand, :buffers, _ctx, state)
      when state.status == :waiting_on_pads do
    Membrane.Logger.debug("""
    Ignoring demand (#{inspect(demand)} buffers) on pad #{inspect(pad_ref)} because thes\
    element is still waiting for other pads to be linked.
    """)

    {[], state}
  end

  @impl true
  def handle_info({chunk_type, %ExHLS.Chunk{} = chunk}, _ctx, state) do
    pad_name = chunk_type_to_pad_name(chunk_type)

    buffer = %Buffer{
      payload: chunk.payload,
      pts: chunk.pts_ms |> Membrane.Time.milliseconds(),
      dts: chunk.dts_ms |> Membrane.Time.milliseconds(),
      metadata: chunk.metadata
    }

    state =
      state
      |> update_in([pad_name, :qex], &Qex.push(&1, buffer))
      |> update_in([pad_name, :qex_size], &(&1 + 1))
      |> update_in([pad_name, :requested], &(&1 - 1))
      |> update_in([pad_name, :oldest_buffer_dts], fn
        nil -> buffer.dts
        oldest_dts -> oldest_dts
      end)
      |> request_media_chunks()

    {[redemand: state[pad_name].ref], state}
  end

  @impl true
  def handle_info({chunk_type, :end_of_stream}, _ctx, state) do
    pad_name = chunk_type_to_pad_name(chunk_type)

    state =
      if state[pad_name].eos_received? do
        state
      else
        state
        |> put_in([pad_name, :eos_received?], true)
        |> update_in([pad_name, :qex], &Qex.push(&1, :end_of_stream))
        |> update_in([pad_name, :qex_size], &(&1 + 1))
      end

    state = state |> update_in([pad_name, :requested], &(&1 - 1))

    {[redemand: state[pad_name].ref], state}
  end

  defp chunk_type_to_pad_name(:audio_chunk), do: :audio_output
  defp chunk_type_to_pad_name(:video_chunk), do: :video_output

  defp pad_name_to_media_type(:audio_output), do: :audio
  defp pad_name_to_media_type(:video_output), do: :video

  defp pop_buffers(Pad.ref(pad_name, _id) = pad_ref, demand, state) do
    how_many_pop = min(state[pad_name].qex_size, demand)

    1..how_many_pop//1
    |> Enum.flat_map_reduce(state, fn _i, state ->
      {buffer_or_eos, qex} = state[pad_name].qex |> Qex.pop!()

      state =
        state
        |> put_in([pad_name, :qex], qex)
        |> update_in([pad_name, :qex_size], &(&1 - 1))

      case buffer_or_eos do
        %Buffer{} = buffer ->
          state = state |> put_in([pad_name, :oldest_buffer_dts], buffer.dts)
          {[buffer: {pad_ref, buffer}], state}

        :end_of_stream ->
          {[end_of_stream: pad_ref], state}
      end
    end)
  end

  defp request_media_chunks(state) do
    [:audio_output, :video_output]
    |> Enum.reduce(state, fn pad_name, state ->
      %{eos_received?: eos_received?, oldest_buffer_dts: oldest_dts, ref: pad_ref} =
        state[pad_name]

      request_size =
        case state[pad_name].qex |> Qex.first() do
          _any when pad_ref == nil or eos_received? ->
            0

          # todo: maybe we should handle rollovers
          {:value, %Buffer{dts: newest_dts}}
          when newest_dts - oldest_dts >= state.buffered_stream_time ->
            0

          _empty_or_not_new_enough ->
            @requested_chunks_boundary - state[pad_name].requested
        end

      1..request_size//1
      |> Enum.each(fn _i -> request_single_chunk(pad_name, state) end)

      state
      |> update_in([pad_name, :requested], &(&1 + request_size))
    end)
  end

  defp request_single_chunk(:audio_output, state),
    do: ClientGenServer.request_audio_chunk(state.client_genserver)

  defp request_single_chunk(:video_output, state),
    do: ClientGenServer.request_video_chunk(state.client_genserver)
end
