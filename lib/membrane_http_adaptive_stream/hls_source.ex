defmodule Membrane.HLS.Source do
  use Membrane.Source
  require Membrane.Pad, as: Pad

  alias __MODULE__.ClientGenServer

  alias Membrane.{
    AAC,
    H264,
    RemoteStream
  }

  def_output_pad :video_output,
    accepted_format: any_of(H264, %RemoteStream{content_format: H264}),
    availability: :on_request,
    max_instances: 1,
    flow_control: :manual,
    demand_unit: :buffers

  def_output_pad :audio_output,
    accepted_format: any_of(AAC, %RemoteStream{content_format: AAC}),
    availability: :on_request,
    max_instances: 1,
    flow_control: :manual,
    demand_unit: :buffers

  @type variant_selection_policy() ::
          :lowest_resolution
          | :highest_resolution
          | :lowest_bandwidth
          | :highest_bandwidth
          | (variants_map :: %{optional(integer()) => ExHLS.Client.variant_description()} ->
               variant_id :: integer())

  def_options url: [spec: String.t()],
              buffer_size: [spec: pos_integer(), default: 0],
              container: [spec: :mpeg_ts | :fmp4, default: :mpeg_ts],
              variant_selection_policy: [
                spec: variant_selection_policy(),
                default: :highest_resolution
              ]

  @impl true
  def handle_init(_ctx, opts) do
    state =
      Map.from_struct(opts)
      |> Map.merge(%{
        audio_output: %{pad_ref: nil, requested: 0, qex: Qex.new(), qex_size: 0},
        video_output: %{pad_ref: nil, requested: 0, qex: Qex.new(), qex_size: 0},
        client_genserver: nil
      })

    {[], state}
  end

  @impl true
  def handle_setup(_ctx, state) do
    demuxing_engine =
      case state.container do
        :mpeg_ts -> ExHLS.DemuxingEngine.MPEGTS
        :fmp4 -> ExHLS.DemuxingEngine.CMAF
      end

    {:ok, clinet_genserver} =
      ClientGenServer.start_link(state.url, demuxing_engine, state.variant_selection_policy)

    {[], %{state | client_genserver: clinet_genserver}}
  end

  @impl true
  def handle_pad_added(Pad.ref(pad_name, _id) = pad_ref, _ctx, state) do
    state = state |> put_in([pad_name, :pad_ref], pad_ref)
    {[], state}
  end

  @impl true
  def handle_playing(ctx, state) do
    if ctx.pads |> map_size() < 2 do
      raise "HLS Source requires both audio and video output pads to be present"
    end

    {[audio_stream_format], [video_stream_format]} =
      ClientGenServer.get_tracks_info(state.client_genserver)
      |> Map.values()
      |> Enum.split_with(fn
        %RemoteStream{content_format: AAC} -> true
        %RemoteStream{content_format: H264} -> false
        %AAC{} -> true
        %H264{} -> false
      end)

    actions = [
      stream_format: {state.audio_output.pad_ref, audio_stream_format},
      stream_format: {state.video_output.pad_ref, video_stream_format}
    ]

    {actions, state}
  end

  @impl true
  def handle_demand(pad_ref, demand, :buffers, _ctx, state) do
    {buffers, state} = pop_buffers(pad_ref, demand, state)
    state = request_frames(state)
    {[buffer: {pad_ref, buffers}], state}
  end

  @impl true
  def handle_info({stream_type, frame}, _ctx, state)
      when stream_type in [:audio_stream, :video_stream] do
    pad_name =
      case stream_type do
        :audio_stream -> :audio_output
        :video_stream -> :video_output
      end

    state =
      state
      |> update_in([pad_name, :qex], &Qex.push(&1, frame))
      |> update_in([pad_name, :qex_size], &(&1 + 1))
      |> update_in([pad_name, :requested], &(&1 - 1))

    {[redemand: state[pad_name].pad_ref], state}
  end

  defp pop_buffers(Pad.ref(pad_name, _id), demand, state) do
    range_upperbound = min(state[pad_name].qex_size, demand)

    if range_upperbound > 0 do
      1..range_upperbound
      |> Enum.map_reduce(state, fn _i, state ->
        {frame, qex} = state[pad_name].qex |> Qex.pop!()

        buffer = %Membrane.Buffer{
          payload: frame.payload,
          pts: frame.pts |> Membrane.Time.milliseconds(),
          dts: frame.dts |> Membrane.Time.milliseconds(),
          metadata: frame.metadata
        }

        state =
          state
          |> put_in([pad_name, :qex], qex)
          |> update_in([pad_name, :qex_size], &(&1 - 1))

        {buffer, state}
      end)
    else
      {[], state}
    end
  end

  defp request_frames(state) do
    [:audio_output, :video_output]
    |> Enum.reduce(state, fn pad_name, state ->
      request_size = state.buffer_size - state[pad_name].qex_size - state[pad_name].requested
      :ok = do_request(state, pad_name, request_size)

      state
      |> update_in([pad_name, :requested], &(&1 + request_size))
    end)
  end

  defp do_request(_state, _pad_name, request_size) when request_size < 1, do: :ok

  defp do_request(state, :audio_output, request_size) do
    1..request_size
    |> Enum.each(fn _i ->
      ClientGenServer.request_audio(state.client_genserver)
    end)
  end

  defp do_request(state, :video_output, request_size) do
    1..request_size
    |> Enum.each(fn _i ->
      ClientGenServer.request_video(state.client_genserver)
    end)
  end
end
