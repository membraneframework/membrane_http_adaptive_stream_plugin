defmodule Membrane.HLS.Source do
  defmodule ClientGenServer do
    use GenServer
    alias ExHLS.Client

    @impl true
    def init(url: url, impl: impl) do
      client = Client.new(url, impl)
      variant_id = Client.get_variants(client) |> Enum.at(0) |> elem(0)
      client = Client.choose_variant(client, variant_id)
      {:ok, %{client: client}}
    end

    @impl true
    def handle_cast({:get_audio, pid}, state) do
      {frame, client} = Client.read_audio_frame(state.client)
      state = put_in(state.client, client)
      send(pid, {:audio_frame, frame})
      {:noreply, state}
    end

    @impl true
    def handle_cast({:get_video, pid}, state) do
      {frame, client} = Client.read_video_frame(state.client)
      state = put_in(state.client, client)
      send(pid, {:video_frame, frame})
      {:noreply, state}
    end

    @impl true
    def handle_call(:get_tracks_info, _caller, state) do
      {:ok, result, client} = Client.get_tracks_info(state.client)
      state = put_in(state.client, client)
      {:reply, result, state}
    end
  end

  use Membrane.Source
  require Membrane.Pad, as: Pad

  alias ExHLS.Client

  alias Membrane.{
    AAC,
    H264,
    RemoteStream
  }

  def_output_pad :video_output,
    accepted_format: any_of(H264, %RemoteStream{content_type: H264}),
    availability: :on_request,
    max_instances: 1,
    flow_control: :manual,
    demand_unit: :buffers

  def_output_pad :audio_output,
    accepted_format: any_of(AAC, %RemoteStream{content_type: AAC}),
    availability: :on_request,
    max_instances: 1,
    flow_control: :manual,
    demand_unit: :buffers

  def_options url: [spec: String.t()],
              buffer_size: [spec: pos_integer(), default: 0],
              container: [spec: :mpegts | :fmp4, default: :mpegts]

  @impl true
  def handle_init(_ctx, opts) do
    state =
      Map.from_struct(opts)
      |> Map.merge(%{
        audio_output: %{pad_ref: nil, pending: 0, qex: Qex.new()},
        video_output: %{pad_ref: nil, pending: 0, qex: Qex.new()},
        client_genserver: nil
      })

    {[], state}
  end

  @impl true
  def handle_setup(_ctx, state) do
    demuxing_engine =
      case state.container do
        :mpegts -> ExHLS.DemuxingEngine.MPEGTS
        :fmp4 -> ExHLS.DemuxingEngine.FMP4
      end

    client = Client.new(state.url, demuxing_engine)

    {:ok, clinet_genserver} =
      GenServer.start_link(ClientGenServer, url: state.url, impl: demuxing_engine)

    %{state | client_genserver: clinet_genserver}
  end

  @impl true
  def handle_pad_added(Pad.ref(pad_name, _id) = pad_ref, _ctx, state) do
    state = state |> put_in([pad_name, :pad_ref], pad_ref)
    {[], state}
  end

  @impl true
  def handle_playing(_ctx, state) do
    {:ok, tracks_info} = GenServer.call(client_server, :get_tracks_info)

    {[audio_stream_format], [video_stream_format]} =
      tracks_info
      |> Map.values()
      |> Enum.split_with(fn
        %RemoteStream{content_type: AAC} -> true
        %AAC{} -> true
        %RemoteStream{content_type: H264} -> false
        %H264{} -> false
      end)

    {[
       stream_format: {:audio_output, audio_stream_format},
       stream_format: {:video_output, video_stream_format}
     ], %{state | client_server: client_server}}
  end

  @impl true
  def handle_demand(pad, _demand, _demand_unit, _ctx, state) do
    {actions, state} =
      if state[pad].buffer != [] do
        {first, rest} = List.pop_at(state[pad].buffer, 0)
        state = put_in(state[pad].buffer, rest)

        buffer = %Membrane.Buffer{
          payload: first.payload,
          pts: first.pts |> Membrane.Time.milliseconds(),
          dts: first.dts |> Membrane.Time.milliseconds(),
          metadata: first.metadata
        }

        {[buffer: {pad, buffer}], state}
      else
        {[], state}
      end

    state = request_frames(state)

    {actions, state}
  end

  @impl true
  def handle_info({frame_type, frame}, _ctx, state) do
    pad =
      case frame_type do
        :audio_frame -> :audio_output
        :video_frame -> :video_output
      end

    state =
      update_in(state[pad].buffer, &(&1 ++ [frame])) |> update_in([pad, :pending], &(&1 - 1))

    {[redemand: :audio_output, redemand: :video_output], state}
  end

  @impl true
  def handle_info(_msg, _ctx, state) do
    {[], state}
  end

  defp request_frames(state) do
    Enum.reduce([:audio_output, :video_output], state, fn pad, state ->
      queue = state[pad]
      to_request = @how_many_frames_buffered - length(queue.buffer) - queue.pending

      request_type =
        case pad do
          :audio_output -> :get_audio
          :video_output -> :get_video
        end

      Enum.each(1..to_request, fn _i ->
        GenServer.cast(state.client_server, {request_type, self()})
      end)

      update_in(state[pad].pending, &(&1 + to_request))
    end)
  end
end
