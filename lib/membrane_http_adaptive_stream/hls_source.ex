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
    flow_control: :manual,
    demand_unit: :buffers

  def_output_pad :audio_output,
    accepted_format: any_of(AAC, %RemoteStream{content_format: AAC}),
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
              variant_selection_policy: [
                spec: variant_selection_policy(),
                default: :highest_resolution
              ]

  @impl true
  def handle_init(_ctx, opts) do
    state =
      Map.from_struct(opts)
      |> Map.merge(%{
        audio_output: %{requested: 0, qex: Qex.new(), qex_size: 0},
        video_output: %{requested: 0, qex: Qex.new(), qex_size: 0},
        client_genserver: nil
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
  def handle_playing(ctx, state) do
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
      stream_format: {:audio_output, audio_stream_format},
      stream_format: {:video_output, video_stream_format}
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
  def handle_info({data_type, sample}, _ctx, state)
      when data_type in [:audio_sample_, :video_sample] do
    pad_ref =
      case data_type do
        :audio_sample -> :audio_output
        :video_sample -> :video_output
      end

    state =
      state
      |> update_in([pad_ref, :qex], &Qex.push(&1, sample))
      |> update_in([pad_ref, :qex_size], &(&1 + 1))
      |> update_in([pad_ref, :requested], &(&1 - 1))

    {[redemand: pad_ref], state}
  end

  defp pop_buffers(pad_ref, demand, state) do
    range_upperbound = min(state[pad_ref].qex_size, demand)

    if range_upperbound > 0 do
      1..range_upperbound
      |> Enum.map_reduce(state, fn _i, state ->
        {%ExHLS.Sample{} = sample, qex} = state[pad_ref].qex |> Qex.pop!()

        buffer = %Membrane.Buffer{
          payload: sample.payload,
          pts: sample.pts_ms |> Membrane.Time.milliseconds(),
          dts: sample.dts_ms |> Membrane.Time.milliseconds(),
          metadata: sample.metadata
        }

        state =
          state
          |> put_in([pad_ref, :qex], qex)
          |> update_in([pad_ref, :qex_size], &(&1 - 1))

        {buffer, state}
      end)
    else
      {[], state}
    end
  end

  defp request_samples(state) do
    [:audio_output, :video_output]
    |> Enum.reduce(state, fn pad_ref, state ->
      request_size = state.buffer_size - state[pad_ref].qex_size - state[pad_ref].requested

      if request_size > 0 do
        1..request_size
        |> Enum.each(fn _i -> reuqest_single_sample(pad_ref, state) end)
      end

      state
      |> update_in([pad_ref, :requested], &(&1 + request_size))
    end)
  end

  defp request_single_sample(:audio_output, state),
    do: ClientGenServer.request_audio_sample(state.client_genserver)

  defp request_single_sample(:video_output, state),
    do: ClientGenServer.request_video_sample(state.client_genserver)
end
