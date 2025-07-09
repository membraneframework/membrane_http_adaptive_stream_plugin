defmodule Membrane.HLS.Source do
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
    demand_unit: :buffers

  def_output_pad :audio_output,
    accepted_format: any_of(AAC, %RemoteStream{content_format: AAC}),
    flow_control: :manual,
    demand_unit: :buffers

  @requested_samples_boundary 5

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

  def_options url: [
                spec: String.t(),
                description: "URL of the HLS playlist manifest"
              ],
              buffered_stream_time: [
                spec: Membrane.Time.t(),
                default: Membrane.Time.seconds(1),
                inspector: &Membrane.Time.inspect/1,
                description: """
                Amount of time of stream, that will be buffered by #{inspect(__MODULE__)}.

                Defaults to 1 second.
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
    initial_pad_state = %{requested: 0, qex: Qex.new(), oldest_buffer_dts: nil}

    state =
      Map.from_struct(opts)
      |> Map.merge(%{
        audio_output: initial_pad_state,
        video_output: initial_pad_state,
        client_genserver: nil
      })

    {[], state}
  end

  @impl true
  def handle_setup(_ctx, state) do
    {:ok, client_genserver} =
      ClientGenServer.start_link(state.url, state.variant_selection_policy)

    # todo: maybe we should call here `get_tracks_info/1` to start downloading segments
    # or we should start buffering the frames?

    {[], %{state | client_genserver: client_genserver}}
  end

  @impl true
  def handle_playing(_ctx, state) do
    {[audio_stream_format], [video_stream_format]} =
      ClientGenServer.get_tracks_info(state.client_genserver)
      |> Map.values()
      |> Enum.split_with(&is_audio_stream_format/1)

    actions = [
      stream_format: {:audio_output, audio_stream_format},
      stream_format: {:video_output, video_stream_format}
    ]

    {actions, state}
  end

  defp is_audio_stream_format(stream_format) do
    case stream_format do
      %RemoteStream{content_format: AAC} -> true
      %RemoteStream{content_format: H264} -> false
      %AAC{} -> true
      %H264{} -> false
    end
  end

  @impl true
  def handle_demand(pad_ref, demand, :buffers, _ctx, state) do
    {buffers, state} = pop_buffers(pad_ref, demand, state)
    state = request_samples(state)
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

        buffer = %Buffer{
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
      oldest_dts = state[pad_ref].oldest_buffer_dts

      request_size =
        case state[pad_ref].qex |> Qex.first() do
          # todo: maybe we should handle rollovers
          {:value, %Buffer{dts: newest_dts}}
          when newest_dts - oldest_dts >= state.buffered_stream_time ->
            0

          _empty_or_not_new_enough ->
            @requested_samples_boundary - state[pad_ref].qex_size - state[pad_ref].requested
        end

      1..request_size//1
      |> Enum.each(fn _i -> request_single_sample(pad_ref, state) end)

      state
      |> update_in([pad_ref, :requested], &(&1 + request_size))
    end)
  end

  defp request_single_sample(:audio_output, state),
    do: ClientGenServer.request_audio_sample(state.client_genserver)

  defp request_single_sample(:video_output, state),
    do: ClientGenServer.request_video_sample(state.client_genserver)
end
