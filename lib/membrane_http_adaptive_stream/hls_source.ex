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

  # The boundary on how many chunks of one stream will be requested
  # from Membrane.HLS.Source.ClientGenServer at once.
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
      requested: 0,
      qex: Qex.new(),
      qex_size: 0,
      oldest_buffer_dts: nil,
      eos_received?: false
    }

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
    # or we should start buffering frames?

    {[], %{state | client_genserver: client_genserver}}
  end

  @impl true
  def handle_playing(_ctx, state) do
    {[audio_stream_format], [video_stream_format]} =
      ClientGenServer.get_tracks_info(state.client_genserver)
      |> Map.values()
      |> Enum.split_with(&audio_stream_format?/1)

    actions = [
      stream_format: {:audio_output, audio_stream_format},
      stream_format: {:video_output, video_stream_format}
    ]

    state = request_media_chunks(state)
    {actions, state}
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
  def handle_demand(pad_ref, demand, :buffers, _ctx, state) do
    {actions, state} = pop_buffers(pad_ref, demand, state)
    state = request_media_chunks(state)
    {actions, state}
  end

  @impl true
  def handle_info({data_type, %ExHLS.Chunk{} = chunk}, _ctx, state) do
    pad_ref = data_type_to_pad_ref(data_type)

    buffer = %Buffer{
      payload: chunk.payload,
      pts: chunk.pts_ms |> Membrane.Time.milliseconds(),
      dts: chunk.dts_ms |> Membrane.Time.milliseconds(),
      metadata: chunk.metadata
    }

    state =
      state
      |> update_in([pad_ref, :qex], &Qex.push(&1, buffer))
      |> update_in([pad_ref, :qex_size], &(&1 + 1))
      |> update_in([pad_ref, :requested], &(&1 - 1))
      |> update_in([pad_ref, :oldest_buffer_dts], fn
        nil -> buffer.dts
        oldest_dts -> oldest_dts
      end)
      |> request_media_chunks()

    {[redemand: pad_ref], state}
  end

  @impl true
  def handle_info({data_type, :end_of_stream}, _ctx, state) do
    pad_ref = data_type_to_pad_ref(data_type)

    state =
      if state[pad_ref].eos_received? do
        state
      else
        state
        |> put_in([pad_ref, :eos_received?], true)
        |> update_in([pad_ref, :qex], &Qex.push(&1, :end_of_stream))
        |> update_in([pad_ref, :qex_size], &(&1 + 1))
      end

    state = state |> update_in([pad_ref, :requested], &(&1 - 1))

    {[redemand: pad_ref], state}
  end

  defp data_type_to_pad_ref(:audio_chunk), do: :audio_output
  defp data_type_to_pad_ref(:video_chunk), do: :video_output

  defp pop_buffers(pad_ref, demand, state) do
    how_many_pop = min(state[pad_ref].qex_size, demand)

    1..how_many_pop//1
    |> Enum.flat_map_reduce(state, fn _i, state ->
      {buffer_or_eos, qex} = state[pad_ref].qex |> Qex.pop!()

      state =
        state
        |> put_in([pad_ref, :qex], qex)
        |> update_in([pad_ref, :qex_size], &(&1 - 1))

      case buffer_or_eos do
        %Buffer{} = buffer ->
          state = state |> put_in([pad_ref, :oldest_buffer_dts], buffer.dts)
          {[buffer: {pad_ref, buffer}], state}

        :end_of_stream ->
          {[end_of_stream: pad_ref], state}
      end
    end)
  end

  defp request_media_chunks(state) do
    [:audio_output, :video_output]
    |> Enum.reduce(state, fn pad_ref, state ->
      oldest_dts = state[pad_ref].oldest_buffer_dts
      eos_received? = state[pad_ref].eos_received?

      request_size =
        case state[pad_ref].qex |> Qex.first() do
          _any when eos_received? ->
            0

          # todo: maybe we should handle rollovers
          {:value, %Buffer{dts: newest_dts}}
          when newest_dts - oldest_dts >= state.buffered_stream_time ->
            0

          _empty_or_not_new_enough ->
            @requested_chunks_boundary - state[pad_ref].requested
        end

      1..request_size//1
      |> Enum.each(fn _i -> request_single_chunk(pad_ref, state) end)

      state
      |> update_in([pad_ref, :requested], &(&1 + request_size))
    end)
  end

  defp request_single_chunk(:audio_output, state),
    do: ClientGenServer.request_audio_chunk(state.client_genserver)

  defp request_single_chunk(:video_output, state),
    do: ClientGenServer.request_video_chunk(state.client_genserver)
end
