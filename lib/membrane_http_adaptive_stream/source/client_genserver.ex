defmodule Membrane.HTTPAdaptiveStream.Source.ClientGenServer do
  # This GenServer is used by Membrane.HTTPAdaptiveStream.Source to allow downloading the
  # HLS segments asynchronously.
  @moduledoc false

  use GenServer
  alias ExHLS.Client

  @spec start_link(%{
          url: String.t(),
          variant_selection_policy: Membrane.HTTPAdaptiveStream.Source.variant_selection_policy(),
          source: pid(),
          how_much_to_skip: Membrane.Time.t()
        }) ::
          GenServer.on_start()
  def start_link(%{
        url: url,
        variant_selection_policy: variant_selection_policy,
        source: source,
        how_much_to_skip: how_much_to_skip
      }) do
    GenServer.start_link(__MODULE__,
      url: url,
      variant_selection_policy: variant_selection_policy,
      source: source,
      how_much_to_skip: how_much_to_skip
    )
  end

  @spec request_chunk_or_eos(pid()) :: :ok
  def request_chunk_or_eos(client_genserver) do
    GenServer.cast(client_genserver, {:request_chunk_or_eos, self()})
  end

  @spec get_skipped_segments_cumulative_duration_ms(pid()) ::
          {:ok, Membrane.Time.t()} | {:error, reason :: any()}
  def get_skipped_segments_cumulative_duration_ms(client_genserver) do
    GenServer.call(client_genserver, :get_skipped_segments_cumulative_duration_ms)
  end

  # this function should be called by Membrane.HTTPAdaptiveStream.Source
  # before we start buffering the chunks, to avoid waiting
  # on downloading many segments
  @spec get_tracks_info(pid()) :: map()
  def get_tracks_info(client_genserver) do
    GenServer.call(client_genserver, :get_tracks_info)
  end

  @impl true
  def init(
        url: url,
        variant_selection_policy: variant_selection_policy,
        source: source,
        how_much_to_skip: how_much_to_skip
      ) do
    state = %{
      url: url,
      variant_selection_policy: variant_selection_policy,
      source: source,
      how_much_to_skip_ms: Membrane.Time.as_milliseconds(how_much_to_skip, :round),
      client: nil,
      stream: nil,
      tracks_info: nil
    }

    {:ok, state, {:continue, :setup}}
  end

  @impl true
  def handle_continue(:setup, state) do
    client = Client.new(state.url, how_much_to_skip_ms: state.how_much_to_skip_ms)

    state =
      %{state | client: client}
      |> choose_variant()

    {:ok, tracks_info, client} = Client.get_tracks_info(state.client)
    stream = Client.generate_stream(client)

    state = %{
      state
      | client: client,
        stream: stream,
        tracks_info: tracks_info
    }

    send(state.source, {:client_genserver, self()})

    {:noreply, state}
  end

  defp choose_variant(state) do
    variants = Client.get_variants(state.client)

    if variants != %{} do
      get_resolution_fn = fn {_id, %{resolution: {width, height}}} -> width * height end
      get_bandwidth_fn = fn {_id, %{bandwidth: bandwidth}} -> bandwidth end

      chosen_variant_id =
        case state.variant_selection_policy do
          :lowest_resolution ->
            variants |> Enum.min_by(get_resolution_fn) |> elem(0)

          :highest_resolution ->
            variants |> Enum.max_by(get_resolution_fn) |> elem(0)

          :lowest_bandwidth ->
            variants |> Enum.min_by(get_bandwidth_fn) |> elem(0)

          :highest_bandwidth ->
            variants |> Enum.max_by(get_bandwidth_fn) |> elem(0)

          custom_policy when is_function(custom_policy, 1) ->
            variants |> custom_policy.()
        end

      client = state.client |> Client.choose_variant(chosen_variant_id)
      %{state | client: client}
    else
      state
    end
  end

  @impl true
  def handle_cast({:request_chunk_or_eos, pid}, state) do
    # this is ugly but this is how StreamSplit works
    {chunk_or_eos, stream} =
      try do
        state.stream |> StreamSplit.pop()
      rescue
        _e in MatchError -> {:end_of_stream, state.stream}
      end

    case chunk_or_eos do
      %ExHLS.Chunk{} = chunk -> send(pid, {:chunk, chunk})
      :end_of_stream -> send(pid, :end_of_stream)
    end

    {:noreply, %{state | stream: stream}}
  end

  @impl true
  def handle_call(:get_skipped_segments_cumulative_duration_ms, _from, state) do
    {:ok, skipped_duration_ms} =
      Client.get_skipped_segments_cumulative_duration_ms(state.client)

    reply = Membrane.Time.milliseconds(skipped_duration_ms)
    {:reply, reply, state}
  end

  @impl true
  def handle_call(:get_tracks_info, _from, state) do
    {:reply, state.tracks_info, state}
  end
end
