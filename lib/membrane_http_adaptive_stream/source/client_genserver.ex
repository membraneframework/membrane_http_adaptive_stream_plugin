defmodule Membrane.HTTPAdaptiveStream.Source.ClientGenServer do
  # This GenServer is used by Membrane.HTTPAdaptiveStream.Source to allow downloading the
  # HLS segments asynchronously.
  @moduledoc false

  use GenServer
  alias ExHLS.Client

  @spec start_link(
          String.t(),
          Membrane.HTTPAdaptiveStream.Source.variant_selection_policy(),
          Membrane.Time.t()
        ) ::
          GenServer.on_start()
  def start_link(url, variant_selection_policy, how_much_to_skip) do
    GenServer.start_link(__MODULE__,
      url: url,
      variant_selection_policy: variant_selection_policy,
      how_much_to_skip: how_much_to_skip
    )
  end

  @spec request_audio_chunk(pid()) :: :ok
  def request_audio_chunk(client_genserver) do
    GenServer.cast(client_genserver, {:request_audio_chunk, self()})
  end

  @spec request_video_chunk(pid()) :: :ok
  def request_video_chunk(client_genserver) do
    GenServer.cast(client_genserver, {:request_video_chunk, self()})
  end

  # this function should be called by Membrane.HTTPAdaptiveStream.Source
  # before we start buffering the chunks, to avoid waiting
  # on downloading many segments
  @spec get_tracks_info(pid()) :: map()
  def get_tracks_info(client_genserver) do
    GenServer.call(client_genserver, :get_tracks_info)
  end

  @spec how_much_skipped(pid()) :: Membrane.Time.t()
  def how_much_skipped(client_genserver) do
    GenServer.call(client_genserver, :how_much_skipped)
  end

  @impl true
  def init(
        url: url,
        variant_selection_policy: variant_selection_policy,
        how_much_to_skip: how_much_to_skip
      ) do
    state = %{
      url: url,
      variant_selection_policy: variant_selection_policy,
      how_much_to_skip: how_much_to_skip,
      client: nil,
      how_much_skipped: nil
    }

    {:ok, state, {:continue, :setup}}
  end

  @impl true
  def handle_continue(:setup, state) do
    how_much_to_skip_ms = Membrane.Time.as_milliseconds(state.how_much_to_skip, :round)

    state =
      %{state | client: Client.new(state.url, how_much_to_skip_ms)}
      |> choose_variant()

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
  def handle_cast({:request_audio_chunk, pid}, state) do
    {chunk, client} = Client.read_audio_chunk(state.client)
    state = put_in(state.how_much_skipped, state.client.base_timestamp_ms |> round() |> Membrane.Time.milliseconds())
    send(pid, {:audio_chunk, chunk})
    {:noreply, %{state | client: client}}
  end

  @impl true
  def handle_cast({:request_video_chunk, pid}, state) do
    {chunk, client} = Client.read_video_chunk(state.client)
    state = put_in(state.how_much_skipped, state.client.base_timestamp_ms |> round() |> Membrane.Time.milliseconds())
    send(pid, {:video_chunk, chunk})
    {:noreply, %{state | client: client}}
  end

  @impl true
  def handle_call(:get_tracks_info, _from, state) do
    {:ok, tracks_info, client} = Client.get_tracks_info(state.client)
    {:reply, tracks_info, %{state | client: client}}
  end
  
  @impl true
  def handle_call(:how_much_skipped, _from, state) do
    {:reply, state.how_much_skipped, state}
  end
end
