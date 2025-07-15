defmodule Membrane.HLS.Source.ClientGenServer do
  # This GenServer is used by Membrane.HLS.Source to allow downloading the
  # HLS segments asynchronously.
  @moduledoc false

  use GenServer
  alias ExHLS.Client

  @spec start_link(
          String.t(),
          Membrane.HLS.Source.variant_selection_policy()
        ) ::
          GenServer.on_start()
  def start_link(url, variant_selection_policy) do
    GenServer.start_link(__MODULE__,
      url: url,
      variant_selection_policy: variant_selection_policy
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

  # this function should be called by Membrane.HLS.Source
  # before we start buffering the chunks, to avoid waiting
  # on downloading many segments
  @spec get_tracks_info(pid()) :: map()
  def get_tracks_info(client_genserver) do
    GenServer.call(client_genserver, :get_tracks_info)
  end

  @impl true
  def init(url: url, variant_selection_policy: variant_selection_policy) do
    state = %{
      url: url,
      variant_selection_policy: variant_selection_policy,
      client: nil
    }

    # let's create Client and choose variant asnychronously
    # beyond init/1, because it requires doing some HTTP requests
    # so it can take some time
    self() |> send(:setup)

    {:ok, state}
  end

  @impl true
  def handle_info(:setup, state) do
    state =
      %{state | client: Client.new(state.url)}
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
    send(pid, {:audio_chunk, chunk})
    {:noreply, %{state | client: client}}
  end

  @impl true
  def handle_cast({:request_video_chunk, pid}, state) do
    {chunk, client} = Client.read_video_chunk(state.client)
    send(pid, {:video_chunk, chunk})
    {:noreply, %{state | client: client}}
  end

  @impl true
  def handle_call(:get_tracks_info, _from, state) do
    {:ok, tracks_info, client} = Client.get_tracks_info(state.client)
    {:reply, tracks_info, %{state | client: client}}
  end
end
