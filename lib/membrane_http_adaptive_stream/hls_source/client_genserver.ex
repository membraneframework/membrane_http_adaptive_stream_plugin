defmodule Membrane.HLS.Source.ClientGenServer do
  # This GenServer is used by Membrane.HLS.Source to allow downloading the
  # HLS segments asynchronously.
  @moduledoc false

  use GenServer
  alias ExHLS.Client

  @spec start_link(
          String.t(),
          ExHLS.DemuxingEngine.CMAF | ExHLS.DemuxingEngine.MPEGTS,
          Membrane.HLS.Source.variant_selection_policy()
        ) ::
          GenServer.on_start()
  def start_link(url, demuxing_engine, variant_selection_policy) do
    GenServer.start_link(__MODULE__,
      url: url,
      demuxing_engine: demuxing_engine,
      variant_selection_policy: variant_selection_policy
    )
  end

  @spec request_audio(pid()) :: :ok
  def request_audio(client_genserver) do
    GenServer.cast(client_genserver, {:request_audio, self()})
  end

  @spec request_video(pid()) :: :ok
  def request_video(client_genserver) do
    GenServer.cast(client_genserver, {:request_video, self()})
  end

  @spec get_tracks_info(pid()) :: map()
  def get_tracks_info(client_genserver) do
    GenServer.call(client_genserver, :get_tracks_info)
  end

  @impl true
  def init(
        url: url,
        demuxing_engine: demuxing_engine,
        variant_selection_policy: variant_selection_policy
      ) do
    state = %{
      client: Client.new(url, demuxing_engine),
      variant_selection_policy: variant_selection_policy
    }

    {:ok, state |> choose_variant()}
  end

  defp choose_variant(state) do
    variants = Client.get_variants(state.client)
    get_resolution = fn {_id, %{resolution: {width, height}}} -> width * height end
    get_bandwidth = fn {_id, %{bandwidth: bandwidth}} -> bandwidth end

    chosen_variant_id =
      case state.variant_selection_policy do
        :lowest_resolution ->
          variants |> Enum.min_by(get_resolution) |> elem(0)

        :highest_resolution ->
          variants |> Enum.max_by(get_resolution) |> elem(0)

        :lowest_bandwidth ->
          variants |> Enum.min_by(get_bandwidth) |> elem(0)

        :highest_bandwidth ->
          variants |> Enum.max_by(get_bandwidth) |> elem(0)

        custom_policy when is_function(custom_policy, 1) ->
          variants |> custom_policy.()
      end

    client = state.client |> Client.choose_variant(chosen_variant_id)
    %{state | client: client}
  end

  @impl true
  def handle_cast({:request_audio, pid}, state) do
    {frame, client} = Client.read_audio_frame(state.client)
    send(pid, {:audio_stream, frame})
    {:noreply, %{state | client: client}}
  end

  @impl true
  def handle_cast({:request_video, pid}, state) do
    {frame, client} = Client.read_video_frame(state.client)
    send(pid, {:video_stream, frame})
    {:noreply, %{state | client: client}}
  end

  @impl true
  def handle_call(:get_tracks_info, _from, state) do
    {:ok, tracks_info, client} = Client.get_tracks_info(state.client)
    {:reply, tracks_info, %{state | client: client}}
  end
end
