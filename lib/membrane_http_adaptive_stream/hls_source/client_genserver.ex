defmodule Membrane.HLS.Source.ClientGenServer do
  # This GenServer is used by Membrane.HLS.Source to allow downloading the
  # HLS segments asynchronously.
  @moduledoc false

  use GenServer
  alias ExHLS.Client

  @spec start_link(String.t(), ExHLS.DemuxingEngine.CMAF | ExHLS.DemuxingEngine.MPEGTS) ::
          GenServer.on_start()
  def start_link(url, demuxing_engine) do
    GenServer.start_link(__MODULE__, url: url, demuxing_engine: demuxing_engine)
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
  def init(url: url, demuxing_engine: demuxing_engine) do
    client = Client.new(url, demuxing_engine)
    variant_id = Client.get_variants(client) |> Enum.at(0) |> elem(0)
    client = Client.choose_variant(client, variant_id)
    {:ok, %{client: client}}
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
    {:noreply}
  end

  @impl true
  def handle_call(:get_tracks_info, _from, state) do
    {:ok, tracks_info, client} = Client.get_tracks_info(state.client)
    {:reply, tracks_info, %{state | client: client}}
  end
end
