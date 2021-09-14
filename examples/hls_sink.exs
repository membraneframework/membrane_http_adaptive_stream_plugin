Mix.install([
  :membrane_core,
  {:membrane_http_adaptive_stream_plugin, path: Path.expand("../"), override: true},
  :membrane_h264_ffmpeg_plugin,
  :membrane_mp4_plugin,
  :membrane_hackney_plugin
])

defmodule MyBin do
  use Membrane.Bin

  def_input_pad :input, demand_unit: :buffers, caps: :any
  # def_output_pad :output, demand_unit: :buffers, caps: :any
  def_options segment_duration: [type: :integer]

  @impl true
  def handle_init(opts) do
    children = [
      payloader: Membrane.MP4.Payloader.H264,
      cmaf_muxer: %Membrane.MP4.CMAF.Muxer{
        segment_duration: opts.segment_duration |> Membrane.Time.seconds()
      },
      sink: %Membrane.HTTPAdaptiveStream.Sink{
        manifest_module: Membrane.HTTPAdaptiveStream.HLS,
        target_window_duration: 30 |> Membrane.Time.seconds(),
        target_segment_duration: 2 |> Membrane.Time.seconds(),
        persist?: false,
        storage: %Membrane.HTTPAdaptiveStream.Storages.FileStorage{directory: __DIR__}
      }
    ]

    links = [
      link_bin_input()
      |> to(:payloader)
      |> to(:cmaf_muxer)
      |> to(:sink)
    ]

    {{:ok, spec: %ParentSpec{children: children, links: links}}, %{}}
  end

  @impl true
  def handle_element_end_of_stream({:sink, _}, _ctx, state) do
    {{:ok, notify: :end_of_stream}, state}
  end

  def handle_element_end_of_stream(_element, _ctx, state) do
    {:ok, state}
  end
end

defmodule Example do
  @moduledoc """
  An example pipeline showing how to use `Membrane.HTTPAdaptiveStream.Sink` element.

  The pipeline will download a file containing h264 stream, parse and payload the video stream, mux it to CMAF format and
  finally dump it to an HLS playlist.

  Given output directory for hls playlist must exist before running the example. You can either modify the script to change the directory yourself
  or provide `HLS_OUTPUT_DIR` environmental variable pointing to preferred path.

  To play the stream you will need an http server and an hls player. The easiest way is to go with a built-in python http server and `ffplay` command
  provided by ffmpeg.

  ```bash
  # run this command in the output directory
  python3 -m http.server 8000`
  ```

  ```bash
  # run this command to play the stream
  ffplay http://localhost:8000/index.m3u8
  ```

  ## Run
  The pipeline can be run as a script therefore it will download all necessary dependencies:
  ```bash
  elixir hls_sink.exs
  ```
  """

  use Membrane.Pipeline

  @impl true
  def handle_init(_) do
    children = [
      source: %Membrane.Hackney.Source{
        location: "https://raw.githubusercontent.com/membraneframework/static/gh-pages/video-samples/test-video.h264",
        hackney_opts: [follow_redirect: true]
      },
      parser: %Membrane.H264.FFmpeg.Parser{
        framerate: {30, 1},
        alignment: :au,
        attach_nalus?: true
      },
      bin: %MyBin{
        segment_duration: 2
      }
    ]

    links = [
      link(:source)
      |> to(:parser)
      |> to(:bin)
    ]

    {{:ok, spec: %ParentSpec{children: children, links: links}}, %{}}
  end

  @impl true
  def handle_notification(:end_of_stream, :bin, _context, state) do
    Membrane.Pipeline.stop_and_terminate(self())
    {:ok, state}
  end

  def handle_notification(_notification, _element, _context, state) do
    {:ok, state}
  end
end

ref =
  Example.start_link()
  |> elem(1)
  |> tap(&Membrane.Pipeline.play/1)
  |> then(&Process.monitor/1)

receive do
  {:DOWN, ^ref, :process, _pid, _reason} ->
    :ok
end
