Mix.install([
  :membrane_core,
  {:membrane_http_adaptive_stream_plugin, path: Path.expand("../"), override: true},
  :membrane_h264_ffmpeg_plugin,
  :membrane_mp4_plugin,
  :membrane_hackney_plugin
])

defmodule MyBin do # TODO Membrane.HTTPAdaptiveStream.SomeNameBin jak go nazwać? jak opisać?
  use Membrane.Bin

  def_options muxer_segment_duration: [
                type: :time,
                default: 2 |> Membrane.Time.seconds()
              ],
              manifest_name: [
                type: :string,
                spec: String.t(),
                default: "index",
                description: "Name of the main manifest file"
              ],
              manifest_module: [
                type: :atom,
                spec: module,
                description: """
                Implementation of the `Membrane.HTTPAdaptiveStream.Manifest`
                behaviour.
                """
              ],
              storage: [
                type: :struct,
                spec: Storage.config_t(),
                description: """
                Storage configuration. May be one of `Membrane.HTTPAdaptiveStream.Storages.*`.
                See `Membrane.HTTPAdaptiveStream.Storage` behaviour.
                """
              ],
              target_window_duration: [
                spec: pos_integer | :infinity,
                type: :time,
                default: Membrane.Time.seconds(40),
                description: """
                Manifest duration is keept above that time, while the oldest segments
                are removed whenever possible.
                """
              ],
              persist?: [
                type: :bool,
                default: false,
                description: """
                If true, stale segments are removed from the manifest only. Once
                playback finishes, they are put back into the manifest.
                """
              ],
              target_segment_duration: [
                type: :time,
                default: 0,
                description: """
                Expected length of each segment. Setting it is not necessary, but
                may help players achieve better UX.
                """
              ]

  def_input_pad :input,
  demand_unit: :buffers,
  caps: [Membrane.H264, Membrane.AAC],
  availability: :on_request,
  options: [
    encoding: [
      spec: :H264 | :AAC,
      description: """
      Encoding atom determining which payloader will be used.
      """
    ]
  ]

  @impl true
  def handle_init(opts) do
    children = [
      sink: %Membrane.HTTPAdaptiveStream.Sink{
        manifest_name: opts.manifest_name,
        manifest_module: opts.manifest_module,
        storage: opts.storage,
        target_window_duration: opts.target_window_duration,
        persist?: opts.persist?,
        target_segment_duration: opts.target_segment_duration,
      }
    ]

    {{:ok, spec: %ParentSpec{children: children, links: []}}, %{muxer_segment_duration: opts.muxer_segment_duration}}
  end

  @impl true
  def handle_pad_added(Pad.ref(:input, ref) = pad, context, state) do
    muxer_module = %Membrane.MP4.CMAF.Muxer{segment_duration: state.muxer_segment_duration}
    payloader_module = case context.options[:encoding] do
      :H264 ->
        Membrane.MP4.Payloader.H264
      :AAC ->
        Membrane.MP4.Payloader.AAC
    end

    links = [
      link_bin_input(pad)
      |> to({:payloader, ref}, payloader_module)
      |> to({:cmaf_muxer, ref}, muxer_module)
      |> to(:sink)
    ]

    children = [
      {:payloader, ref} => payloader_module,
      {:cmaf_muxer, ref} => muxer_module
    ]

    {{:ok, spec: %ParentSpec{children: children, links: links}}, %{}} #! tu czyszczę state'a
  end

  @impl true
  def handle_pad_removed(Pad.ref(:input, ref), _ctx, state) do
    children =
              [



        {:payloader, ref},
        {:cmaf_muxer, ref}
      ]
    {{:ok, remove_child: children}, state}
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
      my_bin: %MyBin{
        muxer_segment_duration: 2 |> Membrane.Time.seconds(),
        manifest_module: Membrane.HTTPAdaptiveStream.HLS,
        target_window_duration: 30 |> Membrane.Time.seconds(),
        target_segment_duration: 2 |> Membrane.Time.seconds(),
        persist?: false,
        storage: %Membrane.HTTPAdaptiveStream.Storages.FileStorage{directory: __DIR__}
      }
    ]

    links = [
      link(:source)
      |> to(:parser)
      |> via_in(:input, options: [encoding: :H264])
      |> to(:my_bin)
    ]

    {{:ok, spec: %ParentSpec{children: children, links: links}}, %{}}
  end

  @impl true
  def handle_notification(:end_of_stream, :my_bin, _context, state) do
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
