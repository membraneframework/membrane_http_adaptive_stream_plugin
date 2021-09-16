defmodule Membrane.HTTPAdaptiveStream.StreamSinkBin do
  @moduledoc """
  This bin takes a parsed video stream and dumps an HLS playlist produced from it to storage.

  The bin payloads and muxes the incoming stream to the CMAF format and dumps it to an HLS playlist.

  # Input streams
  ! can this bin handle multiple streams??
  The bin expects parsed H264 or AAC video streams to be connected via `:input` pads.
  The type of video stream has to be specified via the `:encoding` option.

  # Output
  Specify one of `Membrane.HTTPAdaptiveStream.Storages` as `:storage` to configure the sink.


  #! generalnie to nie wiem jak pisać dokumentację
  """
  use Membrane.Bin

  alias Membrane.{ParentSpec, Time, MP4}
  alias Membrane.HTTPAdaptiveStream.{Sink, Storage}

  def_options muxer_segment_duration: [
                type: :time,
                default: 2 |> Time.seconds()
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
                default: Time.seconds(40),
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
      sink: %Sink{
        manifest_name: opts.manifest_name,
        manifest_module: opts.manifest_module,
        storage: opts.storage,
        target_window_duration: opts.target_window_duration,
        persist?: opts.persist?,
        target_segment_duration: opts.target_segment_duration
      }
    ]

    {{:ok, spec: %ParentSpec{children: children, links: []}},
     %{muxer_segment_duration: opts.muxer_segment_duration}}
  end

  @impl true
  def handle_pad_added(Pad.ref(:input, ref) = pad, context, state) do
    muxer_module = %MP4.CMAF.Muxer{segment_duration: state.muxer_segment_duration}

    payloader_module =
      case context.options[:encoding] do
        :H264 ->
          MP4.Payloader.H264

        :AAC ->
          MP4.Payloader.AAC
      end

    links = [
      link_bin_input(pad)
      |> to({:payloader, ref}, payloader_module)
      |> to({:cmaf_muxer, ref}, muxer_module)
      |> to(:sink)
    ]

    {{:ok, spec: %ParentSpec{links: links}}, %{}}
  end

  @impl true
  def handle_pad_removed(Pad.ref(:input, ref), _ctx, state) do
    children = [
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
