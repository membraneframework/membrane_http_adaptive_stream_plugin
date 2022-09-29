defmodule Membrane.HTTPAdaptiveStream.SinkBin do
  @moduledoc """
  Bin responsible for receiving audio and video streams, performing payloading and CMAF muxing
  to eventually store them using provided storage configuration.

  ## Input streams
  Parsed H264 or AAC video or audio streams are expected to be connected via the `:input` pad.
  The type of stream has to be specified via the pad's `:encoding` option.

  ## Output
  Specify one of `Membrane.HTTPAdaptiveStream.Storages` as `:storage` to configure the sink.
  """
  use Membrane.Bin

  alias Membrane.{MP4, ParentSpec, Time}
  alias Membrane.HTTPAdaptiveStream.{Manifest, Sink, Storage}
  alias Membrane.MP4.Muxer.CMAF

  @payloaders %{H264: MP4.Payloader.H264, AAC: MP4.Payloader.AAC}

  def_options manifest_name: [
                spec: String.t(),
                default: "index",
                description: "Name of the main manifest file"
              ],
              manifest_module: [
                spec: module,
                description: """
                Implementation of the `Membrane.HTTPAdaptiveStream.Manifest`
                behaviour.
                """
              ],
              storage: [
                spec: Storage.config_t(),
                description: """
                Storage configuration. May be one of `Membrane.HTTPAdaptiveStream.Storages.*`.
                See `Membrane.HTTPAdaptiveStream.Storage` behaviour.
                """
              ],
              target_window_duration: [
                spec: pos_integer | :infinity,
                default: Time.seconds(40),
                description: """
                Manifest duration is kept above that time, while the oldest segments
                are removed whenever possible.
                """
              ],
              persist?: [
                spec: boolean,
                default: false,
                description: """
                If true, stale segments are removed from the manifest only. Once
                playback finishes, they are put back into the manifest.
                """
              ],
              mode: [
                spec: :live | :vod,
                default: :vod,
                description: """
                Tells if the session is live or a VOD type of broadcast. It can influence type of metadata
                inserted into the playlist's manifest.
                """
              ],
              hls_mode: [
                spec: :muxed_av | :separate_av,
                default: :separate_av,
                description: """
                Option defining how the incoming tracks will be handled and how CMAF will be muxed.

                - In `:muxed_av` audio will be added to each video rendition, creating CMAF segments that contain both audio and video.
                - In `:separate_av` audio and video tracks will be separate and synchronization will need to be sorted out by the player.
                """
              ],
              header_naming_fun: [
                spec: (Manifest.Track.t(), counter :: non_neg_integer() -> String.t()),
                default: &Manifest.Track.default_header_naming_fun/2,
                description:
                  "A function that generates consequent media header names for a given track"
              ],
              segment_naming_fun: [
                type: :function,
                spec: (Manifest.Track.t() -> String.t()),
                default: &Manifest.Track.default_segment_naming_fun/1,
                description:
                  "A function that generates consequent segment names for a given track"
              ]

  def_input_pad :input,
    demand_unit: :buffers,
    caps: [{Membrane.H264, profile: one_of([:constrained_baseline, :baseline])}, Membrane.AAC],
    availability: :on_request,
    options: [
      encoding: [
        spec: :H264 | :AAC,
        description: """
        Encoding type determining which payloader will be used for the given stream.
        """
      ],
      track_name: [
        spec: String.t() | nil,
        default: nil,
        description: """
        Name that will be used to name the media playlist for the given track, as well as its header and segments files.
        It must not contain any URI reserved characters
        """
      ],
      segment_duration: [
        spec: Sink.SegmentDuration.t(),
        description: """
        The segment duration range  of the regular segments.
        """
      ],
      partial_segment_duration: [
        spec: Sink.SegmentDuration.t() | nil,
        default: nil,
        description: """
        The segment duration range  of the partial segments.
        If not set then the bin won't produce any partial segments.
        """
      ]
    ]

  @impl true
  def handle_init(opts) do
    children =
      [
        sink: %Sink{
          manifest_name: opts.manifest_name,
          manifest_module: opts.manifest_module,
          storage: opts.storage,
          target_window_duration: opts.target_window_duration,
          persist?: opts.persist?,
          header_naming_fun: opts.header_naming_fun,
          segment_naming_fun: opts.segment_naming_fun,
          mode: opts.mode
        }
      ] ++
        if(opts.hls_mode == :muxed_av, do: [audio_tee: Membrane.Tee.Parallel], else: [])

    state = %{
      mode: opts.hls_mode,
      streams_to_start: 0,
      streams_to_end: 0
    }

    {{:ok, spec: %ParentSpec{children: children}}, state}
  end

  defp track_options(context) do
    %{
      track_name: track_name,
      segment_duration: segment_duration,
      partial_segment_duration: partial_segment_duration
    } = context.options

    target_partial_segment_duration =
      case partial_segment_duration do
        nil -> nil
        duration -> duration.target
      end

    [
      track_name: track_name,
      segment_duration: segment_duration,
      target_partial_segment_duration: target_partial_segment_duration
    ]
  end

  @impl true
  def handle_pad_added(Pad.ref(:input, ref) = pad, context, state) do
    %{
      encoding: encoding,
      segment_duration: segment_duration,
      partial_segment_duration: partial_segment_duration
    } = context.options

    muxer = %MP4.Muxer.CMAF{
      segment_duration_range: convert_segment_duration_for_muxer(segment_duration),
      partial_segment_duration_range: convert_segment_duration_for_muxer(partial_segment_duration)
    }

    payloader = Map.fetch!(@payloaders, encoding)

    spec =
      cond do
        state.mode == :separate_av ->
          %ParentSpec{
            links: [
              link_bin_input(pad)
              |> to({:payloader, ref}, payloader)
              |> to({:cmaf_muxer, ref}, muxer)
              |> via_in(pad, options: track_options(context))
              |> to(:sink)
            ]
          }

        state.mode == :muxed_av and encoding == :H264 ->
          %ParentSpec{
            children: %{
              {:payloader, ref} => payloader,
              {:cmaf_muxer, ref} => muxer
            },
            links: [
              link_bin_input(pad)
              |> to({:payloader, ref})
              |> to({:cmaf_muxer, ref}),
              link(:audio_tee)
              |> to({:cmaf_muxer, ref}),
              link({:cmaf_muxer, ref})
              |> via_in(pad, options: track_options(context))
              |> to(:sink)
            ]
          }

        state.mode == :muxed_av and encoding == :AAC ->
          if count_audio_tracks(context) > 1,
            do: raise("In :muxed_av mode, only one audio input is accepted")

          %ParentSpec{
            children: %{{:payloader, ref} => payloader},
            links: [
              link_bin_input(pad)
              |> to({:payloader, ref})
              |> to(:audio_tee)
            ]
          }
      end

    update_streams_count = fn count ->
      if state.mode == :separate_av or encoding == :H264 do
        count + 1
      else
        count
      end
    end

    state =
      state
      |> Map.update!(:streams_to_start, update_streams_count)
      |> Map.update!(:streams_to_end, update_streams_count)

    {{:ok, spec: spec}, state}
  end

  @impl true
  def handle_pad_removed(Pad.ref(:input, ref), ctx, state) do
    children =
      ([
         {:payloader, ref}
       ] ++ if(state.mode != :muxed_av, do: [{:cmaf_muxer, ref}], else: []))
      |> Enum.filter(fn child_name ->
        child_entry = Map.get(ctx.children, child_name)
        child_entry != nil and !child_entry.terminating?
      end)

    {{:ok, remove_child: children}, state}
  end

  @impl true
  def handle_element_start_of_stream(
        {:sink, _},
        _ctx,
        %{streams_to_start: 1} = state
      ) do
    {{:ok, notify: :start_of_stream}, %{state | streams_to_start: 0}}
  end

  @impl true
  def handle_element_start_of_stream({:sink, _}, _ctx, state) do
    {:ok, Map.update!(state, :streams_to_start, &(&1 - 1))}
  end

  @impl true
  def handle_element_start_of_stream(_element, _ctx, state) do
    {:ok, state}
  end

  def handle_element_end_of_stream({:sink, _}, _ctx, %{streams_to_end: 1} = state) do
    {{:ok, notify: :end_of_stream}, %{state | streams_to_end: 0}}
  end

  @impl true
  def handle_element_end_of_stream({:sink, _}, _ctx, state) do
    {:ok, Map.update!(state, :streams_to_end, &(&1 - 1))}
  end

  @impl true
  def handle_element_end_of_stream(_element, _ctx, state) do
    {:ok, state}
  end

  @impl true
  def handle_notification(
        {:track_playable, track_info},
        :sink,
        _ctx,
        state
      ) do
    # notify about playable just when track becomes available
    {{:ok, notify: {:track_playable, track_info}}, state}
  end

  @impl true
  def handle_notification(
        {:cleanup, cleanup},
        :sink,
        _ctx,
        state
      ) do
    {{:ok, notify: {:cleanup, cleanup}}, state}
  end

  defp count_audio_tracks(context),
    do:
      Enum.count(context.pads, fn {_pad, metadata} ->
        metadata.options.encoding == :AAC
      end)

  defp convert_segment_duration_for_muxer(nil), do: nil

  defp convert_segment_duration_for_muxer(%Sink.SegmentDuration{min: min, target: target}),
    do: %CMAF.SegmentDurationRange{min: min, target: target}
end
