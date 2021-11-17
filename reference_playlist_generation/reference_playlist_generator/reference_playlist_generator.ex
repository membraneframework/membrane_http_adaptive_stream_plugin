defmodule Membrane.HTTPAdaptiveStream.ReferencePlaylistGenerator do
  @moduledoc """
  Generates refernece master and media playlists and stores them in directory specified in pipeline options.
  """
    alias Membrane.HTTPAdaptiveStream.{SinkBin, HLS}
    alias Membrane.HTTPAdaptiveStream.Storages.FileStorage
    alias Membrane.Hackney.Source
    alias Membrane.H264.FFmpeg.Parser
    use Membrane.Pipeline

    @impl true
    def handle_init(options) do
      children = [
        source: %Source{
          location:
            "https://raw.githubusercontent.com/membraneframework/static/gh-pages/video-samples/test-video.h264",
          hackney_opts: [follow_redirect: true]
        },
        parser: %Parser{
          framerate: {30, 1},
          alignment: :au,
          attach_nalus?: true
        },
        sink_bin: %SinkBin{
          muxer_segment_duration: 2 |> Membrane.Time.seconds(),
          manifest_module: HLS,
          target_window_duration: 30 |> Membrane.Time.seconds(),
          target_segment_duration: 2 |> Membrane.Time.seconds(),
          persist?: false,
          storage: %FileStorage{
            directory: options[:output_dir]
          }
        }
      ]

      links = [
        link(:source)
        |> to(:parser)
        |> via_in(:input, options: [encoding: :H264, track_name: "example"])
        |> to(:sink_bin)
      ]

      {{:ok, spec: %ParentSpec{children: children, links: links}}, %{}}
    end

    @impl true
    def handle_notification(:end_of_stream, :sink_bin, _context, state) do
      Membrane.Pipeline.stop_and_terminate(self())
      {:ok, state}
    end

    def handle_notification(_notification, _element, _context, state) do
      {:ok, state}
    end

  end
