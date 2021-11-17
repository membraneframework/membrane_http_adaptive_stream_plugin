defmodule Membrane.HTTPAdaptiveStream.ReferencePlaylistGenerator do
  @moduledoc """
  Generates refernece master and media playlists and stores them in directory specified in pipeline options.
  """
    alias Membrane.HTTPAdaptiveStream.{SinkBin, HLS}
    alias Membrane.HTTPAdaptiveStream.Storages.FileStorage
    alias Membrane.H264.FFmpeg.Parser, as: H264_Parser
    alias Membrane.AAC.Parser, as: AAC_Parser
    use Membrane.Pipeline

    # @impl true
    # def handle_init(options) do
    #   children = [
    #     source: %Source{
    #       location:
    #         "https://raw.githubusercontent.com/membraneframework/static/gh-pages/video-samples/test-video.h264",
    #       hackney_opts: [follow_redirect: true]
    #     },
    #     parser: %H264_Parser{
    #       framerate: {30, 1},
    #       alignment: :au,
    #       attach_nalus?: true
    #     },
    #     sink_bin: %SinkBin{
    #       muxer_segment_duration: 2 |> Membrane.Time.seconds(),
    #       manifest_module: HLS,
    #       target_window_duration: 30 |> Membrane.Time.seconds(),
    #       target_segment_duration: 2 |> Membrane.Time.seconds(),
    #       persist?: false,
    #       storage: %FileStorage{
    #         directory: options[:output_dir]
    #       }
    #     }
    #   ]

    #   links = [
    #     link(:source)
    #     |> to(:parser)
    #     |> via_in(:input, options: [encoding: :H264, track_name: "example"])
    #     |> to(:sink_bin)
    #   ]

    #   {{:ok, spec: %ParentSpec{children: children, links: links}}, %{}}
    # end
    @impl true
    def handle_init(options) do
      sink_bin = %SinkBin{
              muxer_segment_duration: 2 |> Membrane.Time.seconds(),
              manifest_module: HLS,
              target_window_duration: 30 |> Membrane.Time.seconds(),
              target_segment_duration: 2 |> Membrane.Time.seconds(),
              persist?: false,
              storage: %FileStorage{
                directory: options[:output_dir]
              }
            }

      elements = options[:sources] |> Enum.map(fn {source, encoding, track_name} -> [{{:source, track_name}, source}, {{:parser, track_name}, get_parser_for_encoding(encoding)}, [encoding: encoding, track_name: track_name]] end)
      links = elements |> Enum.map(fn [{source_id, _}, {parser_id, _}, pad_options] -> link(source_id) |> to(parser_id) |> via_in(Pad.ref(:input, pad_options[:track_name]), options: pad_options) |> to(:sink_bin) end)
      children = elements |> List.flatten() |> Enum.into(%{:sink_bin => sink_bin})
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

    defp get_parser_for_encoding(encoding) do
      case encoding do
        :H264 -> %H264_Parser{}
        :AAC -> %AAC_Parser{}
      end
    end
  end
