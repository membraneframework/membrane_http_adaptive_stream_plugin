defmodule Membrane.HTTPAdaptiveStream.SinkBinIntegrationTest do
  use ExUnit.Case

  import Membrane.Testing.Assertions
  alias Membrane.{Testing}
  alias Membrane.Hackney.Source

  @create_references_mode false

  @single_video_track_source [
    {%Source{
       location:
         "https://raw.githubusercontent.com/membraneframework/static/gh-pages/video-samples/test-video.h264",
       hackney_opts: [follow_redirect: true]
     }, :H264, "single_video_track"}
  ]
  @single_video_track_ref_path "./test/membrane_http_adaptive_stream/integration_test/reference_playlists/single_video_track/"
  @single_video_track_test_path "./test/membrane_http_adaptive_stream/integration_test/test_playlists/single_video_track/"

  defmodule TestPipeline do
    use Membrane.Pipeline
    alias Membrane.HTTPAdaptiveStream.{SinkBin, HLS}
    alias Membrane.HTTPAdaptiveStream.Storages.FileStorage
    alias Membrane.H264.FFmpeg.Parser, as: H264_Parser
    alias Membrane.AAC.Parser, as: AAC_Parser

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

      elements =
        options[:sources]
        |> Enum.map(fn {source, encoding, track_name} ->
          [
            {{:source, track_name}, source},
            {{:parser, encoding, track_name}, get_parser_for_encoding(encoding)}
          ]
        end)

      links =
        elements
        |> Enum.map(fn [
                         {{:source, name} = source_id, _},
                         {{:parser, encoding, name} = parser_id, _}
                       ] ->
          link(source_id)
          |> to(parser_id)
          |> via_in(Pad.ref(:input, name), options: [encoding: encoding, track_name: name])
          |> to(:sink_bin)
        end)

      children = elements |> List.flatten() |> Enum.into(%{:sink_bin => sink_bin})

      {{:ok, spec: %ParentSpec{children: children, links: links}}, %{}}
    end

    defp get_parser_for_encoding(encoding) do
      case encoding do
        :H264 ->
          %H264_Parser{
            framerate: {30, 1},
            alignment: :au,
            attach_nalus?: true
          }

        :AAC ->
          %AAC_Parser{}
      end
    end
  end

  test "single video track" do
    test_pipeline(
      @single_video_track_source,
      @single_video_track_ref_path,
      @single_video_track_test_path
    )
  end

  defp run_pipeline(sources, result_directory) do
    {:ok, pipeline} =
      %Testing.Pipeline.Options{
        module: TestPipeline,
        custom_args: %{
          sources: sources,
          output_dir: result_directory
        }
      }
      |> Testing.Pipeline.start_link()

    Testing.Pipeline.play(pipeline)
    assert_pipeline_playback_changed(pipeline, _, :playing)

    # For every input track pipeline receives two :end_of_stream notifications,
    # one from :sink_bin, and the other from :sink - :sink_bin inner child
    for _source <- sources,
        _end_of_stream_notification_count <- 1..2,
        do: assert_end_of_stream(pipeline, :sink_bin)

    Testing.Pipeline.stop_and_terminate(pipeline, blocking?: true)
    assert_pipeline_playback_changed(pipeline, _, :stopped)
  end

  defp test_pipeline(sources, reference_directory, test_directory) do
    if @create_references_mode do
      run_pipeline(sources, reference_directory)
    end

    run_pipeline(sources, test_directory)

    {:ok, reference_playlist_content} = File.ls(reference_directory)

    for file_name <- reference_playlist_content,
        do:
          assert(
            File.read!(reference_directory <> file_name) ==
              File.read!(test_directory <> file_name)
          )

    {:ok, test_playlist_content} = File.ls(test_directory)

    for file_name <- test_playlist_content |> Enum.filter(&(!String.starts_with?(&1, "."))),
        do: File.rm!(test_directory <> file_name)
  end
end
