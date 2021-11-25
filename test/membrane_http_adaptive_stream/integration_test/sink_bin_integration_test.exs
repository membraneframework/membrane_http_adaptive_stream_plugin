defmodule Membrane.HTTPAdaptiveStream.SinkBinIntegrationTest do
  use ExUnit.Case, async: true

  import Membrane.Testing.Assertions
  alias Membrane.Testing

  @doc """
  The boolean flag below controls whether reference HLS content in fixtures directory will be created simultaneously with test content.
  It should be set only when developing new HLS features that are expected to introduce changes to reference HLS files. Nevertheless it should
  be done only locally to create and push new reference HLS files and this flag must not be set in remote repository. There is unit test in code below
  that will cause CI to fail if this flag happens to be set on remote repository.
  """
  @create_fixtures false

  @single_video_track_source [
    {"https://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/ffmpeg-testsrc.h264",
     :H264, "single_video_track"}
  ]
  @single_video_track_ref_path "./test/membrane_http_adaptive_stream/integration_test/fixtures/single_video_track/"
  @single_video_track_test_path "./test/membrane_http_adaptive_stream/integration_test/tmp/single_video_track/"

  @audio_video_tracks_sources [
    {"https://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/test-audio.aac",
     :AAC, "audio_track"},
    {"https://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/ffmpeg-testsrc.h264",
     :H264, "video_track"}
  ]
  @audio_video_tracks_ref_path "./test/membrane_http_adaptive_stream/integration_test/fixtures/audio_video_tracks/"
  @audio_video_tracks_test_path "./test/membrane_http_adaptive_stream/integration_test/tmp/audio_video_tracks/"

  @audio_multiple_video_tracks_sources [
    {"https://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s.aac",
     :AAC, "audio_track"},
    {"https://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_480x270.h264",
     :H264, "video_480x270"},
    {"https://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_540x360.h264",
     :H264, "video_540x360"},
    {"https://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_720x480.h264",
     :H264, "video_720x480"}
  ]
  @audio_multiple_video_tracks_ref_path "./test/membrane_http_adaptive_stream/integration_test/fixtures/audio_multiple_video_tracks/"
  @audio_multiple_video_tracks_test_path "./test/membrane_http_adaptive_stream/integration_test/tmp/audio_multiple_video_tracks/"

  defmodule TestPipeline do
    use Membrane.Pipeline
    alias Membrane.HTTPAdaptiveStream.{SinkBin, HLS}
    alias Membrane.HTTPAdaptiveStream.Storages.FileStorage

    @impl true
    def handle_init(%{:sources => sources, :output_dir => output_dir}) do
      sink_bin = %SinkBin{
        muxer_segment_duration: 2 |> Membrane.Time.seconds(),
        manifest_module: HLS,
        target_window_duration: 30 |> Membrane.Time.seconds(),
        target_segment_duration: 2 |> Membrane.Time.seconds(),
        persist?: false,
        storage: %FileStorage{
          directory: output_dir
        }
      }

      children =
        sources
        |> Enum.flat_map(fn {source, encoding, track_name} ->
          parser =
            case encoding do
              :H264 ->
                %Membrane.H264.FFmpeg.Parser{
                  framerate: {30, 1},
                  alignment: :au,
                  attach_nalus?: true
                }

              :AAC ->
                %Membrane.AAC.Parser{
                  out_encapsulation: :none
                }
            end

          [
            {{:source, track_name}, source},
            {{:parser, encoding, track_name}, parser}
          ]
        end)
        |> then(&[{:sink_bin, sink_bin} | &1])

      links =
        sources
        |> Enum.map(fn {_source, encoding, track_name} ->
          link({:source, track_name})
          |> to({:parser, encoding, track_name})
          |> via_in(Pad.ref(:input, track_name),
            options: [encoding: encoding, track_name: track_name]
          )
          |> to(:sink_bin)
        end)

      {{:ok, spec: %ParentSpec{children: children, links: links}}, %{}}
    end
  end

  describe "Reference HLS content creation should only take place locally." do
    test "check if fixture creation is unset" do
      assert not @create_fixtures
    end
  end

  describe "Testing HLS content creation for single media track." do
    test "single video track" do
      test_pipeline(
        @single_video_track_source,
        @single_video_track_ref_path,
        @single_video_track_test_path
      )
    end
  end

  describe "Testing HLS content creation for single audio and single video tracks." do
    test "audio and video tracks" do
      test_pipeline(
        @audio_video_tracks_sources,
        @audio_video_tracks_ref_path,
        @audio_video_tracks_test_path
      )
    end
  end

  describe "Testing HLS content creation for single audio and multiple video of various quality tracks." do
    test "audio and multiple video tracks" do
      test_pipeline(
        @audio_multiple_video_tracks_sources,
        @audio_multiple_video_tracks_ref_path,
        @audio_multiple_video_tracks_test_path
      )
    end
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

    for _source <- sources,
        do: assert_end_of_stream(pipeline, :sink_bin, {Membrane.Pad, :input, _source})

    Testing.Pipeline.stop_and_terminate(pipeline, blocking?: true)
    assert_pipeline_playback_changed(pipeline, _, :stopped)
  end

  defp test_pipeline(sources, reference_directory, test_directory) do
    hackney_sources =
      sources
      |> Enum.map(fn {path, encoding, name} ->
        {%Membrane.Hackney.Source{location: path, hackney_opts: [follow_redirect: true]},
         encoding, name}
      end)

    if @create_fixtures do
      run_pipeline(hackney_sources, reference_directory)
    end

    on_exit(fn ->
      {:ok, test_playlist_content} = File.ls(test_directory)

      for file_name <- test_playlist_content |> Enum.filter(&(!String.starts_with?(&1, "."))),
          do: File.rm!(test_directory <> file_name)
    end)

    run_pipeline(hackney_sources, test_directory)

    {:ok, reference_playlist_content} = File.ls(reference_directory)

    for file_name <- reference_playlist_content,
        do:
          assert(
            File.read!(reference_directory <> file_name) ==
              File.read!(test_directory <> file_name)
          )
  end
end
