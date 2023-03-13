defmodule Membrane.HTTPAdaptiveStream.SinkBinIntegrationTest do
  use ExUnit.Case, async: true

  import Membrane.Testing.Assertions
  alias Membrane.Testing

  # The boolean flag below controls whether reference HLS content in fixtures directory will be created simultaneously with test content.
  # It should be set only when developing new HLS features that are expected to introduce changes to reference HLS files. Nevertheless it should
  # be done only locally to create and push new reference HLS files and this flag must not be set in remote repository. There is unit test in code below
  # that will cause CI to fail if this flag happens to be set on remote repository. Every new version of reference HSL content must
  # be manually verified by its creator by using some player e.g. ffplay command.

  @create_fixtures false

  @audio_video_tracks_sources [
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/test-audio.aac",
     :AAC, "audio_track"},
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/ffmpeg-testsrc.h264",
     :H264, "video_track"}
  ]
  @audio_video_tracks_ref_path "./test/membrane_http_adaptive_stream/integration_test/fixtures/audio_video_tracks/"
  @audio_video_tracks_test_path "/tmp/membrane_http_adaptive_stream_audio_video_test/"

  @audio_multiple_video_tracks_sources [
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s.aac",
     :AAC, "audio_track"},
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_480x270.h264",
     :H264, "video_480x270"},
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_540x360.h264",
     :H264, "video_540x360"},
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_720x480.h264",
     :H264, "video_720x480"}
  ]
  @audio_multiple_video_tracks_ref_path "./test/membrane_http_adaptive_stream/integration_test/fixtures/audio_multiple_video_tracks/"
  @audio_multiple_video_tracks_test_path "/tmp/membrane_http_adaptive_stream_audio_multiple_video_test/"

  @muxed_av_sources [
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s.aac",
     :AAC, "audio_track"},
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_480x270.h264",
     :H264, "video_480x270"},
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_540x360.h264",
     :H264, "video_540x360"},
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_720x480.h264",
     :H264, "video_720x480"}
  ]
  @muxed_av_ref_path "./test/membrane_http_adaptive_stream/integration_test/fixtures/muxed_av/"
  @muxed_av_test_path "/tmp/membrane_http_adaptive_stream_muxed_av_test/"

  defmodule TestPipeline do
    use Membrane.Pipeline

    alias Membrane.HTTPAdaptiveStream
    alias Membrane.HTTPAdaptiveStream.Manifest.Track.SegmentDuration
    alias Membrane.HTTPAdaptiveStream.Storages.FileStorage
    alias Membrane.Time

    @impl true
    def handle_init(_ctx, %{
          sources: sources,
          storage: storage,
          hls_mode: hls_mode,
          partial_segments: partial_segments
        }) do
      sink_bin = %HTTPAdaptiveStream.SinkBin{
        manifest_module: HTTPAdaptiveStream.HLS,
        target_window_duration: 30 |> Membrane.Time.seconds(),
        persist?: false,
        storage: storage,
        hls_mode: hls_mode,
        mode: if(partial_segments, do: :live, else: :vod)
      }

      children =
        sources
        |> Enum.flat_map(fn {source, encoding, track_name} ->
          parser =
            case encoding do
              :H264 ->
                %Membrane.H264.FFmpeg.Parser{
                  framerate: {25, 1},
                  alignment: :au,
                  attach_nalus?: true,
                  skip_until_parameters?: false
                }

              :AAC ->
                %Membrane.AAC.Parser{
                  out_encapsulation: :none
                }
            end

          [
            child({:source, track_name}, source)
            |> child({:parser, track_name}, parser)
          ]
        end)
        |> then(&[child(:sink_bin, sink_bin) | &1])

      structure =
        children ++
          Enum.map(sources, fn {_source, encoding, track_name} ->
            get_child({:parser, track_name})
            |> via_in(Pad.ref(:input, track_name),
              options: [
                encoding: encoding,
                track_name: track_name,
                segment_duration: segment_duration_for(encoding),
                partial_segment_duration:
                  if(partial_segments, do: partial_segment_duration_for(encoding), else: nil)
              ]
            )
            |> get_child(:sink_bin)
          end)

      {[spec: structure, playback: :playing], %{}}
    end

    defp segment_duration_for(:AAC),
      do: SegmentDuration.new(Time.milliseconds(2000), Time.milliseconds(2000))

    defp segment_duration_for(:H264),
      do: SegmentDuration.new(Time.milliseconds(1500), Time.milliseconds(2000))

    defp partial_segment_duration_for(:AAC),
      do: SegmentDuration.new(Time.milliseconds(250), Time.milliseconds(500))

    defp partial_segment_duration_for(:H264),
      do: SegmentDuration.new(Time.milliseconds(250), Time.milliseconds(500))
  end

  test "check if fixture creation is disabled" do
    refute @create_fixtures
  end

  describe "Test HLS content creation for " do
    setup %{test_directory: test_directory} do
      File.mkdir_p!(test_directory)
      :ok
    end

    @tag test_directory: @audio_video_tracks_test_path
    test "audio and video tracks" do
      test_pipeline(
        @audio_video_tracks_sources,
        @audio_video_tracks_ref_path,
        @audio_video_tracks_test_path
      )
    end

    @tag test_directory: @audio_multiple_video_tracks_test_path
    test "audio and multiple video tracks" do
      test_pipeline(
        @audio_multiple_video_tracks_sources,
        @audio_multiple_video_tracks_ref_path,
        @audio_multiple_video_tracks_test_path
      )
    end

    @tag test_directory: @muxed_av_test_path
    test "audio and multiple video tracks - muxed AV" do
      test_pipeline(
        @muxed_av_sources,
        @muxed_av_ref_path,
        @muxed_av_test_path,
        :muxed_av
      )
    end

    @tag test_directory: @audio_video_tracks_test_path
    test "audio and video tracks with partial segments" do
      alias Membrane.HTTPAdaptiveStream.Storages.SendStorage

      hackney_sources =
        @audio_video_tracks_sources
        |> Enum.map(fn {path, encoding, name} ->
          {%Membrane.Hackney.Source{location: path, hackney_opts: [follow_redirect: true]},
           encoding, name}
        end)

      pipeline =
        Testing.Pipeline.start_link_supervised!(
          module: TestPipeline,
          custom_args: %{
            sources: hackney_sources,
            hls_mode: :separate_av,
            partial_segments: true,
            storage: %SendStorage{destination: self()}
          }
        )

      assert_pipeline_play(pipeline)

      assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}, 1_000

      assert_receive {SendStorage, :store, %{type: :manifest, name: "video_track.m3u8"}}, 500

      assert_receive {SendStorage, :store, %{type: :manifest, name: "audio_track.m3u8"}}, 500

      # the values below define a set of expected partial segments
      # that should be followed by regular segments containing all previous parts
      # (belonging to the current segment)
      expected_segments = [
        {:video, 0, 21},
        {:video, 1, 5},
        {:audio, 0, 4},
        {:audio, 1, 4},
        {:audio, 2, 4},
        {:audio, 3, 4},
        {:audio, 4, 4},
        {:audio, 5, 1}
      ]

      for {type, segment_idx, parts} <- expected_segments do
        manifest_name = "#{type}_track.m3u8"
        segment_name = "#{type}_segment_#{segment_idx}_#{type}_track.m4s"

        partial_segments =
          for _i <- 1..parts do
            assert_receive {SendStorage, :store,
                            %{
                              name: ^segment_name,
                              type: :partial_segment,
                              contents: segment
                            }}

            assert_receive {SendStorage, :store, %{type: :manifest, name: ^manifest_name}}

            segment
          end

        full_segment = IO.iodata_to_binary(partial_segments)

        assert_receive {SendStorage, :store,
                        %{
                          name: ^segment_name,
                          type: :segment,
                          contents: ^full_segment
                        }}
      end

      assert_pipeline_notified(pipeline, :sink_bin, :end_of_stream)

      :ok = Testing.Pipeline.terminate(pipeline, blocking?: true)
    end
  end

  defp run_pipeline(sources, result_directory, hls_mode) do
    alias Membrane.HTTPAdaptiveStream.Storages.FileStorage

    pipeline =
      [
        module: TestPipeline,
        custom_args: %{
          sources: sources,
          hls_mode: hls_mode,
          partial_segments: false,
          storage: %FileStorage{
            directory: result_directory
          }
        }
      ]
      |> Testing.Pipeline.start_link_supervised!()

    assert_pipeline_play(pipeline)

    assert_pipeline_notified(pipeline, :sink_bin, :end_of_stream, 5_000)

    # Give some time to save all of the files to disk
    Process.sleep(1_000)

    :ok = Testing.Pipeline.terminate(pipeline, blocking?: true)
  end

  defp test_pipeline(sources, reference_directory, test_directory, hls_mode \\ :separate_av) do
    hackney_sources =
      sources
      |> Enum.map(fn {path, encoding, name} ->
        {%Membrane.Hackney.Source{location: path, hackney_opts: [follow_redirect: true]},
         encoding, name}
      end)

    # on_exit(fn ->
    #   File.rm_rf!(test_directory)
    # end)

    if @create_fixtures do
      File.rm_rf(reference_directory)
      File.mkdir(reference_directory)
      run_pipeline(hackney_sources, reference_directory, hls_mode)
    else
      run_pipeline(hackney_sources, test_directory, hls_mode)

      {:ok, reference_playlist_content} = File.ls(reference_directory)

      for file_name <- reference_playlist_content,
          do:
            assert(
              File.read!(reference_directory <> file_name) ==
                File.read!(test_directory <> file_name),
              "Contents of file #{reference_directory <> file_name} differ from contents of file #{test_directory <> file_name}"
            )
    end
  end
end
