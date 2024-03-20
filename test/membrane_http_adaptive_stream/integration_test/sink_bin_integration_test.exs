defmodule Membrane.HTTPAdaptiveStream.SinkBinIntegrationTest do
  use ExUnit.Case, async: true

  import Membrane.Testing.Assertions

  alias Membrane.Testing

  # The boolean flag below controls whether reference HLS content in fixtures directory will be created simultaneously with test content.
  # It should be set only when developing new HLS features that are expected to introduce changes to reference HLS files. Nevertheless it should
  # be done only locally to create and push new reference HLS files and this flag must not be set in remote repository. There is unit test in code below
  # that will cause CI to fail if this flag happens to be set on remote repository. Every new version of reference HLS content must
  # be manually verified by its creator by using some player e.g. ffplay command.

  @pipeline_config %{
    hls_mode: :separate_av,
    target_window_duration: Membrane.Time.seconds(30),
    persist?: false
  }
  @create_fixtures false

  @min_number_of_segments_in_delta_playlist 6

  @audio_video_tracks_sources [
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/test-audio.aac",
     :AAC, :LC, "audio_track"},
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/ffmpeg-testsrc.h264",
     :H264, :high, "video_track"}
  ]
  @audio_video_tracks_ref_path "./test/membrane_http_adaptive_stream/integration_test/fixtures/audio_video_tracks/"
  @live_stream_ref_path "./test/membrane_http_adaptive_stream/integration_test/fixtures/live/"
  @persisted_stream_ref_path "./test/membrane_http_adaptive_stream/integration_test/fixtures/persisted/"

  @audio_video_hevc_tracks_source [
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/test-audio.aac",
     :AAC, :LC, "audio_track"},
    {"test/membrane_http_adaptive_stream/integration_test/fixtures/ffmpeg-testsrc.hevc", :H265,
     :main, "video_track"}
  ]
  @audio_video_hevc_tracks_ref_path "./test/membrane_http_adaptive_stream/integration_test/fixtures/audio_video_hevc_tracks/"

  @audio_multiple_video_tracks_sources [
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s.aac",
     :AAC, :LC, "audio_track"},
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_480x270.h264",
     :H264, :constrained_baseline, "video_480x270"},
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_540x360.h264",
     :H264, :high, "video_540x360"},
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_720x480.h264",
     :H264, :high, "video_720x480"}
  ]
  @audio_multiple_video_tracks_ref_path "./test/membrane_http_adaptive_stream/integration_test/fixtures/audio_multiple_video_tracks/"

  @muxed_av_sources [
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s.aac",
     :AAC, :LC, "audio_track"},
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_480x270.h264",
     :H264, :constrained_baseline, "video_480x270"},
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_540x360.h264",
     :H264, :high, "video_540x360"},
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_720x480.h264",
     :H264, :high, "video_720x480"}
  ]
  @muxed_av_ref_path "./test/membrane_http_adaptive_stream/integration_test/fixtures/muxed_av/"

  @delta_test_sources [
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_480x270_120s.h264",
     :H264, :high, "long_video"},
    {"http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_120s.aac",
     :AAC, :LC, "long_audio"}
  ]

  @eps 1.0e-8

  defmodule TestPipeline do
    use Membrane.Pipeline

    alias Membrane.HTTPAdaptiveStream
    alias Membrane.HTTPAdaptiveStream.Storages.FileStorage
    alias Membrane.Time

    @non_b_frames_profiles [:constrained_baseline, :baseline]

    @impl true
    def handle_init(_ctx, %{
          sources: sources,
          storage: storage,
          hls_mode: hls_mode,
          target_window_duration: target_window_duration,
          partial_segments: partial_segments,
          persist?: persist?
        }) do
      sink_bin = %HTTPAdaptiveStream.SinkBin{
        manifest_module: HTTPAdaptiveStream.HLS,
        target_window_duration: target_window_duration,
        persist?: persist?,
        storage: storage,
        hls_mode: hls_mode,
        mode: if(partial_segments, do: :live, else: :vod),
        cleanup_after: Membrane.Time.second()
      }

      children =
        sources
        |> Enum.flat_map(fn {source, encoding, profile, track_name} ->
          parser =
            case {encoding, profile} do
              {:H264, profile} when profile in @non_b_frames_profiles ->
                %Membrane.H264.Parser{
                  output_alignment: :au,
                  generate_best_effort_timestamps: %{framerate: {25, 1}, add_dts_offset: false}
                }

              {:H264, _profile} ->
                %Membrane.H264.Parser{
                  output_alignment: :au,
                  generate_best_effort_timestamps: %{framerate: {25, 1}}
                }

              {:H265, _profile} ->
                %Membrane.H265.Parser{
                  output_alignment: :au,
                  generate_best_effort_timestamps: %{framerate: {25, 1}}
                }

              {:AAC, _profile} ->
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
          Enum.map(sources, fn {_source, encoding, _profile, track_name} ->
            get_child({:parser, track_name})
            |> via_in(Pad.ref(:input, track_name),
              options: [
                encoding: encoding,
                track_name: track_name,
                segment_duration: segment_duration_for(encoding),
                partial_segment_duration:
                  if(partial_segments, do: partial_segment_duration_for(encoding), else: nil),
                max_framerate: if(encoding in [:H264, :H265], do: 25, else: nil)
              ]
            )
            |> get_child(:sink_bin)
          end)

      {[spec: structure], %{}}
    end

    defp segment_duration_for(:AAC),
      do: Time.milliseconds(2000)

    defp segment_duration_for(codec) when codec in [:H264, :H265],
      do: Time.milliseconds(2000)

    defp partial_segment_duration_for(:AAC),
      do: Time.milliseconds(500)

    defp partial_segment_duration_for(codec) when codec in [:H264, :H265],
      do: Time.milliseconds(500)
  end

  test "check if fixture creation is disabled" do
    refute @create_fixtures
  end

  describe "Test HLS content creation for" do
    @tag :tmp_dir
    test "audio and video tracks", %{tmp_dir: tmp_dir} do
      test_pipeline(
        @audio_video_tracks_sources,
        @audio_video_tracks_ref_path,
        tmp_dir
      )
    end

    @tag :tmp_dir
    test "audio and hevc video tracks", %{tmp_dir: tmp_dir} do
      test_pipeline(
        @audio_video_hevc_tracks_source,
        @audio_video_hevc_tracks_ref_path,
        tmp_dir
      )
    end

    @tag :tmp_dir
    test "audio and multiple video tracks", %{tmp_dir: tmp_dir} do
      test_pipeline(
        @audio_multiple_video_tracks_sources,
        @audio_multiple_video_tracks_ref_path,
        tmp_dir
      )
    end

    @tag :tmp_dir
    test "audio and multiple video tracks live playlist", %{tmp_dir: tmp_dir} do
      pipeline_config = %{@pipeline_config | target_window_duration: Membrane.Time.seconds(10)}

      test_pipeline(
        @audio_multiple_video_tracks_sources,
        @live_stream_ref_path,
        tmp_dir,
        pipeline_config
      )
    end

    @tag :tmp_dir
    test "audio and multiple video tracks persisted mode", %{tmp_dir: tmp_dir} do
      pipeline_config = %{
        @pipeline_config
        | target_window_duration: Membrane.Time.seconds(10),
          persist?: true
      }

      test_pipeline(
        @audio_multiple_video_tracks_sources,
        @persisted_stream_ref_path,
        tmp_dir,
        pipeline_config
      )
    end

    @tag :tmp_dir
    test "audio and multiple video tracks - muxed AV", %{tmp_dir: tmp_dir} do
      pipeline_config = %{@pipeline_config | hls_mode: :muxed_av}

      test_pipeline(
        @muxed_av_sources,
        @muxed_av_ref_path,
        tmp_dir,
        pipeline_config
      )
    end

    @tag :tmp_dir
    test "test creation of delta playlist", %{tmp_dir: tmp_dir} do
      alias Membrane.HTTPAdaptiveStream.Storages.FileStorage

      hackney_sources =
        @delta_test_sources
        |> Enum.map(fn {path, encoding, profile, name} ->
          {%Membrane.Hackney.Source{location: path, hackney_opts: [follow_redirect: true]},
           encoding, profile, name}
        end)

      pipeline =
        Testing.Pipeline.start_link_supervised!(
          module: TestPipeline,
          custom_args: %{
            sources: hackney_sources,
            hls_mode: @pipeline_config.hls_mode,
            partial_segments: true,
            target_window_duration: :infinity,
            persist?: @pipeline_config.persist?,
            storage: %FileStorage{
              directory: tmp_dir
            }
          }
        )

      assert_pipeline_notified(pipeline, :sink_bin, :end_of_stream, 10_000)

      File.ls!(tmp_dir)
      |> Enum.filter(&String.match?(&1, ~r/.*(?<!index|delta)\.m3u8$/))
      |> Enum.each(fn manifest_filename ->
        manifest_file = File.read!(Path.join(tmp_dir, manifest_filename))

        target_duration =
          manifest_file
          |> then(&Regex.run(~r/#EXT-X-TARGETDURATION:(\d+)/, &1, capture: :all_but_first))
          |> hd()
          |> String.to_integer()

        {segments_in_manifest, segment_durations} =
          manifest_file
          |> then(&Regex.scan(~r/#EXTINF:(\d+\.\d+),\s\w+segment_\d+_.+.m4s/, &1))
          |> Enum.map(&List.to_tuple/1)
          |> Enum.unzip()

        # delta manifest will be generated when the sum of full (finished) segment durations
        # is greater than 6 * target duration
        # AND there is at least one segment that can be skipped
        segment_durations_sum =
          segment_durations
          |> Enum.drop(1)
          |> Enum.reduce(0, &(String.to_float(&1) + &2))

        # for the fixtures used in this test, the following condition will be true,
        # so delta must be generated
        assert segment_durations_sum > @min_number_of_segments_in_delta_playlist * target_duration

        delta_manifest_filename = String.replace_suffix(manifest_filename, ".m3u8", "_delta.m3u8")

        # check if manifest contains #CAN-SKIP-UNTIL tag
        can_skip_until =
          manifest_file
          |> then(&Regex.run(~r/CAN-SKIP-UNTIL=(\d+\.*\d*)/, &1, capture: :all_but_first))
          |> hd()
          |> String.to_float()

        # check if delta file exists
        assert File.exists?(Path.join(tmp_dir, delta_manifest_filename))

        delta_manifest_file = File.read!(Path.join(tmp_dir, delta_manifest_filename))

        {segments_in_delta_manifest, delta_durations} =
          delta_manifest_file
          |> then(&Regex.scan(~r/#EXTINF:(\d+\.\d+),\s\w+segment_\d+_.+.m4s/, &1))
          |> Enum.map(&List.to_tuple/1)
          |> Enum.unzip()

        # check if #CAN-SKIP-UNTIL tag has the correct value
        delta_durations_sum = Enum.reduce(delta_durations, 0, &(String.to_float(&1) + &2))
        assert_in_delta can_skip_until, delta_durations_sum, @eps

        number_of_segments_in_delta_manifest = Enum.count(segments_in_delta_manifest)

        # check if segments in delta manifest are the same as last segments from regular manifest
        assert Enum.take(segments_in_manifest, -number_of_segments_in_delta_manifest) ==
                 segments_in_delta_manifest

        # check if delta manifest contains #EXT-X-SKIP tag with correct value
        skipped_segments =
          delta_manifest_file
          |> then(&Regex.run(~r/EXT-X-SKIP:SKIPPED-SEGMENTS=(\d+)/, &1, capture: :all_but_first))
          |> hd()
          |> String.to_integer()

        assert skipped_segments + number_of_segments_in_delta_manifest ==
                 Enum.count(segments_in_manifest)
      end)
    end

    test "audio and video tracks with partial segments" do
      alias Membrane.HTTPAdaptiveStream.Storages.SendStorage

      hackney_sources =
        @audio_video_tracks_sources
        |> Enum.map(fn {path, encoding, profile, name} ->
          {%Membrane.Hackney.Source{location: path, hackney_opts: [follow_redirect: true]},
           encoding, profile, name}
        end)

      pipeline =
        Testing.Pipeline.start_link_supervised!(
          module: TestPipeline,
          custom_args: %{
            sources: hackney_sources,
            hls_mode: @pipeline_config.hls_mode,
            partial_segments: true,
            target_window_duration: @pipeline_config.target_window_duration,
            persist?: @pipeline_config.persist?,
            storage: %SendStorage{destination: self()}
          }
        )

      assert_pipeline_notified(pipeline, :sink_bin, :end_of_stream, 10_000)

      assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}, 1_000

      # the values below define a set of expected partial segments
      # that should be followed by regular segments containing all previous parts
      # (belonging to the current segment)
      expected_segments = [
        {:video, 0, 20},
        {:video, 1, 4},
        {:audio, 0, 4},
        {:audio, 1, 4},
        {:audio, 2, 4},
        {:audio, 3, 4},
        {:audio, 4, 0}
      ]

      for {type, segment_idx, parts} <- expected_segments do
        segment_name = "#{type}_segment_#{segment_idx}_#{type}_track.m4s"

        partial_segments =
          for i <- 0..parts do
            partial_name = String.replace_suffix(segment_name, ".m4s", "_#{i}_part.m4s")

            assert_receive {SendStorage, :store,
                            %{
                              name: ^segment_name,
                              type: :partial_segment,
                              contents: segment,
                              metadata: %{partial_name: ^partial_name}
                            }}

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

      preload_hints =
        expected_segments
        |> Enum.flat_map(fn {_type, _segment_idx, parts} = tuple ->
          for idx <- 0..parts, do: Tuple.append(tuple, idx)
        end)
        |> Enum.map(fn {type, segment_idx, parts, part_idx} ->
          {segment_idx, part_idx} = calculate_preload_sn(segment_idx, part_idx, parts)
          "#{type}_segment_#{segment_idx}_#{type}_track_#{part_idx}_part.m4s"
        end)

      expected_segments
      |> Enum.flat_map(fn {type, _segment_idx, parts} -> for _idx <- 0..parts, do: type end)
      |> Enum.reduce(preload_hints, fn type, preload_hints ->
        manifest_name = "#{type}_track.m3u8"

        assert_receive {SendStorage, :store,
                        %{type: :manifest, contents: contents, name: ^manifest_name}}

        preload_hint = extract_preload_hint(contents)

        assert preload_hint in preload_hints

        List.delete(preload_hints, preload_hint)
      end)

      :ok = Testing.Pipeline.terminate(pipeline)

      # last manifests after EoS is received by the sink
      assert_receive {SendStorage, :store, %{type: :manifest, name: "video_track.m3u8"}}, 500
      assert_receive {SendStorage, :store, %{type: :manifest, name: "audio_track.m3u8"}}, 500
    end
  end

  test "audio and video tracks with removing pad" do
    alias Membrane.HTTPAdaptiveStream.Storages.SendStorage

    hackney_sources =
      @audio_video_tracks_sources
      |> Enum.map(fn {path, encoding, profile, name} ->
        {%Membrane.Hackney.Source{location: path, hackney_opts: [follow_redirect: true]},
         encoding, profile, name}
      end)

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        module: TestPipeline,
        custom_args: %{
          sources: hackney_sources,
          hls_mode: @pipeline_config.hls_mode,
          partial_segments: true,
          target_window_duration: @pipeline_config.target_window_duration,
          persist?: @pipeline_config.persist?,
          storage: %SendStorage{destination: self()}
        }
      )

    audio_track = "audio_track"

    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}, 2_000
    assert_receive {SendStorage, :store, %{type: :manifest, name: "video_track.m3u8"}}, 1_000
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio_track.m3u8"}}, 1_000

    :ok =
      Testing.Pipeline.execute_actions(pipeline,
        remove_children: [{:source, audio_track}, {:parser, audio_track}]
      )

    assert_pipeline_notified(pipeline, :sink_bin, :end_of_stream, 10_000)

    :ok = Testing.Pipeline.terminate(pipeline)

    # last manifests after EoS is received by the sink
    assert_receive {SendStorage, :remove, %{type: :manifest, name: "index.m3u8"}}, 10_000
    assert_receive {SendStorage, :remove, %{type: :manifest, name: "video_track.m3u8"}}, 10_000
    assert_receive {SendStorage, :remove, %{type: :manifest, name: "audio_track.m3u8"}}, 10_000
  end

  defp run_pipeline(sources, result_directory, %{
         hls_mode: hls_mode,
         persist?: persist?,
         target_window_duration: target_window_duration
       }) do
    alias Membrane.HTTPAdaptiveStream.Storages.FileStorage

    pipeline =
      [
        module: TestPipeline,
        custom_args: %{
          sources: sources,
          hls_mode: hls_mode,
          target_window_duration: target_window_duration,
          partial_segments: false,
          persist?: persist?,
          storage: %FileStorage{
            directory: result_directory
          }
        }
      ]
      |> Testing.Pipeline.start_link_supervised!()

    assert_pipeline_notified(pipeline, :sink_bin, :end_of_stream, 10_000)

    :ok = Testing.Pipeline.terminate(pipeline)
  end

  defp test_pipeline(
         sources,
         reference_directory,
         test_directory,
         pipeline_config \\ @pipeline_config
       ) do
    hackney_sources =
      sources
      |> Enum.map(fn {path, encoding, profile, name} ->
        case path do
          "http" <> _rest ->
            {%Membrane.Hackney.Source{location: path, hackney_opts: [follow_redirect: true]},
             encoding, profile, name}

          _path ->
            {%Membrane.File.Source{location: path}, encoding, profile, name}
        end
      end)

    if @create_fixtures do
      File.rm_rf(reference_directory)
      File.mkdir(reference_directory)
      run_pipeline(hackney_sources, reference_directory, pipeline_config)
    else
      run_pipeline(hackney_sources, test_directory, pipeline_config)

      {:ok, reference_playlist_content} = File.ls(reference_directory)

      for file_name <- reference_playlist_content do
        test_file_path = Path.join(test_directory, file_name)
        reference_file_path = Path.join(reference_directory, file_name)

        assert(
          File.read!(reference_file_path) == File.read!(test_file_path),
          "Contents of file #{Path.join(reference_directory, file_name)} differ from contents of file #{Path.join(test_directory, file_name)}"
        )
      end
    end
  end

  defp calculate_preload_sn(segment_idx, part_idx, parts) do
    if part_idx == parts do
      {segment_idx + 1, 0}
    else
      {segment_idx, part_idx + 1}
    end
  end

  defp extract_preload_hint(contents) do
    contents
    |> then(
      &Regex.run(~r/#EXT-X-PRELOAD-HINT:TYPE=PART,URI="(.*)"$/, &1, capture: :all_but_first)
    )
    |> hd()
  end
end
