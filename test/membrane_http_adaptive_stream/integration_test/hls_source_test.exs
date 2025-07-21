defmodule Membrane.HLS.Source.Test do
  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.{AAC, H264, RemoteStream}

  alias Membrane.Testing

  @mpegts_url "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8"
  @fmp4_url "https://raw.githubusercontent.com/membraneframework-labs/ex_hls/refs/heads/plug-demuxing-engine-into-client/fixture/output.m3u8"

  @ref_files_dir "test/membrane_http_adaptive_stream/integration_test/fixtures/hls_source"
  @fmp4_video_ref_file Path.join(@ref_files_dir, "fmp4/video.h264")
  @fmp4_audio_ref_file Path.join(@ref_files_dir, "fmp4/audio.aac")
  @mpeg_ts_video_ref_file Path.join(@ref_files_dir, "mpeg_ts/video.h264")
  @mpeg_ts_audio_ref_file Path.join(@ref_files_dir, "mpeg_ts/audio.aac")

  describe "Membrane.HLS.Source demuxes audio and video from HLS stream" do
    @tag :tmp_dir
    test "(fMP4)", %{tmp_dir: tmp_dir} do
      audio_result_file = Path.join(tmp_dir, "audio.aac")
      video_result_file = Path.join(tmp_dir, "video.h264")

      spec =
        pipeline_spec(
          @fmp4_url,
          %Membrane.Transcoder{output_stream_format: Membrane.AAC},
          audio_result_file,
          video_result_file
        )

      pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)
      Process.sleep(10_000)
      Testing.Pipeline.terminate(pipeline)

      # fixtures created locally with a quite good internet connection have
      #  - 139_085 bytes for audio
      #  - 500_571 bytes for video
      assert_track(audio_result_file, @fmp4_audio_ref_file, 70_000)
      assert_track(video_result_file, @fmp4_video_ref_file, 200_000)
    end

    @tag :tmp_dir
    test "(MPEG-TS)", %{tmp_dir: tmp_dir} do
      audio_result_file = Path.join(tmp_dir, "audio.aac")
      video_result_file = Path.join(tmp_dir, "video.h264")

      spec =
        pipeline_spec(
          @mpegts_url,
          %Membrane.Transcoder{
            assumed_input_stream_format: %Membrane.AAC{
              encapsulation: :ADTS
            },
            output_stream_format: Membrane.AAC
          },
          audio_result_file,
          video_result_file
        )

      pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)
      Process.sleep(10_000)
      Testing.Pipeline.terminate(pipeline)

      # fixtures created locally with a quite good internet connection have
      #  - 78_732 bytes for audio
      #  - 136_754 bytes for video
      assert_track(audio_result_file, @mpeg_ts_audio_ref_file, 40_000)
      assert_track(video_result_file, @mpeg_ts_video_ref_file, 70_000)
    end
  end

  defmacrop test_new_tracks_notification(hls_url, video_format_pattern, audio_format_pattern) do
    quote do
      source_spec =
        child(:hls_source, %Membrane.HLS.Source{
          url: unquote(hls_url),
          variant_selection_policy: :lowest_resolution
        })

      pipeline = Testing.Pipeline.start_link_supervised!(spec: source_spec)

      # let's assert :new_tracks notification

      assert_pipeline_notified(pipeline, :hls_source, {:new_tracks, new_tracks})

      dbg(new_tracks)

      assert length(new_tracks) == 2
      assert unquote(video_format_pattern) = new_tracks[:video_output]
      assert unquote(audio_format_pattern) = new_tracks[:audio_output]

      # let's assert stream formats going via pads

      linking_spec = [
        get_child(:hls_source)
        |> via_out(:video_output)
        |> child(:video_sink, %Testing.Sink{}),
        get_child(:hls_source)
        |> via_out(:audio_output)
        |> child(:audio_sink, %Testing.Sink{})
      ]

      Testing.Pipeline.execute_actions(pipeline, spec: linking_spec)

      assert_sink_stream_format(pipeline, :video_sink, unquote(video_format_pattern))
      assert_sink_stream_format(pipeline, :audio_sink, unquote(audio_format_pattern))

      Testing.Pipeline.terminate(pipeline)
    end
  end

  describe "Membrane.HLS.Source sends :new_tracks notification" do
    @tag :a
    test "(fMP4)" do
      test_new_tracks_notification(
        @fmp4_url,
        %H264{
          width: 480,
          height: 270,
          alignment: :au,
          nalu_in_metadata?: false,
          stream_structure:
            {:avc1,
             <<1, 100, 0, 21, 255, 225, 0, 28, 103, 100, 0, 21, 172, 217, 65, 224>> <> _rest}
        },
        %AAC{
          sample_rate: 44100,
          channels: 2,
          mpeg_version: 2,
          samples_per_frame: 1024,
          frames_per_buffer: 1,
          encapsulation: :none,
          config:
            {:esds,
             <<3, 128, 128, 128, 37, 0, 2, 0, 4, 128, 128, 128, 23, 64, 21, 0, 0, 0>> <> _rest}
        }
      )
    end

    test "(MPEG-TS)" do
      test_new_tracks_notification(
        @mpegts_url,
        %RemoteStream{content_format: H264, type: :bytestream},
        %RemoteStream{content_format: AAC, type: :bytestream}
      )
    end
  end

  defp pipeline_spec(url, audio_transcoder, audio_result_file, video_result_file) do
    [
      child(:hls_source, %Membrane.HLS.Source{
        url: url,
        variant_selection_policy: :lowest_resolution
      })
      |> via_out(:video_output)
      |> child(%Membrane.Transcoder{
        output_stream_format: Membrane.H264
      })
      |> child(Membrane.Realtimer)
      |> child(%Membrane.File.Sink{
        location: video_result_file
      }),
      get_child(:hls_source)
      |> via_out(:audio_output)
      |> child(audio_transcoder)
      |> child(Membrane.Realtimer)
      |> child(%Membrane.File.Sink{
        location: audio_result_file
      })
    ]
  end

  defp assert_track(result_file, fixture, asserted_bytes) do
    <<expected_prefix::binary-size(asserted_bytes), _sufix::binary>> =
      fixture |> File.read!()

    assert result_file
           |> File.read!()
           |> String.starts_with?(expected_prefix)
  end
end
