defmodule Membrane.HLS.Source.Test do
  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  alias Membrane.Testing

  @mpegts_url "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8"
  @fmp4_url "https://raw.githubusercontent.com/membraneframework-labs/ex_hls/refs/heads/plug-demuxing-engine-into-client/fixture/output.m3u8"

  @hls_source_fixtures "test/membrane_http_adaptive_stream/integration_test/fixtures/hls_source"
  @fmp4_video_fixture Path.join(@hls_source_fixtures, "fmp4/video.h264")
  @fmp4_audio_fixture Path.join(@hls_source_fixtures, "fmp4/audio.aac")
  @mpeg_ts_video_fixture Path.join(@hls_source_fixtures, "mpeg_ts/video.h264")
  @mpeg_ts_audio_fixture Path.join(@hls_source_fixtures, "mpeg_ts/audio.aac")

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
      assert_track(audio_result_file, @fmp4_audio_fixture, 70_000)
      assert_track(video_result_file, @fmp4_video_fixture, 200_000)
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
      assert_track(audio_result_file, @mpeg_ts_audio_fixture, 40_000)
      assert_track(video_result_file, @mpeg_ts_video_fixture, 70_000)
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
