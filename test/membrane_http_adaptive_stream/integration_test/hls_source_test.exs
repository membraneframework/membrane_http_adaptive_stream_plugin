defmodule Membrane.HLS.Source.Test do
  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.Testing

  @mpegts_url "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8"
  @fmp4_url "https://raw.githubusercontent.com/membraneframework-labs/ex_hls/refs/heads/plug-demuxing-engine-into-client/fixture/output.m3u8"

  @tag :a
  test "fmp4" do
    spec = [
      child(:hls_source, %Membrane.HLS.Source{url: @fmp4_url})
      |> via_out(:video_output)
      |> child(%Membrane.Transcoder{output_stream_format: Membrane.RawVideo})
      |> child(Membrane.SDL.Player),
      get_child(:hls_source)
      |> via_out(:audio_output)
      |> child(%Membrane.Transcoder{output_stream_format: Membrane.RawAudio})
      |> child(Membrane.PortAudio.Sink)
    ]

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    Process.sleep(10_000)
    Testing.Pipeline.terminate(pipeline)
  end

  @tag :b
  test "mpeg-ts" do
    spec = [
      child(:hls_source, %Membrane.HLS.Source{url: @mpegts_url})
      |> via_out(:video_output)
      |> child(%Membrane.Transcoder{
        output_stream_format: Membrane.RawVideo
      })
      |> child(Membrane.Realtimer)
      |> child(Membrane.SDL.Player),
      get_child(:hls_source)
      |> via_out(:audio_output)
      |> child(%Membrane.Transcoder{
        assumed_input_stream_format: %Membrane.AAC{
          encapsulation: :ADTS
        },
        output_stream_format: Membrane.RawAudio
      })
      |> child(Membrane.Realtimer)
      |> child(Membrane.PortAudio.Sink)
    ]

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    Process.sleep(10_000)
    Testing.Pipeline.terminate(pipeline)
  end
end
