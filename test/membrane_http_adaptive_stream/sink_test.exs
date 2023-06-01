defmodule Membrane.HTTPAdaptiveStream.SinkTest do
  use ExUnit.Case, async: true

  import Membrane.Testing.Assertions

  require Membrane.Pad

  alias Membrane.{Buffer, Pad, Testing, Time}
  alias Membrane.HTTPAdaptiveStream.Manifest.{SegmentDuration, Track}
  alias Membrane.HTTPAdaptiveStream.Sink
  alias Membrane.HTTPAdaptiveStream.Storages.SendStorage

  defmodule Source do
    @moduledoc """
    Trival source to test audio and multiple video track recognition
    """

    use Membrane.Source
    alias Membrane.CMAF.Track

    def_output_pad :output, accepted_format: Track, mode: :push

    def_options content_type: [spec: :audio | :video], source_id: [spec: String.t()]

    @impl true
    def handle_playing(_ctx, state) do
      stream_format = %Track{content_type: state.content_type, header: "test_header"}
      {[stream_format: {:output, stream_format}], state}
    end

    @impl true
    def handle_parent_notification({:buffer, buffer}, _ctx, state) do
      {[buffer: {:output, buffer}], state}
    end
  end

  test "single track" do
    pipeline = mk_pipeline([{:audio, "audio_track"}])
    assert_receive {SendStorage, :store, %{type: :header}}

    send_buf(pipeline, "audio_track", 2)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio" <> _}}
    assert_receive {SendStorage, :store, %{name: "audio_segment_0_" <> _}}
    assert_pipeline_notified(pipeline, :sink, {:track_playable, "audio_track"})

    send_buf(pipeline, "audio_track", 4)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio" <> _}}
    assert_receive {SendStorage, :store, %{name: "audio_segment_1_" <> _}}

    send_buf(pipeline, "audio_track", 2)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio" <> _}}
    assert_receive {SendStorage, :store, %{name: "audio_segment_2_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_segment_0_" <> _}}
    refute_receive {SendStorage, _, _}

    :ok = Testing.Pipeline.terminate(pipeline, blocking?: true)

    assert_received {SendStorage, :store, %{type: :manifest, name: "audio" <> _}}
  end

  test "video and audio track" do
    pipeline = mk_pipeline([{:audio, "audio_track"}, {:video, "video_track"}])
    assert_receive {SendStorage, :store, %{type: :header, name: "audio_header" <> _}}
    assert_receive {SendStorage, :store, %{type: :header, name: "video_header" <> _}}

    send_buf(pipeline, "audio_track", 2)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio" <> _}}
    assert_receive {SendStorage, :store, %{name: "audio_segment_0_" <> _}}
    assert_pipeline_notified(pipeline, :sink, {:track_playable, "audio_track"})

    send_buf(pipeline, "video_track", 3)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "video_" <> _}}
    assert_receive {SendStorage, :store, %{name: "video_segment_0_" <> _}}
    assert_pipeline_notified(pipeline, :sink, {:track_playable, "video_track"})

    send_buf(pipeline, "audio_track", 4)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio" <> _}}
    assert_receive {SendStorage, :store, %{name: "audio_segment_1_" <> _}}

    send_buf(pipeline, "video_track", 5)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "video" <> _}}
    assert_receive {SendStorage, :store, %{name: "video_segment_1_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "video_segment_0_" <> _}}

    send_buf(pipeline, "audio_track", 2)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio" <> _}}
    assert_receive {SendStorage, :store, %{name: "audio_segment_2_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_segment_0_" <> _}}
    refute_receive {SendStorage, _, _}

    :ok = Testing.Pipeline.terminate(pipeline, blocking?: true)

    assert_received {SendStorage, :store, %{type: :manifest, name: "audio" <> _}}
    assert_received {SendStorage, :store, %{type: :manifest, name: "video" <> _}}
    # Cache will be cleared on first track removal, thus index and that track manifest
    # will be stored again upon second track removal.
    assert_received {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_received {SendStorage, :store, %{type: :manifest, name: _}}
    refute_received {SendStorage, _, _}
  end

  test "audio and multiple video tracks" do
    pipeline =
      mk_pipeline([
        {:video, "video_0"},
        {:audio, "audio"},
        {:video, "video_1"},
        {:video, "video_2"}
      ])

    assert_receive {SendStorage, :store, %{type: :header, name: "audio_header" <> _}}
    assert_receive {SendStorage, :store, %{type: :header, name: "video_header_" <> _}}
    assert_receive {SendStorage, :store, %{type: :header, name: "video_header_" <> _}}
    assert_receive {SendStorage, :store, %{type: :header, name: "video_header_" <> _}}
    refute_receive {SendStorage, _, _}

    send_buf(pipeline, "audio", 2)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio" <> _}}
    assert_receive {SendStorage, :store, %{name: "audio_segment_0_" <> _}}
    refute_receive {SendStorage, _, _}
    assert_pipeline_notified(pipeline, :sink, {:track_playable, "audio"})

    send_buf(pipeline, "video_1", 5)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "video_1" <> _}}
    assert_receive {SendStorage, :store, %{name: "video_segment_0_video_1" <> _}}
    refute_receive {SendStorage, _, _}
    assert_pipeline_notified(pipeline, :sink, {:track_playable, "video_1"})

    send_buf(pipeline, "audio", 4)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio" <> _}}
    assert_receive {SendStorage, :store, %{name: "audio_segment_1_" <> _}}
    refute_receive {SendStorage, _, _}

    send_buf(pipeline, "video_2", 5)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "video_2" <> _}}
    assert_receive {SendStorage, :store, %{name: "video_segment_0_video_2" <> _}}
    refute_receive {SendStorage, _, _}
    assert_pipeline_notified(pipeline, :sink, {:track_playable, "video_2"})

    send_buf(pipeline, "video_2", 6)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "video_2" <> _}}
    assert_receive {SendStorage, :store, %{name: "video_segment_1_video_2" <> _}}
    assert_receive {SendStorage, :remove, %{name: "video_segment_0_video_2" <> _}}
    refute_receive {SendStorage, _, _}

    send_buf(pipeline, "video_0", 2)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "video_0" <> _}}
    assert_receive {SendStorage, :store, %{name: "video_segment_0_video_0" <> _}}
    refute_receive {SendStorage, _, _}
    assert_pipeline_notified(pipeline, :sink, {:track_playable, "video_0"})

    send_buf(pipeline, "video_1", 6)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "video_1" <> _}}
    assert_receive {SendStorage, :store, %{name: "video_segment_1_video_1" <> _}}
    assert_receive {SendStorage, :remove, %{name: "video_segment_0_video_1" <> _}}
    refute_receive {SendStorage, _, _}

    :ok = Testing.Pipeline.terminate(pipeline, blocking?: true)
  end

  test "cleanup" do
    cleanup_after = Membrane.Time.seconds(1)
    pipeline = mk_pipeline([{:audio, "audio_track"}], cleanup_after: cleanup_after)
    assert_receive {SendStorage, :store, %{type: :header}}

    send_buf(pipeline, "audio_track", 2)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio" <> _}}
    assert_receive {SendStorage, :store, %{name: "audio_segment_0_" <> _}}
    assert_pipeline_notified(pipeline, :sink, {:track_playable, "audio_track"})

    :ok = Testing.Pipeline.terminate(pipeline)

    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio" <> _}}

    Process.sleep(Membrane.Time.as_milliseconds(cleanup_after))

    assert_receive {SendStorage, :remove, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :remove, %{type: :manifest, name: "audio" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_segment_0_" <> _}}
    refute_receive {SendStorage, _, _}
  end

  defp mk_pipeline(sources, opts \\ []) do
    import Membrane.ChildrenSpec

    sources =
      Enum.map(sources, fn {content_type, source_id} ->
        {{:source, source_id}, %Source{content_type: content_type, source_id: source_id}}
      end)

    segment_duration = SegmentDuration.new(Time.seconds(5))

    manifest_config = %Sink.ManifestConfig{module: Membrane.HTTPAdaptiveStream.HLS}
    track_config = %Sink.TrackConfig{target_window_duration: Time.seconds(5), mode: :vod}

    structure =
      [
        child(:sink, %Sink{
          manifest_config: manifest_config,
          track_config: track_config,
          storage: %SendStorage{destination: self()},
          cleanup_after: opts[:cleanup_after]
        })
      ] ++
        Enum.map(sources, fn {{:source, source_id}, config} ->
          child({:source, source_id}, config)
          |> via_in(Pad.ref(:input, source_id),
            options: [track_name: source_id, segment_duration: segment_duration]
          )
          |> get_child(:sink)
        end)

    pipeline = Testing.Pipeline.start_link_supervised!(spec: structure)

    assert_pipeline_play(pipeline)

    pipeline
  end

  defp send_buf(pipeline, source_id, duration) do
    buffer = %Buffer{
      payload: "test_payload",
      metadata: %{duration: Time.seconds(duration), independent?: true}
    }

    Testing.Pipeline.message_child(pipeline, {:source, source_id}, {:buffer, buffer})
  end
end
