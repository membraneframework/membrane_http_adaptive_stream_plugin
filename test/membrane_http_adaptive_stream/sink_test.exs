defmodule Membrane.HTTPAdaptiveStream.SinkTest do
  use ExUnit.Case, async: true
  import Membrane.Testing.Assertions
  alias Membrane.{Buffer, Pad, Testing, Time}
  alias Membrane.HTTPAdaptiveStream.Sink
  alias Membrane.HTTPAdaptiveStream.Storages.SendStorage
  require Membrane.Pad

  defmodule Source do
    use Membrane.Source
    alias Membrane.CMAF.Track

    def_output_pad :output, caps: Track, mode: :push

    def_options content_type: [spec: :audio | :video]

    @impl true
    def handle_prepared_to_playing(_ctx, state) do
      caps = %Track{content_type: state.content_type, header: "test_header"}
      {{:ok, caps: {:output, caps}}, state}
    end

    @impl true
    def handle_other(buffer, _ctx, state) do
      {{:ok, buffer: {:output, buffer}}, state}
    end
  end

  test "single track" do
    pipeline = mk_pipeline([:audio])
    assert_receive {SendStorage, :store, %{type: :header}}

    send_buf(pipeline, :audio, 2)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio.m3u8"}}
    assert_receive {SendStorage, :store, %{name: "audio_segment_0_" <> _}}
    assert_pipeline_notified(pipeline, :sink, {:track_playable, :audio})

    send_buf(pipeline, :audio, 4)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio.m3u8"}}
    assert_receive {SendStorage, :store, %{name: "audio_segment_1_" <> _}}

    send_buf(pipeline, :audio, 2)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio.m3u8"}}
    assert_receive {SendStorage, :store, %{name: "audio_segment_2_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_segment_0_" <> _}}
    refute_receive {SendStorage, _, _}

    :ok = Testing.Pipeline.stop(pipeline)
    assert_pipeline_playback_changed(pipeline, _, :stopped)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio.m3u8"}}
    assert_pipeline_notified(pipeline, :sink, {:cleanup, cleanup_fun})
    assert :ok = cleanup_fun.()
    assert_receive {SendStorage, :remove, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :remove, %{type: :manifest, name: "audio.m3u8"}}
    assert_receive {SendStorage, :remove, %{name: "audio_segment_1_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_segment_2_" <> _}}
    refute_receive {SendStorage, _, _}
  end

  test "multi track" do
    pipeline = mk_pipeline([:audio, :video])
    assert_receive {SendStorage, :store, %{type: :header, name: "audio_header" <> _}}
    assert_receive {SendStorage, :store, %{type: :header, name: "video_header" <> _}}

    send_buf(pipeline, :audio, 2)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "video" <> _}}
    assert_receive {SendStorage, :store, %{name: "audio_segment_0_" <> _}}
    assert_pipeline_notified(pipeline, :sink, {:track_playable, :audio})

    send_buf(pipeline, :video, 3)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "video" <> _}}
    assert_receive {SendStorage, :store, %{name: "video_segment_0_" <> _}}
    assert_pipeline_notified(pipeline, :sink, {:track_playable, :video})

    send_buf(pipeline, :audio, 4)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio.m3u8"}}
    assert_receive {SendStorage, :store, %{name: "audio_segment_1_" <> _}}

    send_buf(pipeline, :video, 5)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "video" <> _}}
    assert_receive {SendStorage, :store, %{name: "video_segment_1_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "video_segment_0_" <> _}}

    send_buf(pipeline, :audio, 2)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio.m3u8"}}
    assert_receive {SendStorage, :store, %{name: "audio_segment_2_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_segment_0_" <> _}}
    refute_receive {SendStorage, _, _}

    :ok = Testing.Pipeline.stop(pipeline)
    assert_pipeline_playback_changed(pipeline, _, :stopped)
    assert_receive {SendStorage, :store, %{type: :manifest, name: "audio.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: "video" <> _}}
    # Cache will be cleared on first track removal, thus index and that track manifest
    # will be stored again upon second track removal.
    assert_receive {SendStorage, :store, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :manifest, name: _}}
    assert_pipeline_notified(pipeline, :sink, {:cleanup, cleanup_fun})
    assert :ok = cleanup_fun.()
    assert_receive {SendStorage, :remove, %{type: :manifest, name: "index.m3u8"}}
    assert_receive {SendStorage, :remove, %{type: :manifest, name: "audio.m3u8"}}
    assert_receive {SendStorage, :remove, %{type: :manifest, name: "video" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_segment_1_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_segment_2_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "video_segment_1_" <> _}}
    refute_receive {SendStorage, _, _}
  end

  defp mk_pipeline(sources) do
    import Membrane.ParentSpec
    sources = Enum.map(sources, &{{:source, &1}, %Source{content_type: &1}})

    children =
      [
        sink: %Sink{
          manifest_module: Membrane.HTTPAdaptiveStream.HLS,
          storage: %SendStorage{destination: self()},
          target_window_duration: Time.seconds(5)
        }
      ] ++ sources

    links =
      Enum.map(sources, fn {{:source, type}, _config} ->
        link({:source, type}) |> via_in(Pad.ref(:input, type)) |> to(:sink)
      end)

    assert {:ok, pipeline} =
             Testing.Pipeline.start_link(%Testing.Pipeline.Options{
               elements: children,
               links: links
             })

    :ok = Testing.Pipeline.play(pipeline)
    assert_pipeline_playback_changed(pipeline, _, :playing)
    pipeline
  end

  defp send_buf(pipeline, content_type, duration) do
    buffer = %Buffer{payload: "test_payload", metadata: %{duration: Time.seconds(duration)}}
    Testing.Pipeline.message_child(pipeline, {:source, content_type}, buffer)
  end
end
