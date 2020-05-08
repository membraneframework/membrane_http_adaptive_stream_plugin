defmodule Membrane.HTTPAdaptiveStream.SinkTest do
  use ExUnit.Case, async: true
  import Membrane.Testing.Assertions
  require Membrane.Pad
  alias Membrane.{Buffer, Pad, Testing, Time}
  alias Membrane.HTTPAdaptiveStream.Sink
  alias Membrane.HTTPAdaptiveStream.Storages.SendStorage

  defmodule Source do
    use Membrane.Source
    alias Membrane.CMAF

    def_output_pad :output, caps: CMAF, mode: :push

    def_options content_type: [spec: :audio | :video]

    @impl true
    def handle_prepared_to_playing(_ctx, state) do
      caps = %CMAF{content_type: state.content_type, init: "test_init"}
      {{:ok, caps: {:output, caps}}, state}
    end

    @impl true
    def handle_other(buffer, _ctx, state) do
      {{:ok, buffer: {:output, buffer}}, state}
    end
  end

  test "single track" do
    pipeline = mk_pipeline([:audio])
    assert_receive {SendStorage, :store, %{type: :init}}

    send_buf(pipeline, :audio, 2)
    assert_receive {SendStorage, :store, %{type: :playlist}}
    assert_receive {SendStorage, :store, %{name: "audio_fragment_0_" <> _}}
    assert_pipeline_notified(pipeline, :sink, {:track_playable, :audio})

    send_buf(pipeline, :audio, 4)
    assert_receive {SendStorage, :store, %{type: :playlist}}
    assert_receive {SendStorage, :store, %{name: "audio_fragment_1_" <> _}}

    send_buf(pipeline, :audio, 2)
    assert_receive {SendStorage, :store, %{type: :playlist}}
    assert_receive {SendStorage, :store, %{name: "audio_fragment_2_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_fragment_0_" <> _}}
    refute_receive {SendStorage, _, _}

    :ok = Testing.Pipeline.stop(pipeline)
    assert_pipeline_playback_changed(pipeline, _, :stopped)
    assert_pipeline_notified(pipeline, :sink, {:cleanup, cleanup_fun})
    assert :ok = cleanup_fun.()
    assert_receive {SendStorage, :remove, %{type: :playlist, name: "index" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_fragment_1_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_fragment_2_" <> _}}
    refute_receive {SendStorage, _, _}
  end

  test "multi track" do
    pipeline = mk_pipeline([:audio, :video])
    assert_receive {SendStorage, :store, %{type: :init, name: "audio_init" <> _}}
    assert_receive {SendStorage, :store, %{type: :init, name: "video_init" <> _}}

    send_buf(pipeline, :audio, 2)
    assert_receive {SendStorage, :store, %{type: :playlist, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :playlist, name: "audio.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :playlist, name: "video.m3u8"}}
    assert_receive {SendStorage, :store, %{name: "audio_fragment_0_" <> _}}
    assert_pipeline_notified(pipeline, :sink, {:track_playable, :audio})

    send_buf(pipeline, :video, 3)
    assert_receive {SendStorage, :store, %{type: :playlist, name: "video.m3u8"}}
    assert_receive {SendStorage, :store, %{name: "video_fragment_0_" <> _}}
    assert_pipeline_notified(pipeline, :sink, {:track_playable, :video})

    send_buf(pipeline, :audio, 4)
    assert_receive {SendStorage, :store, %{type: :playlist, name: "audio.m3u8"}}
    assert_receive {SendStorage, :store, %{name: "audio_fragment_1_" <> _}}

    send_buf(pipeline, :video, 5)
    assert_receive {SendStorage, :store, %{type: :playlist, name: "video.m3u8"}}
    assert_receive {SendStorage, :store, %{name: "video_fragment_1_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "video_fragment_0_" <> _}}

    send_buf(pipeline, :audio, 2)
    assert_receive {SendStorage, :store, %{type: :playlist, name: "audio.m3u8"}}
    assert_receive {SendStorage, :store, %{name: "audio_fragment_2_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_fragment_0_" <> _}}
    refute_receive {SendStorage, _, _}

    :ok = Testing.Pipeline.stop(pipeline)
    assert_pipeline_playback_changed(pipeline, _, :stopped)
    assert_pipeline_notified(pipeline, :sink, {:cleanup, cleanup_fun})
    assert :ok = cleanup_fun.()
    assert_receive {SendStorage, :remove, %{type: :playlist, name: "index" <> _}}
    assert_receive {SendStorage, :remove, %{type: :playlist, name: "audio" <> _}}
    assert_receive {SendStorage, :remove, %{type: :playlist, name: "video" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_fragment_1_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_fragment_2_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "video_fragment_1_" <> _}}
    refute_receive {SendStorage, _, _}
  end

  defp mk_pipeline(sources) do
    import Membrane.ParentSpec
    sources = Enum.map(sources, &{{:source, &1}, %Source{content_type: &1}})

    children =
      [
        sink: %Sink{
          playlist_module: Membrane.HTTPAdaptiveStream.HLS.Playlist,
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
