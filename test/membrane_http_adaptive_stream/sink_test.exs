defmodule Membrane.HTTPAdaptiveStream.SinkTest do
  use ExUnit.Case, async: true
  require Membrane.Pad
  alias Membrane.{Buffer, Pad, Time}
  alias Membrane.HTTPAdaptiveStream.Sink
  alias Membrane.HTTPAdaptiveStream.Storages.SendStorage

  test "single track" do
    assert {:ok, state} = init()
    assert {:ok, state} = add_track(:audio, state)
    assert_receive {SendStorage, :store, %{type: :init}}

    assert {{:ok, _actions}, state} =
             Sink.handle_write(Pad.ref(:input, :audio), gen_buf(2), nil, state)

    assert_receive {SendStorage, :store, %{type: :playlist}}
    assert_receive {SendStorage, :store, %{name: "audio_fragment_0_" <> _}}

    assert {{:ok, _actions}, state} =
             Sink.handle_write(Pad.ref(:input, :audio), gen_buf(4), nil, state)

    assert_receive {SendStorage, :store, %{type: :playlist}}
    assert_receive {SendStorage, :store, %{name: "audio_fragment_1_" <> _}}

    assert {{:ok, _actions}, state} =
             Sink.handle_write(Pad.ref(:input, :audio), gen_buf(2), nil, state)

    assert_receive {SendStorage, :store, %{type: :playlist}}
    assert_receive {SendStorage, :store, %{name: "audio_fragment_2_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_fragment_0_" <> _}}
    refute_receive {SendStorage, _, _}

    assert {{:ok, actions}, state} = Sink.handle_playing_to_prepared(nil, state)
    assert {:ok, {:cleanup, cleanup_fun}} = Keyword.fetch(actions, :notify)
    assert :ok = cleanup_fun.()
    assert_receive {SendStorage, :remove, %{type: :playlist, name: "index" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_fragment_1_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_fragment_2_" <> _}}
    refute_receive {SendStorage, _, _}
  end

  test "multi track" do
    assert {:ok, state} = init()
    assert {:ok, state} = add_track(:audio, state)
    assert_receive {SendStorage, :store, %{type: :init, name: "audio_init" <> _}}
    assert {:ok, state} = add_track(:video, state)
    assert_receive {SendStorage, :store, %{type: :init, name: "video_init" <> _}}

    assert {{:ok, _actions}, state} =
             Sink.handle_write(Pad.ref(:input, :audio), gen_buf(2), nil, state)

    assert_receive {SendStorage, :store, %{type: :playlist, name: "index.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :playlist, name: "audio.m3u8"}}
    assert_receive {SendStorage, :store, %{type: :playlist, name: "video.m3u8"}}
    assert_receive {SendStorage, :store, %{name: "audio_fragment_0_" <> _}}

    assert {{:ok, _actions}, state} =
             Sink.handle_write(Pad.ref(:input, :video), gen_buf(3), nil, state)

    assert_receive {SendStorage, :store, %{type: :playlist, name: "video.m3u8"}}
    assert_receive {SendStorage, :store, %{name: "video_fragment_0_" <> _}}

    assert {{:ok, _actions}, state} =
             Sink.handle_write(Pad.ref(:input, :audio), gen_buf(4), nil, state)

    assert_receive {SendStorage, :store, %{type: :playlist, name: "audio.m3u8"}}
    assert_receive {SendStorage, :store, %{name: "audio_fragment_1_" <> _}}

    assert {{:ok, _actions}, state} =
             Sink.handle_write(Pad.ref(:input, :video), gen_buf(5), nil, state)

    assert_receive {SendStorage, :store, %{type: :playlist, name: "video.m3u8"}}
    assert_receive {SendStorage, :store, %{name: "video_fragment_1_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "video_fragment_0_" <> _}}

    assert {{:ok, _actions}, state} =
             Sink.handle_write(Pad.ref(:input, :audio), gen_buf(2), nil, state)

    assert_receive {SendStorage, :store, %{type: :playlist, name: "audio.m3u8"}}
    assert_receive {SendStorage, :store, %{name: "audio_fragment_2_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_fragment_0_" <> _}}
    refute_receive {SendStorage, _, _}

    assert {{:ok, actions}, state} = Sink.handle_playing_to_prepared(nil, state)
    assert {:ok, {:cleanup, cleanup_fun}} = Keyword.fetch(actions, :notify)
    assert :ok = cleanup_fun.()
    assert_receive {SendStorage, :remove, %{type: :playlist, name: "index" <> _}}
    assert_receive {SendStorage, :remove, %{type: :playlist, name: "audio" <> _}}
    assert_receive {SendStorage, :remove, %{type: :playlist, name: "video" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_fragment_1_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "audio_fragment_2_" <> _}}
    assert_receive {SendStorage, :remove, %{name: "video_fragment_1_" <> _}}
    refute_receive {SendStorage, _, _}
  end

  defp init() do
    Sink.handle_init(%Sink{
      playlist_module: Membrane.HTTPAdaptiveStream.HLS.Playlist,
      storage: %SendStorage{destination: self()},
      target_window_duration: Time.seconds(5)
    })
  end

  defp add_track(content_type, state) do
    Sink.handle_caps(
      Pad.ref(:input, content_type),
      %Membrane.CMAF{
        content_type: content_type,
        init: "test_init"
      },
      nil,
      state
    )
  end

  defp gen_buf(duration) do
    %Buffer{payload: "test_payload", metadata: %{duration: Time.seconds(duration)}}
  end
end
