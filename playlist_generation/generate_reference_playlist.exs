alias Membrane.HTTPAdaptiveStream.ReferencePlaylistGenerator

pipeline = ReferencePlaylistGenerator.start_link([output_dir: "reference_playlist"])
  |> elem(1)
  |> tap(&Membrane.Pipeline.play/1)
  |> then(&Process.monitor/1)

receive do
  {:DOWN, ^pipeline, :process, _pid, _reason} ->
    :ok
end
