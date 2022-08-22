defmodule Membrane.HTTPAdaptiveStream.Manifest do
  @moduledoc """
  Behaviour for manifest serialization.
  """
  use Bunch.Access

  alias __MODULE__.SegmentAttribute
  alias __MODULE__.Track

  @type serialized_manifest_t :: {manifest_name :: String.t(), manifest_content :: String.t()}

  @type serialized_manifests_t :: %{
          main_manifest: serialized_manifest_t(),
          manifest_per_track: %{
            optional(track_id :: any()) => serialized_manifest_t()
          }
        }

  @callback serialize(t) :: serialized_manifests_t()

  @type t :: %__MODULE__{
          name: String.t(),
          module: module,
          tracks: %{(id :: any) => Track.t()}
        }

  @enforce_keys [:name, :module]
  defstruct @enforce_keys ++ [tracks: %{}]

  @doc """
  Adds a track to the manifest.

  Returns the name under which the header file should be stored.
  """
  @spec add_track(t, Track.Config.t()) :: {header_name :: String.t(), t}
  def add_track(manifest, %Track.Config{} = config) do
    track = Track.new(config)
    manifest = %__MODULE__{manifest | tracks: Map.put(manifest.tracks, config.id, track)}
    {track.header_name, manifest}
  end

  @spec add_segment(
          t,
          track_id :: Track.id_t(),
          Track.segment_duration_t(),
          Track.segment_byte_size_t(),
          list(SegmentAttribute.t())
        ) ::
          {Track.Changeset.t(), t}
  def add_segment(%__MODULE__{} = manifest, track_id, duration, byte_size, attributes \\ []) do
    get_and_update_in(
      manifest,
      [:tracks, track_id],
      &Track.add_segment(&1, duration, byte_size, attributes)
    )
  end

  @doc """
  Finalizes last segment of given track when serving partial segments.
  """
  @spec finalize_last_segment(t(), Track.id_t()) :: {Track.Changeset.t(), t()}
  def finalize_last_segment(manifest, track_id) do
    get_and_update_in(manifest, [:tracks, track_id], &Track.finalize_last_segment/1)
  end

  @spec add_partial_segment(
          t(),
          Track.id_t(),
          boolean(),
          Track.segment_duration_t(),
          list(SegmentAttribute.t())
        ) ::
          {Track.Changeset.t(), t}
  def add_partial_segment(manifest, track_id, independent?, duration, attributes \\ []) do
    get_and_update_in(
      manifest,
      [:tracks, track_id],
      &Track.add_partial_segment(&1, independent?, duration, attributes)
    )
  end

  @spec serialize(t) :: serialized_manifests_t()
  def serialize(%__MODULE__{module: module} = manifest) do
    module.serialize(manifest)
  end

  @spec has_track?(t(), Track.id_t()) :: boolean()
  def has_track?(%__MODULE__{tracks: tracks}, track_id), do: Map.has_key?(tracks, track_id)

  @doc """
  Append a discontinuity to the track.

  This will inform the player that eg. the parameters of the encoder changed and allow you to provide a new MP4 header.
  For details on discontinuities refer to [RFC 8216](https://datatracker.ietf.org/doc/html/rfc8216).
  """
  @spec discontinue_track(t(), Track.id_t()) :: {header_name :: String.t(), t()}
  def discontinue_track(%__MODULE__{} = manifest, track_id) do
    get_and_update_in(
      manifest,
      [:tracks, track_id],
      &Track.discontinue/1
    )
  end

  @spec finish(t, Track.id_t()) :: t
  def finish(%__MODULE__{} = manifest, track_id) do
    update_in(manifest, [:tracks, track_id], &Track.finish/1)
  end

  @doc """
  Restores all the stale segments in all tracks.

  All the tracks must be configured to be persisted beforehand, otherwise this function will raise
  """
  @spec from_beginning(t()) :: t
  def from_beginning(%__MODULE__{} = manifest) do
    tracks = Bunch.Map.map_values(manifest.tracks, &Track.from_beginning/1)
    %__MODULE__{manifest | tracks: tracks}
  end

  @doc """
  Returns stale and current segments' names from all tracks
  """
  @spec all_segments(t) :: [segment_name :: String.t()]
  def all_segments(%__MODULE__{} = manifest) do
    # here we should just group by track instead of flat mapping
    manifest.tracks |> Map.values() |> Enum.flat_map(&Track.all_segments/1)
  end

  @spec all_segments_per_track(t()) :: %{
          optional(track_id :: term()) => [segment_name :: String.t()]
        }
  def all_segments_per_track(%__MODULE__{} = manifest) do
    Map.new(manifest.tracks, fn {track_id, track} -> {track_id, Track.all_segments(track)} end)
  end
end
