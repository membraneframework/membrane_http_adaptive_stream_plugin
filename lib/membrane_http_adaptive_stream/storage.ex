defmodule Membrane.HTTPAdaptiveStream.Storage do
  @moduledoc """
  Behaviour for storing manifests and stream segments.
  """
  use Bunch
  use Bunch.Access

  alias Membrane.HTTPAdaptiveStream.Manifest
  alias Membrane.HTTPAdaptiveStream.Manifest.Changeset

  @type config_t :: struct
  @type state_t :: any
  @type callback_result_t :: :ok | {:error, reason :: any}

  @type segment_metadata_t :: %{duration: Membrane.Time.t()}
  @type partial_segment_metadata :: %{
          duration: Membrane.Time.t(),
          independent: boolean(),
          byte_offset: non_neg_integer()
        }
  @type metadata_t :: segment_metadata_t() | partial_segment_metadata() | %{}

  @typedoc """
  The identifier of a parent that the resource belongs to (the track identifier).

  It can either be a master or secondary playlist (a track playlist).
  In case of master playlist the identifier will be `:master`  while for tracks it can be an arbitrary value.
  """
  @type parent_t :: any()

  @doc """
  Generates the storage state based on the configuration struct.
  """
  @callback init(config_t) :: state_t

  @doc """
  Stores the resource on a storage.

  Gets the mode that should be used when writing to a file and type of the resource
  """
  @callback store(
              parent_id :: parent_t(),
              resource_name :: String.t(),
              content :: String.t() | binary,
              metadata :: metadata_t(),
              context :: %{
                type: :manifest | :header | :segment | :partial_segment,
                mode: :text | :binary
              },
              state_t
            ) :: {callback_result_t, state :: state_t}

  @doc """
  Removes the resource.

  In case of removing a segment the storage should make sure to remove all
  previous partial segments with the same name. It is the user's responsibility to remember
  and distinguish between the partial segments.
  """
  @callback remove(
              parent_id :: parent_t(),
              resource_name :: String.t(),
              context :: %{type: :manifest | :header | :segment},
              state_t
            ) :: {callback_result_t, state :: state_t}

  @enforce_keys [:storage_impl, :impl_state, :cache_enabled?]
  defstruct @enforce_keys ++ [cache: %{}, stored_manifests: MapSet.new()]

  @opaque t :: %__MODULE__{
            storage_impl: module,
            impl_state: any,
            cache_enabled?: bool,
            cache: map,
            stored_manifests: MapSet.t()
          }

  @doc """
  Initializes the storage.

  Accepts the following options:
  - `enable_cache` - if true (default), manifests will be stored only if they've been changed
  """
  @spec new(config_t, [{:enable_cache, boolean}]) :: t
  def new(%storage_impl{} = storage_config, opts \\ []) do
    %__MODULE__{
      storage_impl: storage_impl,
      impl_state: storage_impl.init(storage_config),
      cache_enabled?: Keyword.get(opts, :enable_cache?, true)
    }
  end

  @doc """
  Stores serialized manifest files
  """
  @spec store_manifests(t, Manifest.serialized_manifests_t()) ::
          {callback_result_t, t}
  def store_manifests(storage, %{
        master_manifest: master_manifest,
        manifest_per_track: manifest_per_track
      }) do
    manifests = [{:master, master_manifest} | Map.to_list(manifest_per_track)]
    Bunch.Enum.try_reduce(manifests, storage, &store_manifest/2)
  end

  defp store_manifest({id, {name, manifest}}, storage) do
    %__MODULE__{
      storage_impl: storage_impl,
      impl_state: impl_state,
      cache: cache,
      cache_enabled?: cache_enabled?,
      stored_manifests: stored_manifests
    } = storage

    withl cache: false <- cache[name] == manifest,
          store:
            {:ok, impl_state} <-
              storage_impl.store(
                id,
                name,
                manifest,
                %{},
                %{mode: :text, type: :manifest},
                impl_state
              ),
          do:
            storage = %{
              storage
              | stored_manifests: MapSet.put(stored_manifests, {id, name}),
                impl_state: impl_state
            },
          update_cache?: true <- cache_enabled? do
      storage = put_in(storage, [:cache, name], manifest)
      {:ok, storage}
    else
      cache: true ->
        {:ok, storage}

      store: {{:error, reason}, impl_state} ->
        {{:error, reason}, %{storage | impl_state: impl_state}}

      update_cache?: false ->
        {:ok, storage}
    end
  end

  @doc """
  Stores stream header file.
  """
  @spec store_header(t, track_id :: term(), name :: String.t(), payload :: binary) ::
          {callback_result_t, t}
  def store_header(storage, track_id, name, payload) do
    %__MODULE__{storage_impl: storage_impl, impl_state: impl_state} = storage

    {result, impl_state} =
      storage_impl.store(
        track_id,
        name,
        payload,
        %{},
        %{mode: :binary, type: :header},
        impl_state
      )

    {result, %{storage | impl_state: impl_state}}
  end

  @doc """
  Stores a new segment and removes stale ones.
  """
  @spec apply_track_changeset(
          t,
          track_id :: term(),
          Changeset.t()
        ) :: {callback_result_t, t}
  def apply_track_changeset(storage, track_id, changeset) do
    %__MODULE__{storage_impl: storage_impl, impl_state: impl_state} = storage
    %Changeset{to_add: to_add, to_remove: to_remove} = changeset

    grouped =
      Enum.group_by(
        to_remove,
        fn {type, _value} -> type end,
        fn {_type, value} -> value end
      )

    segment_names = Map.get(grouped, :segment, [])
    header_names = Map.get(grouped, :header, [])

    with {:ok, impl_state} <-
           Bunch.Enum.try_reduce(
             segment_names,
             impl_state,
             &storage_impl.remove(track_id, &1, %{type: :segment}, &2)
           ),
         {:ok, impl_state} <-
           Bunch.Enum.try_reduce(
             header_names,
             impl_state,
             &storage_impl.remove(track_id, &1, %{type: :header}, &2)
           ) do
      {result, impl_state} =
        Bunch.Enum.try_reduce(to_add, impl_state, fn segment, impl_state ->
          %{
            type: type,
            name: name,
            duration: duration,
            sequence_number: sequence_number,
            independent?: independent?,
            byte_offset: byte_offset,
            payload: payload
          } = segment

          metadata = %{
            duration: duration,
            sequence_number: sequence_number,
            independent?: independent?,
            byte_offset: byte_offset
          }

          storage_impl.store(
            track_id,
            name,
            payload,
            metadata,
            %{mode: :binary, type: type},
            impl_state
          )
        end)

      {result, %{storage | impl_state: impl_state}}
    end
  end

  @doc """
  Removes all segments grouped by track.
  """
  @spec clean_all_track_segments(t(), %{(id :: any()) => [String.t()]}) ::
          {callback_result_t, t}
  def clean_all_track_segments(storage, segments_per_track) do
    Bunch.Enum.try_reduce(segments_per_track, storage, fn {track_id, segments}, storage ->
      cleanup(storage, track_id, segments)
    end)
  end

  @doc """
  Removes all the saved segments and manifest for given id.
  """
  @spec cleanup(t, id :: any(), segments :: [String.t()]) :: {callback_result_t, t}
  def cleanup(storage, id, segments) do
    %__MODULE__{storage_impl: storage_impl, impl_state: impl_state, stored_manifests: manifests} =
      storage

    manifest_to_delete = Enum.find(manifests, fn {manifest_id, _name} -> manifest_id == id end)

    {manifest_remove_result, impl_state} =
      case manifest_to_delete do
        nil ->
          :ok

        {manifest_id, manifest_name} ->
          storage_impl.remove(manifest_id, manifest_name, %{type: :manifest}, impl_state)
      end

    with :ok <- manifest_remove_result,
         {:ok, _impl_state} <-
           Bunch.Enum.try_reduce(
             segments,
             impl_state,
             &storage_impl.remove(id, &1, %{type: :segment}, &2)
           ) do
      {:ok,
       %__MODULE__{
         storage
         | cache: %{},
           stored_manifests: MapSet.delete(manifests, manifest_to_delete)
       }}
    else
      error -> {error, %{storage | impl_state: impl_state}}
    end
  end

  @doc """
  Clears the manifest cache.
  """
  @spec clear_cache(t) :: t
  def clear_cache(storage) do
    %__MODULE__{storage | cache: %{}}
  end
end
