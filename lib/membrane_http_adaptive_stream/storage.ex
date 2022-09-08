defmodule Membrane.HTTPAdaptiveStream.Storage do
  @moduledoc """
  Behaviour for storing manifests and stream segments.
  """
  use Bunch
  use Bunch.Access

  alias Membrane.HTTPAdaptiveStream.Manifest.Track.Changeset

  @type config_t :: struct
  @type state_t :: any
  @type callback_result_t :: :ok | {:error, reason :: any}

  @typedoc """
  The identifier of a parent that the resource belongs to.

  It can either be a main or secondary playlist (a track playlist).
  In case of main playlist the identifier will be `:main`  while for tracks it can be an arbitrary value.
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
              metadata :: map(),
              context :: %{
                type: :manifest | :header | :segment | :partial_segment,
                mode: :text | :binary
              },
              state_t
            ) :: callback_result_t

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
            ) :: callback_result_t

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
  @spec store_manifests(t, [{id :: term() | :main, {name :: String.t(), content :: String.t()}}]) ::
          {callback_result_t, t}
  def store_manifests(storage, manifests) do
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
            :ok <-
              storage_impl.store(
                id,
                name,
                manifest,
                %{},
                %{mode: :text, type: :manifest},
                impl_state
              ),
          do: storage = %{storage | stored_manifests: MapSet.put(stored_manifests, {id, name})},
          update_cache?: true <- cache_enabled? do
      storage = put_in(storage, [:cache, name], manifest)
      {:ok, storage}
    else
      cache: true -> {:ok, storage}
      store: {:error, reason} -> {{:error, reason}, storage}
      update_cache?: false -> {:ok, storage}
    end
  end

  @doc """
  Stores stream header file.
  """
  @spec store_header(t, track_id :: term(), name :: String.t(), payload :: binary) ::
          {callback_result_t, t}
  def store_header(storage, track_id, name, payload) do
    %__MODULE__{storage_impl: storage_impl, impl_state: impl_state} = storage

    result =
      storage_impl.store(
        track_id,
        name,
        payload,
        %{},
        %{mode: :binary, type: :header},
        impl_state
      )

    {result, storage}
  end

  @doc """
  Stores a new segment and removes stale ones.
  """
  @spec apply_segment_changeset(
          t,
          track_id :: term(),
          Changeset.t(),
          buffer :: Membrane.Buffer.t()
        ) :: {callback_result_t, t}
  def apply_segment_changeset(storage, track_id, changeset, buffer) do
    %__MODULE__{storage_impl: storage_impl, impl_state: impl_state} = storage

    %Changeset{to_add: {to_add_type, to_add_name, metadata}, to_remove: to_remove} = changeset

    grouped =
      Enum.group_by(
        to_remove,
        fn {type, _value} -> type end,
        fn {_type, value} -> value end
      )

    segment_names = Map.get(grouped, :segment, [])
    header_names = Map.get(grouped, :header, [])

    with :ok <-
           Bunch.Enum.try_each(
             segment_names,
             &storage_impl.remove(track_id, &1, %{type: :segment}, impl_state)
           ),
         :ok <-
           Bunch.Enum.try_each(
             header_names,
             &storage_impl.remove(track_id, &1, %{type: :header}, impl_state)
           ) do
      storage_impl.store(
        track_id,
        to_add_name,
        buffer.payload,
        metadata,
        %{mode: :binary, type: to_add_type},
        impl_state
      )
    end
    |> then(&{&1, storage})
  end

  @spec clean_all_track_segments(t(), %{(id :: any()) => [String.t()]}) :: {callback_result_t, t}
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

    manifest_remove_result =
      case manifest_to_delete do
        nil ->
          :ok

        {manifest_id, manifest_name} ->
          storage_impl.remove(manifest_id, manifest_name, %{type: :manifest}, impl_state)
      end

    with :ok <- manifest_remove_result,
         :ok <-
           Bunch.Enum.try_each(
             segments,
             &storage_impl.remove(id, &1, %{type: :segment}, impl_state)
           ) do
      {:ok,
       %__MODULE__{
         storage
         | cache: %{},
           stored_manifests: MapSet.delete(manifests, manifest_to_delete)
       }}
    else
      error -> {error, storage}
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
