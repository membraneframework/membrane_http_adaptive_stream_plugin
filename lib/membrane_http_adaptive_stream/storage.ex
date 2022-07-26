defmodule Membrane.HTTPAdaptiveStream.Storage do
  @moduledoc """
  Behaviour for storing manifests and stream segments.
  """
  use Bunch
  use Bunch.Access

  @type config_t :: struct
  @type state_t :: any

  @type stateless_result_t :: :ok | {:error, reason :: any}
  @type stateful_result_t :: {stateless_result_t, state_t}
  @type callback_result_t :: stateful_result_t | stateless_result_t

  @doc """
  Generates the storage state based on the configuration struct.
  """
  @callback init(config_t) :: state_t

  @doc """
  Stores the resource on a storage.

  Gets the mode that should be used when writing to a file and type of the resource
  """
  @callback store(
              resource_name :: String.t(),
              content :: String.t() | binary,
              context :: %{type: :manifest | :header | :segment, mode: :text | :binary},
              state_t
            ) :: callback_result_t

  @doc """
  Removes the resource.
  """
  @callback remove(
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
  @spec store_manifests(t, [{name :: String.t(), content :: String.t()}]) ::
          {stateless_result_t, t}
  def store_manifests(storage, manifests) do
    Bunch.Enum.try_reduce(manifests, storage, &store_manifest/2)
  end

  defp store_manifest({name, manifest}, storage) do
    %__MODULE__{
      cache: cache,
      cache_enabled?: cache_enabled?,
      stored_manifests: stored_manifests
    } = storage

    withl cache: false <- cache[name] == manifest,
          store:
            {:ok, storage} <- do_store(storage, name, manifest, %{mode: :text, type: :manifest}),
          do: storage = %{storage | stored_manifests: MapSet.put(stored_manifests, name)},
          update_cache?: true <- cache_enabled? do
      storage = put_in(storage, [:cache, name], manifest)
      {:ok, storage}
    else
      cache: true -> {:ok, storage}
      store: {{:error, _reason}, _storage} = error -> error
      update_cache?: false -> {:ok, storage}
    end
  end

  @doc """
  Stores stream header file.
  """
  @spec store_header(t, name :: String.t(), payload :: binary) :: {stateless_result_t, t}
  def store_header(storage, name, payload) do
    do_store(storage, name, payload, %{mode: :binary, type: :header})
  end

  @doc """
  Stores a new segment and removes stale ones.
  """
  @spec apply_segment_changeset(
          t,
          {to_add :: String.t(), to_remove :: [String.t()]},
          payload :: binary
        ) :: {stateless_result_t, t}
  def apply_segment_changeset(storage, {to_add, to_remove}, payload) do
    with {:ok, storage} <-
           Bunch.Enum.try_reduce(
             to_remove[:segment_names],
             storage,
             &do_remove(&2, &1, %{type: :segment})
           ),
         {:ok, storage} <-
           Bunch.Enum.try_reduce(
             to_remove[:header_names],
             storage,
             &do_remove(&2, &1, %{type: :header})
           ) do
      do_store(storage, to_add, payload, %{mode: :binary, type: :segment})
    end
  end

  @doc """
  Removes all the saved segments and manifests.
  """
  @spec cleanup(t, segments :: [String.t()]) :: {stateless_result_t, t}
  def cleanup(storage, segments) do
    %__MODULE__{stored_manifests: manifests} = storage

    with {:ok, storage} <-
           Bunch.Enum.try_reduce(manifests, storage, &do_remove(&2, &1, %{type: :manifest})),
         {:ok, storage} <-
           Bunch.Enum.try_reduce(segments, storage, &do_remove(&2, &1, %{type: :segment})) do
      {:ok, %__MODULE__{storage | cache: %{}, stored_manifests: MapSet.new()}}
    else
      error -> error
    end
  end

  @doc """
  Clears the manifest cache.
  """
  @spec clear_cache(t) :: t
  def clear_cache(storage) do
    %__MODULE__{storage | cache: %{}}
  end

  defp do_store(storage, resource_name, content, context),
    do: call_impl(storage, :store, [resource_name, content, context])

  defp do_remove(storage, resource_name, context),
    do: call_impl(storage, :remove, [resource_name, context])

  defp call_impl(storage, function_name, args) do
    %__MODULE__{
      storage_impl: storage_impl,
      impl_state: impl_state
    } = storage

    case apply(storage_impl, function_name, args ++ [impl_state]) do
      :ok -> {:ok, storage}
      {:error, _reason} = error -> {error, storage}
      {result, new_state} -> {result, %{storage | impl_state: new_state}}
    end
  end
end
