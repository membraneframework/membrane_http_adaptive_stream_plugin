defmodule Membrane.HTTPAdaptiveStream.Storage do
  @moduledoc """
  Behaviour for storing manifests and stream segments.
  """
  use Bunch
  use Bunch.Access

  @type config_t :: struct
  @type state_t :: any
  @type callback_result_t :: :ok | {:error, reason :: any}

  @doc """
  Generates the storage state based on the configuration struct.
  """
  @callback init(config_t) :: state_t

  @doc """
  Stores the resource.
  """
  @callback store(
              resource_name :: String.t(),
              content :: String.t(),
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
  - `enable_cache` - if true (default), manifests won't be stored when not changed
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
          {callback_result_t, t}
  def store_manifests(storage, manifests) do
    Bunch.Enum.try_reduce(manifests, storage, &store_manifest/2)
  end

  defp store_manifest({name, manifest}, storage) do
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
              storage_impl.store(name, manifest, %{mode: :text, type: :manifest}, impl_state),
          do: storage = %{storage | stored_manifests: MapSet.put(stored_manifests, name)},
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
  @spec store_header(t, name :: String.t(), payload :: binary) :: {callback_result_t, t}
  def store_header(storage, name, payload) do
    %__MODULE__{storage_impl: storage_impl, impl_state: impl_state} = storage

    result = storage_impl.store(name, payload, %{mode: :binary, type: :header}, impl_state)
    {result, storage}
  end

  @doc """
  Stores a new segment and removes stale ones.
  """
  @spec apply_segment_changeset(
          t,
          {to_add :: String.t(), to_remove :: [String.t()]},
          payload :: binary
        ) :: {callback_result_t, t}
  def apply_segment_changeset(storage, {to_add, to_remove}, payload) do
    %__MODULE__{storage_impl: storage_impl, impl_state: impl_state} = storage

    with :ok <-
           Bunch.Enum.try_each(to_remove, &storage_impl.remove(&1, %{type: :segment}, impl_state)),
         :ok <- storage_impl.store(to_add, payload, %{mode: :binary, type: :segment}, impl_state) do
      :ok
    end
    ~> {&1, storage}
  end

  @doc """
  Removes all the saved segments and manifests.
  """
  @spec cleanup(t, segments :: [String.t()]) :: {callback_result_t, t}
  def cleanup(storage, segments) do
    %__MODULE__{storage_impl: storage_impl, impl_state: impl_state, stored_manifests: manifests} =
      storage

    with :ok <-
           Bunch.Enum.try_each(
             manifests,
             &storage_impl.remove(&1, %{type: :manifest}, impl_state)
           ),
         :ok <-
           Bunch.Enum.try_each(segments, &storage_impl.remove(&1, %{type: :segment}, impl_state)) do
      {:ok, %__MODULE__{storage | cache: %{}, stored_manifests: MapSet.new()}}
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
