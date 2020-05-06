defmodule Membrane.HTTPAdaptiveStream.Storage do
  @moduledoc """
  Behaviour for storing playlists and stream chunks.
  """
  use Bunch
  use Bunch.Access

  @type config_t :: struct
  @type state_t :: any
  @type callback_result_t :: :ok | {:error, reason :: any}

  @callback init(config_t) :: state_t
  @callback store(
              resource_name :: String.t(),
              content :: String.t(),
              context :: %{type: :playlist | :init | :chunk, mode: :text | :binary},
              state_t
            ) :: callback_result_t
  @callback remove(
              resource_name :: String.t(),
              context :: %{type: :playlist | :init | :chunk},
              state_t
            ) :: callback_result_t

  @enforce_keys [:storage_impl, :impl_state, :cache_enabled?]
  defstruct @enforce_keys ++ [cache: %{}, stored_playlists: MapSet.new()]

  @opaque t :: %__MODULE__{
            storage_impl: module,
            impl_state: any,
            cache_enabled?: bool,
            cache: map,
            stored_playlists: MapSet.t()
          }

  @doc """
  Initializes the storage.
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
  Stores serialized playlist files
  """
  @spec store_playlists(t, [{name :: String.t(), content :: String.t()}]) ::
          {callback_result_t, t}
  def store_playlists(storage, playlists) do
    Bunch.Enum.try_reduce(playlists, storage, &store_playlist/2)
  end

  defp store_playlist({name, playlist}, storage) do
    %__MODULE__{
      storage_impl: storage_impl,
      impl_state: impl_state,
      cache: cache,
      cache_enabled?: cache_enabled?,
      stored_playlists: stored_playlists
    } = storage

    withl cache: false <- cache[name] == playlist,
          store:
            :ok <-
              storage_impl.store(name, playlist, %{mode: :text, type: :playlist}, impl_state),
          do: storage = %{storage | stored_playlists: MapSet.put(stored_playlists, name)},
          update_cache?: true <- cache_enabled? do
      storage = put_in(storage, [:cache, name], playlist)
      {:ok, storage}
    else
      cache: true -> {:ok, storage}
      store: {:error, reason} -> {{:error, reason}, storage}
      update_cache?: false -> {:ok, storage}
    end
  end

  @doc """
  Stores stream init file.
  """
  @spec store_init(t, name :: String.t(), payload :: binary) :: {callback_result_t, t}
  def store_init(storage, name, payload) do
    %__MODULE__{storage_impl: storage_impl, impl_state: impl_state} = storage

    result = storage_impl.store(name, payload, %{mode: :binary, type: :init}, impl_state)
    {result, storage}
  end

  @doc """
  Stores a new chunk and removes stale ones.
  """
  @spec apply_chunk_changeset(
          t,
          {to_add :: String.t(), to_remove :: [String.t()]},
          payload :: binary
        ) :: {callback_result_t, t}
  def apply_chunk_changeset(storage, {to_add, to_remove}, payload) do
    %__MODULE__{storage_impl: storage_impl, impl_state: impl_state} = storage

    with :ok <-
           Bunch.Enum.try_each(to_remove, &storage_impl.remove(&1, %{type: :chunk}, impl_state)),
         :ok <- storage_impl.store(to_add, payload, %{mode: :binary, type: :chunk}, impl_state) do
      :ok
    end
    ~> {&1, storage}
  end

  @doc """
  Removes all the saved chunks and playlists.
  """
  @spec cleanup(t, chunks :: [String.t()]) :: {callback_result_t, t}
  def cleanup(storage, chunks) do
    %__MODULE__{storage_impl: storage_impl, impl_state: impl_state, stored_playlists: playlists} =
      storage

    with :ok <-
           Bunch.Enum.try_each(
             playlists,
             &storage_impl.remove(&1, %{type: :playlist}, impl_state)
           ),
         :ok <- Bunch.Enum.try_each(chunks, &storage_impl.remove(&1, %{type: :chunk}, impl_state)) do
      {:ok, %__MODULE__{storage | cache: %{}, stored_playlists: MapSet.new()}}
    else
      error -> {error, storage}
    end
  end

  @doc """
  Clears the playlist cache.
  """
  @spec clear_cache(t) :: t
  def clear_cache(storage) do
    %__MODULE__{storage | cache: %{}}
  end
end
