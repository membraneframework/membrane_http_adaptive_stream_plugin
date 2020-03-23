defmodule Membrane.Element.HTTPAdaptiveStream.Storage do
  use Bunch
  use Bunch.Access

  @type config_t :: struct
  @type state_t :: any
  @callback init(config_t) :: state_t
  @callback store(
              resource_name :: String.t(),
              content :: String.t(),
              content_type :: :text | :binary,
              state_t
            ) :: :ok | {:error, reason :: any}
  @callback remove(resource_name :: String.t(), state_t) :: :ok | {:error, reason :: any}

  @enforce_keys [:storage_impl, :impl_state, :cache_enabled?]
  defstruct @enforce_keys ++ [cache: %{}]

  def new(%storage_impl{} = storage_config, opts \\ []) do
    %__MODULE__{
      storage_impl: storage_impl,
      impl_state: storage_impl.init(storage_config),
      cache_enabled?: Keyword.get(opts, :enable_cache?, true)
    }
  end

  def store_playlists(storage, playlists) do
    Bunch.Enum.try_reduce(playlists, storage, &store_playlist/2)
  end

  defp store_playlist({name, playlist}, storage) do
    %__MODULE__{
      storage_impl: storage_impl,
      impl_state: impl_state,
      cache: cache,
      cache_enabled?: cache_enabled?
    } = storage

    withl cache: false <- cache[name] == playlist,
          store:
            :ok <-
              storage_impl.store(name, playlist, %{mode: :text, type: :playlist}, impl_state),
          update_cache?: true <- cache_enabled? do
      storage = put_in(storage, [:cache, name], playlist)
      {:ok, storage}
    else
      cache: true -> {:ok, storage}
      store: {:error, reason} -> {{:error, reason}, storage}
      update_cache?: false -> {:ok, storage}
    end
  end

  def store_init(storage, name, payload) do
    %__MODULE__{storage_impl: storage_impl, impl_state: impl_state} = storage

    result = storage_impl.store(name, payload, %{mode: :binary, type: :init}, impl_state)
    {result, storage}
  end

  def apply_chunk_changeset(storage, {to_add, to_remove}, payload) do
    %__MODULE__{storage_impl: storage_impl, impl_state: impl_state} = storage

    with :ok <- Bunch.Enum.try_each(to_remove, &storage_impl.remove(&1, impl_state)),
         :ok <- storage_impl.store(to_add, payload, %{mode: :binary, type: :chunk}, impl_state) do
      :ok
    end
    ~> {&1, storage}
  end

  def clear_cache(storage) do
    %__MODULE__{storage | cache: %{}}
  end
end
