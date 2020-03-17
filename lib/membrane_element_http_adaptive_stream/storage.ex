defmodule Membrane.Element.HTTPAdaptiveStream.Storage do
  use Bunch
  alias FE.Maybe

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

  @enforce_keys [:storage_impl, :impl_state]
  defstruct @enforce_keys

  def new(%storage_impl{} = storage_config) do
    %__MODULE__{storage_impl: storage_impl, impl_state: storage_impl.init(storage_config)}
  end

  def store_playlists(storage, playlists) do
    %__MODULE__{storage_impl: storage_impl, impl_state: impl_state} = storage

    result =
      Bunch.Enum.try_each(playlists, fn {name, playlist} ->
        storage_impl.store(name, playlist, %{mode: :text, type: :playlist}, impl_state)
      end)

    {result, storage}
  end

  def store_init(storage, name, payload) do
    %__MODULE__{storage_impl: storage_impl, impl_state: impl_state} = storage

    result = storage_impl.store(name, payload, %{mode: :binary, type: :init}, impl_state)
    {result, storage}
  end

  def apply_chunk_changeset(storage, {to_add, to_remove}, payload) do
    %__MODULE__{storage_impl: storage_impl, impl_state: impl_state} = storage

    with :ok <-
           to_remove |> Maybe.map(&storage_impl.remove(&1, impl_state)) |> Maybe.unwrap_or(:ok),
         :ok <- storage_impl.store(to_add, payload, %{mode: :binary, type: :chunk}, impl_state) do
      :ok
    end
    ~> {&1, storage}
  end
end
