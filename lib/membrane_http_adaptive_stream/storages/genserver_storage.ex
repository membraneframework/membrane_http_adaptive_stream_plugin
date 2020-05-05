defmodule Membrane.HTTPAdaptiveStream.Storages.GenServerStorage do
  @behaviour Membrane.HTTPAdaptiveStream.Storage

  @enforce_keys [:destination]
  defstruct @enforce_keys ++ [method: :call]

  @impl true
  def init(%__MODULE__{} = config) do
    method =
      case config.method do
        :call -> &GenServer.call/2
        :cast -> &GenServer.cast/2
      end

    %__MODULE__{config | method: method}
  end

  @impl true
  def store(name, contents, context, %__MODULE__{destination: destination, method: method}) do
    params = Map.merge(context, %{name: name, contents: contents})
    method.(destination, {__MODULE__, :store, params})
  end

  @impl true
  def remove(name, context, %__MODULE__{destination: destination, method: method}) do
    params = Map.merge(context, %{name: name})
    method.(destination, {__MODULE__, :remove, params})
  end
end
