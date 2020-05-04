defmodule Membrane.Element.HTTPAdaptiveStream.SendStorage do
  @behaviour Membrane.Element.HTTPAdaptiveStream.Storage

  @enforce_keys [:destination]
  defstruct @enforce_keys

  @impl true
  def init(%__MODULE__{} = config), do: config

  @impl true
  def store(name, contents, context, %__MODULE__{destination: destination}) do
    send(destination, {__MODULE__, :store, Map.merge(context, %{name: name, contents: contents})})
    :ok
  end

  @impl true
  def remove(name, %__MODULE__{destination: destination}) do
    send(destination, {__MODULE__, :remove, name})
    :ok
  end
end
