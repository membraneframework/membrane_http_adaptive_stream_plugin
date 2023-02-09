defmodule Membrane.HTTPAdaptiveStream.Storages.SendStorage do
  @moduledoc """
  `Membrane.HTTPAdaptiveStream.Storage` implementation that sends a `t:message_t/0`
  to given destination on each call to store/remove.
  """

  @behaviour Membrane.HTTPAdaptiveStream.Storage

  @enforce_keys [:destination]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          destination: Process.dest()
        }

  @type message_t :: store_t | remove_t

  @type store_t ::
          {__MODULE__, :store,
           %{
             name: String.t(),
             contents: String.t(),
             type: :manifest | :header | :segment | :partial_segment,
             mode: :text | :binary
           }}

  @type remove_t ::
          {__MODULE__, :remove, %{name: String.t(), type: :manifest | :header | :segment}}

  @impl true
  def init(%__MODULE__{} = config), do: config

  @impl true
  def store(parent_id, name, contents, metadata, context, %{destination: destination} = state) do
    send(
      destination,
      {__MODULE__, :store,
       Map.merge(context, %{
         parent_id: parent_id,
         name: name,
         contents: contents,
         metadata: metadata
       })}
    )

    {:ok, state}
  end

  @impl true
  def remove(parent_id, name, context, %{destination: destination} = state) do
    send(
      destination,
      {__MODULE__, :remove, Map.merge(context, %{parent_id: parent_id, name: name})}
    )

    {:ok, state}
  end
end
