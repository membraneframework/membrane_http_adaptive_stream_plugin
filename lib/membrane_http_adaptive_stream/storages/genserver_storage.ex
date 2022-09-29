defmodule Membrane.HTTPAdaptiveStream.Storages.GenServerStorage do
  @moduledoc """
  `Membrane.HTTPAdaptiveStream.Storage` implementation that issues a call or cast
  with a `t:message_t/0` to given destination on each call to store/remove.
  """

  @behaviour Membrane.HTTPAdaptiveStream.Storage

  @enforce_keys [:destination]
  defstruct @enforce_keys ++ [method: :call]

  @type t :: %__MODULE__{
          destination: Process.dest(),
          method: :call | :cast
        }

  @type message_t :: store_t | remove_t

  @type store_t ::
          {__MODULE__, :store,
           %{
             name: String.t(),
             contents: String.t(),
             type: :manifest | :header | :segment,
             mode: :text | :binary
           }}

  @type remove_t ::
          {__MODULE__, :remove, %{name: String.t(), type: :manifest | :header | :segment}}

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
  def store(parent_id, name, contents, metadata, context, %__MODULE__{
        destination: destination,
        method: method
      }) do
    params =
      Map.merge(context, %{
        parent_id: parent_id,
        name: name,
        contents: contents,
        metadata: metadata
      })

    method.(destination, {__MODULE__, :store, params})
  end

  @impl true
  def remove(parent_id, name, context, %__MODULE__{destination: destination, method: method}) do
    params = Map.merge(context, %{parent_id: parent_id, name: name})
    method.(destination, {__MODULE__, :remove, params})
  end
end
