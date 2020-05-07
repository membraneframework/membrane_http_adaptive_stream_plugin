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
             type: :playlist | :init | :chunk,
             mode: :text | :binary
           }}

  @type remove_t :: {__MODULE__, :remove, %{name: String.t(), type: :playlist | :init | :chunk}}

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
