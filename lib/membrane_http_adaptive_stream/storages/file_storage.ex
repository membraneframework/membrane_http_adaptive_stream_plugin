defmodule Membrane.HTTPAdaptiveStream.Storages.FileStorage do
  @moduledoc """
  `Membrane.HTTPAdaptiveStream.Storage` implementation that saves the stream to
  files locally.
  """
  @behaviour Membrane.HTTPAdaptiveStream.Storage

  @enforce_keys [:directory]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          directory: Path.t()
        }

  @impl true
  def init(%__MODULE__{} = config), do: config

  @impl true
  def store(name, contents, %{mode: :binary}, %__MODULE__{directory: location}) do
    File.write(Path.join(location, name), contents, [:binary])
  end

  @impl true
  def store(name, contents, %{mode: :text}, %__MODULE__{directory: location}) do
    File.write(Path.join(location, name), contents)
  end

  @impl true
  def remove(name, _ctx, %__MODULE__{directory: location}) do
    File.rm(Path.join(location, name))
  end
end
