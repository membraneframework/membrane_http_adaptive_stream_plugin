defmodule Membrane.Element.HTTPAdaptiveStream.Storage.File do
  @behaviour Membrane.Element.HTTPAdaptiveStream.Storage

  @enforce_keys [:location]
  defstruct @enforce_keys

  @impl true
  def init(%__MODULE__{} = config), do: config

  @impl true
  def store(name, contents, :binary, %__MODULE__{location: location}) do
    File.write(Path.join(location, name), contents, [:binary])
  end

  @impl true
  def store(name, contents, :text, %__MODULE__{location: location}) do
    File.write(Path.join(location, name), contents)
  end

  @impl true
  def remove(name, %__MODULE__{location: location}) do
    File.rm(Path.join(location, name))
  end
end
