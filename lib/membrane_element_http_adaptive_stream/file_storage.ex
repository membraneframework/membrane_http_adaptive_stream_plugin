defmodule Membrane.Element.HTTPAdaptiveStream.FileStorage do
  @behaviour Membrane.Element.HTTPAdaptiveStream.Storage

  @enforce_keys [:location]
  defstruct @enforce_keys

  @impl true
  def init(%__MODULE__{} = config), do: config

  @impl true
  def store(name, contents, %{mode: :binary}, %__MODULE__{location: location}) do
    File.write(Path.join(location, name), contents, [:binary])
  end

  @impl true
  def store(name, contents, %{mode: :text}, %__MODULE__{location: location}) do
    File.write(Path.join(location, name), contents)
  end

  @impl true
  def remove(name, _ctx, %__MODULE__{location: location}) do
    File.rm(Path.join(location, name))
  end
end
