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
  def store(
        _parent_id,
        name,
        contents,
        _metadata,
        %{mode: :binary},
        %{directory: location} = state
      ) do
    {File.write(Path.join(location, name), contents, [:binary]), state}
  end

  @impl true
  def store(_parent_id, name, contents, _metadata, %{mode: :text}, %{directory: location} = state) do
    {File.write(Path.join(location, name), contents), state}
  end

  @impl true
  def remove(_parent_id, name, _ctx, %__MODULE__{directory: location} = state) do
    {File.rm(Path.join(location, name)), state}
  end
end
