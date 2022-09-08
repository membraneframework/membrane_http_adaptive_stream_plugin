defmodule Membrane.HTTPAdaptiveStream.Manifest.SegmentAttribute do
  @moduledoc """
  Definition of Segment Attributes and behaviour for serializing them.
  This module should also contain macros for generating different types of attributes
  """

  @type segment_type_t :: :discontinuity | :creation_time | atom()
  @type t :: {type :: segment_type_t(), arguments :: any()}

  @doc """
  Callback for serializing a segment attribute to a string. It is required for each implementation of this behavior.
  """
  @callback serialize(t()) :: [String.t()]

  @doc """
  Creates a definition of a discontinuity segment attribute.
  """
  defmacro discontinuity(header, discontinuity_index),
    do: {:discontinuity, {header, discontinuity_index}}
end
