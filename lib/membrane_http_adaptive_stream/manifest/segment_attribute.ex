defmodule Membrane.HTTPAdaptiveStream.Manifest.SegmentAttribute do
  @moduledoc """
  Definition of Segment Attributes and behaviour for parsing them.
  This module also shall also contain macros for generating different types of attributes
  """

  @typep segment_type_t() :: :discontinuity
  @type t() :: {type :: segment_type_t(), arguments :: any()}

  @callback serialize(t()) :: [String.t()]

  defmacro discontinuity(header, discontinuity_index),
    do: {:discontinuity, {header, discontinuity_index}}
end
