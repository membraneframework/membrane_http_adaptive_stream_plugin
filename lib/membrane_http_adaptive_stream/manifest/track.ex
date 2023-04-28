defmodule Membrane.HTTPAdaptiveStream.Manifest.Track do
  @moduledoc """
  Struct representing a state of a single manifest track and functions to operate
  on it.
  """
  use Bunch.Access

  require Membrane.HTTPAdaptiveStream.Manifest.SegmentAttribute

  alias Membrane.HTTPAdaptiveStream.Manifest
  alias Membrane.HTTPAdaptiveStream.Manifest.{Changeset, Segment}

  defmodule Config do
    @moduledoc """
    Track configuration.
    """

    alias Membrane.HTTPAdaptiveStream.Manifest.Track

    @enforce_keys [
      :id,
      :track_name,
      :content_type,
      :header_extension,
      :segment_extension,
      :segment_duration,
      :partial_segment_duration,
      :header_naming_fun,
      :segment_naming_fun
    ]
    defstruct @enforce_keys ++
                [
                  target_window_duration: :infinity,
                  persist?: false,
                  mode: :vod
                ]

    @typedoc """
    Track configuration consists of the following fields:
    - `id` - identifies the track, will be serialized and attached to names of manifests, headers and segments
    - `track_name` - the name of the track, determines how manifest files will be named
    - `content_type` - either audio or video
    - `header_extension` - extension of the header file (for example .mp4 for CMAF)
    - `segment_extension` - extension of the segment files (for example .m4s for CMAF)
    - `segment_duration` - expected duration of segments.
    - `partial_segment_duration` - expected duration of each partial segment, nil if not partial segments are expected
    - `header_naming_fun` - a function that generates consequent header names for a given track
    - `segment_naming_fun` - a function that generates consequent segment names for a given track
    - `target_window_duration` - track manifest duration is kept above that time, while the oldest segments
                are removed whenever possible
    - `persist?` - determines whether the entire track contents should be available after the streaming finishes
    - `mode`-   track's mode that dictates type of metadata inserted into playlist's manifest

    """
    @type t :: %__MODULE__{
            id: Track.id_t(),
            track_name: String.t(),
            content_type: :audio | :video,
            header_extension: String.t(),
            segment_extension: String.t(),
            header_naming_fun: (Track.t(), counter :: non_neg_integer() -> String.t()),
            segment_naming_fun: (Track.t() -> String.t()),
            segment_duration: Membrane.Time.t(),
            partial_segment_duration: Membrane.Time.t() | nil,
            target_window_duration: Membrane.Time.t(),
            persist?: boolean(),
            mode: :vod | :live
          }
  end

  @config_keys Config.__struct__() |> Map.from_struct() |> Map.keys()
  defstruct @config_keys ++
              [
                :header_name,
                current_seq_num: 0,
                current_discontinuity_seq_num: 0,
                segments: Qex.new(),
                stale_segments: Qex.new(),
                stale_headers: Qex.new(),
                finished?: false,
                window_duration: 0,
                discontinuities_counter: 0,
                awaiting_discontinuity: nil,
                next_segment_id: 0,
                segment_sequencer: {0, 0}
              ]

  @typedoc """
  The struct representing a track.

  Consists of all the fields from `Config.t` and also:
  - `header_name` - name of the header file
  - `current_seq_num` - the number to identify the next segment
  - `current_discontinuity_seq_num` - number of current discontinuity sequence.
  - `segments` - segments' names and durations
  - `stale_segments` - stale segments' names and durations, kept empty unless `persist?` is set to true
  - `stale_headers` - stale headers' names, kept empty unless `persist?` is set to true
  - `finished?` - determines whether the track is finished
  - `window_duration` - current window duration
  - `discontinuities_counter` - the number of discontinuities that happened so far
  - `next_segment_id` - the sequence number of the next segment that will be generated
  - `segment_sequencer` - keeps track of the current sequence number of a segment.
  """
  @type t :: %__MODULE__{
          id: id_t,
          content_type: :audio | :video | :muxed,
          header_extension: String.t(),
          segment_extension: String.t(),
          partial_segment_duration: segment_duration_t | nil,
          segment_duration: Membrane.Time.t(),
          header_naming_fun: (t, counter :: non_neg_integer() -> String.t()),
          segment_naming_fun: (t -> String.t()),
          target_window_duration: Membrane.Time.t() | Ratio.t(),
          persist?: boolean,
          track_name: String.t(),
          header_name: String.t(),
          current_seq_num: non_neg_integer,
          current_discontinuity_seq_num: non_neg_integer,
          segments: segments_t,
          stale_segments: segments_t,
          finished?: boolean,
          window_duration: non_neg_integer,
          discontinuities_counter: non_neg_integer,
          next_segment_id: non_neg_integer(),
          segment_sequencer: {msn :: non_neg_integer(), part_sn :: non_neg_integer}
        }

  @type id_t :: any

  @type segments_t :: Qex.t(Segment.t())

  @type segment_duration_t :: Membrane.Time.t() | Ratio.t()

  @type segment_size_t :: non_neg_integer()

  @type segment_payload_t :: binary()

  @type segment_metadata :: %{
          payload: segment_payload_t(),
          complete?: boolean(),
          independent?: boolean(),
          last_chunk?: boolean(),
          duration: segment_duration_t(),
          size: segment_size_t()
        }

  @spec new(Config.t()) :: t
  def new(%Config{} = config) do
    type =
      case config.content_type do
        list when is_list(list) -> :muxed
        type -> type
      end

    config =
      config
      |> Map.from_struct()
      |> Map.put(:content_type, type)

    %__MODULE__{
      header_name: config.header_naming_fun.(config, 0) <> config.header_extension
    }
    |> Map.merge(config)
  end

  @spec default_segment_naming_fun(t) :: String.t()
  def default_segment_naming_fun(track) do
    Enum.join([track.content_type, "segment", track.next_segment_id, track.track_name], "_")
  end

  @spec default_header_naming_fun(t, non_neg_integer) :: String.t()
  def default_header_naming_fun(track, counter) do
    Enum.join([track.content_type, "header", track.track_name, "part", "#{counter}"], "_")
  end

  @doc """
  Tells whether the track is able to produce partial media segments.
  """
  @spec supports_partial_segments?(t()) :: boolean()
  def supports_partial_segments?(%__MODULE__{partial_segment_duration: duration}),
    do: duration != nil

  @spec persisted?(t()) :: boolean()
  def persisted?(%{persist?: persist?}), do: persist?

  @doc """
  Recognizes if its regular or partial segment and then update the track appropriately.
  Returns `Changeset`.
  """
  @spec add_chunk(
          t(),
          segment_metadata(),
          list(Manifest.SegmentAttribute.t())
        ) :: {Changeset.t(), t()}
  def add_chunk(track, opts, attributes \\ [])

  def add_chunk(
        %__MODULE__{finished?: false, partial_segment_duration: nil} = track,
        opts,
        attributes
      ),
      do: add_segment(track, opts, attributes)

  def add_chunk(
        %__MODULE__{
          finished?: false,
          segments: segments
        } = track,
        %{
          duration: duration,
          independent?: independent?,
          last_chunk?: last_chunk?,
          size: size,
          payload: payload
        },
        _attributes
      ) do
    new_segment_necessary? =
      case Qex.pop_back(segments) do
        {:empty, _segments} -> true
        {{:value, %Segment{type: :full}}, _segments} -> true
        {{:value, %Segment{type: :partial}}, _segments} -> false
      end

    track =
      if new_segment_necessary? do
        {_changeset, track} =
          add_segment(
            track,
            %{
              payload: payload,
              complete?: false,
              duration: duration,
              last_chunk?: true,
              size: size,
              independent?: true
            }
          )

        track
      else
        track
      end

    {changeset, track} =
      add_partial_segment(
        track,
        payload,
        independent?,
        duration,
        byte_size(payload)
      )

    {new_changeset, track} =
      if last_chunk?, do: finalize_current_segment(track), else: {%Changeset{}, track}

    {Changeset.merge(changeset, new_changeset), track}
  end

  def add_chunk(%__MODULE__{finished?: true} = _track, _opts, _attributes),
    do: raise("Cannot add new segments to finished track")

  @doc """
  Discontinue the track, indicating that parameters of the stream have changed.

  New header has to be stored under the returned filename.
  For details on discontinuity, please refer to [RFC 8216](https://datatracker.ietf.org/doc/html/rfc8216).
  """
  @spec discontinue(t()) :: {header_name :: String.t(), t()}
  def discontinue(%__MODULE__{finished?: false, discontinuities_counter: counter} = track) do
    header = track.header_naming_fun.(track, counter + 1) <> track.header_extension
    discontinuity = Manifest.SegmentAttribute.discontinuity(header, counter + 1)

    track =
      track
      |> Map.update!(:discontinuities_counter, &(&1 + 1))
      |> Map.put(:awaiting_discontinuity, discontinuity)

    {header, track}
  end

  def discontinue(%__MODULE__{finished?: true}), do: raise("Cannot discontinue finished track")

  @doc """
  Marks the track as finished and finalize last segment if needed. After this action, it won't be possible to add any new segments to the track.
  """
  @spec finish(t) :: {Changeset.t(), t()}
  def finish(track) do
    {changset, track} = maybe_finalize_current_segment(track)
    {changset, %__MODULE__{track | finished?: true}}
  end

  @doc """
  Return new track with all stale segments restored, resulting in playback of historic data.
  Only works with 'persist?' option enabled.
  """
  @spec from_beginning(t()) :: t()
  def from_beginning(%__MODULE__{persist?: true} = track) do
    %__MODULE__{
      track
      | segments: Qex.join(track.stale_segments, track.segments),
        current_seq_num: 0
    }
  end

  def from_beginning(%__MODULE__{persist?: false} = _track),
    do: raise("Cannot play the track from the beginning as it wasn't persisted")

  @doc """
  Returns all segments present in the track, including stale segments.
  """
  @spec all_segments(t) :: [segment_name :: String.t()]
  def all_segments(%__MODULE__{} = track) do
    Qex.join(track.stale_segments, track.segments) |> Enum.map(& &1.name)
  end

  # Add a segment of given duration to the track.
  # It is recommended not to pass discontinuity attribute manually but use `discontinue/1` function instead.
  @spec add_segment(
          t,
          segment_metadata(),
          list(Manifest.SegmentAttribute.t())
        ) ::
          {Changeset.t(), t}
  defp add_segment(track, opts, attributes \\ [])

  defp add_segment(
         %__MODULE__{finished?: false, mode: mode} = track,
         %{duration: duration, size: size, complete?: complete?, payload: payload},
         attributes
       ) do
    name = track.segment_naming_fun.(track) <> track.segment_extension

    attributes =
      if is_nil(track.awaiting_discontinuity),
        do: attributes,
        else: [track.awaiting_discontinuity | attributes]

    segment_type = if(complete?, do: :full, else: :partial)

    {elements_to_remove, track} =
      track
      |> Map.update!(
        :segments,
        &Qex.push(&1, %Segment{
          name: name,
          duration: duration,
          size: size,
          type: segment_type,
          attributes: attributes ++ creation_time(mode)
        })
      )
      |> Map.update!(:next_segment_id, &(&1 + 1))
      |> then(fn track ->
        if complete? do
          track
          |> Map.update!(:window_duration, &(&1 + duration))
          |> update_segment_duration(duration)
        else
          track
        end
      end)
      |> Map.put(:awaiting_discontinuity, nil)
      |> maybe_pop_stale_segments_and_headers()

    {to_add, track} =
      if complete? do
        {segment_sequence_number, track} = get_segment_sequence_number(track)

        new_segment = %Changeset.Segment{
          type: :segment,
          duration: duration,
          sequence_number: segment_sequence_number,
          name: name,
          independent?: true,
          payload: payload
        }

        {new_segment, track}
      else
        {partial_sequence_number, track} = get_partial_sequence_number(track, false)

        new_partial_segment = %Changeset.Segment{
          type: :partial_segment,
          duration: duration,
          sequence_number: partial_sequence_number,
          byte_offset: byte_size(payload),
          independent?: true,
          name: name,
          payload: payload
        }

        {new_partial_segment, track}
      end

    changeset = %Changeset{
      to_add: [to_add],
      to_remove: elements_to_remove
    }

    {changeset, track}
  end

  # Append a partial segment to the current incomplete segment.
  # The current segment is supposed to be of type `:partial`, meaning that it is still in a phase
  # of gathering partial segments before being finalized into a full segment. There can only be
  # a single such segment and it must be the last one.
  @spec add_partial_segment(
          t,
          segment_payload_t(),
          boolean,
          segment_duration_t,
          segment_size_t
        ) ::
          {Changeset.t(), t()}
  defp add_partial_segment(
         %__MODULE__{finished?: false} = track,
         payload,
         independent?,
         duration,
         size
       ) do
    use Ratio

    partial_segment = %{
      independent?: independent?,
      duration: duration,
      size: size,
      payload: payload
    }

    {%Segment{type: :partial} = last_segment, segments} = Qex.pop_back!(track.segments)

    {partial_sequence_number, track} = get_partial_sequence_number(track, true)

    new_partial_segment = %Changeset.Segment{
      type: :partial_segment,
      duration: duration,
      sequence_number: partial_sequence_number,
      byte_offset: Enum.map(last_segment.parts, & &1.size) |> Enum.sum(),
      independent?: independent?,
      name: last_segment.name,
      payload: payload
    }

    changeset = %Changeset{
      to_add: [new_partial_segment],
      to_remove: []
    }

    last_segment = %Segment{
      last_segment
      | parts: last_segment.parts ++ [partial_segment]
    }

    {changeset, Map.replace(track, :segments, Qex.push(segments, last_segment))}
  end

  # Finalize current segment, if it's possible. Otherwise do nothing.
  # Returns Changeset
  @spec maybe_finalize_current_segment(t()) :: {Changeset.t(), t}
  defp maybe_finalize_current_segment(%__MODULE__{finished?: false, segments: segments} = track) do
    case Qex.pop_back(segments) do
      {{:value, %Segment{type: :partial}}, _segments} -> finalize_current_segment(track)
      _else -> {%Changeset{}, track}
    end
  end

  defp maybe_finalize_current_segment(track), do: {%Changeset{}, track}

  # Finalize current segment.
  # With low latency, a regular segments gets created gradually. Each partial
  # segment gets appended to a regular one, if all parts are collected then
  # the regular segment is said to be complete.
  # This function aims to finalize the current (latest) segment
  # that is still incomplete so it can live on its own and so a new segment can
  # get started.
  @spec finalize_current_segment(t()) :: {Changeset.t(), t}
  defp finalize_current_segment(%__MODULE__{finished?: false} = track) do
    {%Segment{type: :partial, parts: parts} = last_segment, segments} =
      Qex.pop_back!(track.segments)

    {size, duration, payload} =
      Enum.reduce(parts, {0, 0, []}, fn part, {size, duration, payload} ->
        {size + part.size, duration + part.duration, payload ++ [part.payload]}
      end)

    payload = IO.iodata_to_binary(payload)

    parts = Enum.map(parts, &Map.put(&1, :payload, nil))

    last_segment = %Segment{
      last_segment
      | duration: duration,
        size: size,
        type: :full,
        parts: parts
    }

    {elements_to_remove, track} =
      track
      |> Map.replace!(:segments, Qex.push(segments, last_segment))
      |> Map.update!(:window_duration, &(&1 + duration))
      |> update_segment_duration(duration)
      |> maybe_pop_stale_segments_and_headers()

    {segment_sequence_number, track} = get_segment_sequence_number(track)

    new_segment = %Changeset.Segment{
      type: :segment,
      duration: duration,
      sequence_number: segment_sequence_number,
      name: last_segment.name,
      independent?: true,
      payload: payload
    }

    changeset = %Changeset{
      to_add: [new_segment],
      to_remove: elements_to_remove
    }

    {changeset, track}
  end

  defp maybe_pop_stale_segments_and_headers(track) do
    {stale_segments, stale_headers, track} = pop_stale_segments_and_headers(track)

    track = %{track | current_seq_num: track.current_seq_num + Enum.count(stale_segments)}

    {to_remove_segment_names, to_remove_header_names, stale_segments, stale_headers} =
      if track.persist? do
        {
          [],
          [],
          Qex.join(track.stale_segments, Qex.new(stale_segments)),
          Qex.join(track.stale_headers, Qex.new(stale_headers))
        }
      else
        {
          Enum.map(stale_segments, & &1.name),
          stale_headers,
          track.stale_segments,
          track.stale_headers
        }
      end

    {
      Enum.map(to_remove_header_names, &{:header, &1}) ++
        Enum.map(to_remove_segment_names, &{:segment, &1}),
      %__MODULE__{track | stale_segments: stale_segments, stale_headers: stale_headers}
    }
  end

  defp pop_stale_segments_and_headers(%__MODULE__{target_window_duration: :infinity} = track) do
    {[], [], track}
  end

  defp pop_stale_segments_and_headers(track) do
    %__MODULE__{
      segments: segments,
      window_duration: window_duration,
      target_window_duration: target_window_duration,
      header_name: header_name,
      current_discontinuity_seq_num: discontinuity_seq_number
    } = track

    {segments_to_remove, headers_to_remove, segments, window_duration,
     {new_header_name, discontinuity_seq_number}} =
      do_pop_stale_segments(
        segments,
        window_duration,
        target_window_duration,
        [],
        [],
        {header_name, discontinuity_seq_number}
      )

    # filter out `new_header_name` as it could have been carried by some segment
    # that is about to be deleted but the header has become the main track header
    headers_to_remove =
      headers_to_remove
      |> Enum.filter(&(&1 != new_header_name))

    track = %__MODULE__{
      track
      | segments: segments,
        window_duration: window_duration,
        header_name: new_header_name,
        current_discontinuity_seq_num: discontinuity_seq_number
    }

    {segments_to_remove, headers_to_remove, track}
  end

  defp do_pop_stale_segments(
         segments,
         window_duration,
         target_window_duration,
         segments_acc,
         headers_acc,
         header
       ) do
    use Ratio, comparison: true
    {segment, new_segments} = Qex.pop!(segments)
    new_window_duration = window_duration - segment.duration

    new_header =
      case segment.attributes |> Enum.find(&match?({:discontinuity, {_, _}}, &1)) do
        {:discontinuity, {new_header, seq_number}} ->
          {new_header, seq_number}

        nil ->
          header
      end

    headers_acc =
      if new_header != header do
        {header_name, _} = header
        [header_name | headers_acc]
      else
        headers_acc
      end

    if new_window_duration >= target_window_duration and new_window_duration > 0 do
      do_pop_stale_segments(
        new_segments,
        new_window_duration,
        target_window_duration,
        [segment | segments_acc],
        headers_acc,
        new_header
      )
    else
      {Enum.reverse(segments_acc), Enum.reverse(headers_acc), segments, window_duration, header}
    end
  end

  defp creation_time(mode) do
    case mode do
      :live -> [{:creation_time, DateTime.utc_now()}]
      :vod -> []
    end
  end

  defp get_partial_sequence_number(
         %{segment_sequencer: {msn, part_sn}} = track,
         update?
       ) do
    case update? do
      true ->
        {part_sn, %{track | segment_sequencer: {msn, part_sn + 1}}}

      false ->
        {part_sn, track}
    end
  end

  defp get_segment_sequence_number(%{segment_sequencer: {msn, _part_sn}} = track),
    do: {msn, %{track | segment_sequencer: {msn + 1, 0}}}

  defp update_segment_duration(track, duration) do
    Map.update!(track, :segment_duration, &if(&1 > duration, do: &1, else: duration))
  end
end
