defmodule Membrane.HTTPAdaptiveStream.Manifest.Track do
  @moduledoc """
  Struct representing a state of a single manifest track and functions to operate
  on it.
  """
  use Bunch.Access

  require Membrane.HTTPAdaptiveStream.Manifest.SegmentAttribute

  alias Membrane.HTTPAdaptiveStream.Manifest
  alias Membrane.HTTPAdaptiveStream.Manifest.Segment

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
      :target_segment_duration,
      :min_segment_duration,
      :target_partial_segment_duration,
      :header_naming_fun,
      :segment_naming_fun
    ]
    defstruct @enforce_keys ++
                [
                  target_window_duration: nil,
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
    - `header_naming_fun` - a function that generates consequent header names for a given track
    - `segment_naming_fun` - a function that generates consequent segment names for a given track
    - `target_segment_duration` - expected duration of each segment
    - `target_partial_segment_duration` - expected duration of each partial segment, nil if not partial segments are expected
    - `target_window_duration` - track manifest duration is kept above that time, while the oldest segments
                are removed whenever possible
    - `persist?` - determines whether the entire track contents should be available after the streaming finishes
    """
    @type t :: %__MODULE__{
            id: Track.id_t(),
            track_name: String.t(),
            content_type: :audio | :video,
            header_extension: String.t(),
            segment_extension: String.t(),
            header_naming_fun: (Track.t(), counter :: non_neg_integer() -> String.t()),
            segment_naming_fun: (Track.t() -> String.t()),
            target_segment_duration: Membrane.Time.t(),
            min_segment_duration: Membrane.Time.t(),
            target_partial_segment_duration: Membrane.Time.t() | nil,
            target_window_duration: Membrane.Time.t(),
            persist?: boolean(),
            mode: :vod | :live
          }
  end

  defmodule Changeset do
    @moduledoc """
    Structure representing changes that has been applied to the track. What element has been added
    and what elements are to be removed.
    """

    defmodule Segment do
      @type t :: %__MODULE__{
              metadata: %{
                duration: Membrane.Time.t() | Ratio.t(),
                sequence_number: non_neg_integer()
              },
              name: String.t(),
              payload: binary()
            }
      @enforce_keys [:metadata, :name, :payload]
      defstruct @enforce_keys
    end

    defmodule PartialSegment do
      @type t :: %__MODULE__{
              metadata: %{
                duration: Membrane.Time.t() | Ration.t(),
                sequence_number: non_neg_integer(),
                independent?: boolean(),
                byte_offset: non_neg_integer()
              },
              name: String.t(),
              payload: binary()
            }
      @enforce_keys [:metadata, :name, :payload]
      defstruct @enforce_keys
    end

    @type element_type_t :: :segment | :partial_segment | :header
    @type t :: %__MODULE__{
            to_add: [Segment | PartialSegment],
            to_remove: [{element_type_t(), name :: String.t()}]
          }
    defstruct to_add: [],
              to_remove: []

    @spec merge(t(), t()) :: t()
    def merge(%{to_add: to_add_a, to_remove: to_remove_a}, %{
          to_add: to_add_b,
          to_remove: to_remove_b
        }),
        do: %Changeset{to_add: to_add_a ++ to_add_b, to_remove: to_remove_a ++ to_remove_b}
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
  """
  @type t :: %__MODULE__{
          id: id_t,
          content_type: :audio | :video | :muxed,
          header_extension: String.t(),
          segment_extension: String.t(),
          target_partial_segment_duration: segment_duration_t | nil,
          target_segment_duration: segment_duration_t,
          min_segment_duration: segment_duration_t,
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
          segment_sequencer: {non_neg_integer(), non_neg_integer}
        }

  @type id_t :: any

  @type segments_t :: Qex.t(Segment.t())

  @type segment_duration_t :: Membrane.Time.t() | Ratio.t()

  @type segment_byte_size_t :: non_neg_integer()

  @type segment_payload_t :: binary()

  @type segment_opt_t :: %{
          payload: segment_payload_t(),
          complete?: boolean(),
          independent?: boolean(),
          duration: segment_duration_t(),
          byte_size: segment_byte_size_t()
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
  def supports_partial_segments?(%__MODULE__{target_partial_segment_duration: duration}),
    do: duration != nil

  @spec is_persisted?(t()) :: boolean()
  def is_persisted?(%{persist?: persist?}), do: persist?

  @spec new_segment(
          t(),
          segment_opt_t(),
          list(Manifest.SegmentAttribute.t())
        ) :: {Changeset.t(), t()}
  def new_segment(track, opts, attributes \\ [])

  def new_segment(
        %__MODULE__{target_partial_segment_duration: nil} = track,
        opts,
        attributes
      ),
      do: add_segment(track, opts, attributes)

  def new_segment(track, opts, attributes),
    do: new_partial_segment(track, opts, attributes)

  @spec add_segment(
          t,
          segment_opt_t(),
          list(Manifest.SegmentAttribute.t())
        ) ::
          {Changeset.t(), t}
  defp add_segment(track, opts, attributes \\ [])

  defp add_segment(
         %__MODULE__{finished?: false, mode: mode} = track,
         %{duration: duration, byte_size: byte_size, complete?: complete?, payload: payload},
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
          byte_size: byte_size,
          type: segment_type,
          attributes: attributes ++ creation_time(mode)
        })
      )
      |> Map.update!(:next_segment_id, &(&1 + 1))
      |> then(fn track ->
        if complete? do
          track
          |> Map.update!(:window_duration, &(&1 + duration))
          |> Map.update!(:target_segment_duration, &if(&1 > duration, do: &1, else: duration))
        else
          track
        end
      end)
      |> Map.put(:awaiting_discontinuity, nil)
      |> maybe_pop_stale_segments_and_headers()

    {to_add, track} =
      if complete? do
        {segment_sn, track} = get_segment_sn(track, true)
        metadata = %{duration: duration, sequence_number: segment_sn}
        to_add = %Changeset.Segment{metadata: metadata, name: name, payload: payload}
        {to_add, track}
      else
        {partial_sn, track} = get_partial_sn(track, false)

        metadata = %{
          duration: duration,
          sequence_number: partial_sn,
          byte_offset: byte_size(payload),
          independent?: true
        }

        to_add = %Changeset.PartialSegment{metadata: metadata, name: name, payload: payload}
        {to_add, track}
      end

    changeset = %Changeset{
      to_add: [to_add],
      to_remove: elements_to_remove
    }

    {changeset, track}
  end

  defp add_segment(%__MODULE__{finished?: true} = _track, _opts, _attributes),
    do: raise("Cannot add new segments to finished track")

  @spec add_partial_segment(
          t,
          segment_payload_t(),
          boolean,
          segment_duration_t,
          segment_byte_size_t
        ) ::
          {Changeset.t(), t()}
  defp add_partial_segment(
         %__MODULE__{finished?: false} = track,
         payload,
         independent?,
         duration,
         byte_size
       ) do
    use Ratio

    partial_segment = %{
      independent?: independent?,
      duration: duration,
      byte_size: byte_size,
      payload: payload
    }

    {%Segment{type: :partial} = last_segment, segments} = Qex.pop_back!(track.segments)

    {partial_sn, track} = get_partial_sn(track, true)

    metadata = %{
      duration: duration,
      sequence_number: partial_sn,
      byte_offset: Enum.map(last_segment.parts, & &1.byte_size) |> Enum.sum(),
      independent?: independent?
    }

    last_segment = %Segment{
      last_segment
      | parts: last_segment.parts ++ [partial_segment]
    }

    changeset = %Changeset{
      to_add: [
        %Changeset.PartialSegment{metadata: metadata, name: last_segment.name, payload: payload}
      ],
      to_remove: []
    }

    {changeset, Map.replace(track, :segments, Qex.push(segments, last_segment))}
  end

  @spec new_partial_segment(
          t,
          segment_opt_t(),
          list(Manifest.SegmentAttribute.t())
        ) ::
          {Changeset.t(), t}
  defp new_partial_segment(
         %__MODULE__{
           finished?: false,
           min_segment_duration: min_segment_duration,
           segments: segments
         } = track,
         %{
           duration: duration,
           independent?: independent?,
           byte_size: byte_size,
           payload: payload
         },
         _attributes
       ) do
    partials =
      case Qex.pop_back(segments) do
        {:empty, _segments} -> []
        {{:value, %Segment{type: :partial} = last_segment}, _segments} -> last_segment.parts
        _else -> []
      end

    partials_duration =
      Enum.reduce(partials, 0, fn part, total_duration ->
        total_duration + part.duration
      end)

    {changeset, track, partials} =
      if should_finalize_segment?(
           independent?,
           duration,
           partials,
           partials_duration,
           min_segment_duration
         ) do
        {changeset, track} = finalize_current_segment(track)
        # zamienić na update w tracku
        {changeset, track, []}
      else
        {%Changeset{}, track, partials}
      end

    new_segment_necessary? = Enum.empty?(partials)

    track =
      if new_segment_necessary? do
        {_changeset, track} =
          add_segment(
            track,
            %{
              payload: payload,
              complete?: false,
              duration: duration,
              byte_size: byte_size,
              independent?: true
            }
          )

        track
      else
        track
      end

    {new_changeset, track} =
      add_partial_segment(
        track,
        payload,
        independent?,
        duration,
        byte_size(payload)
      )

    {Changeset.merge(changeset, new_changeset), track}
  end

  defp should_finalize_segment?(
         independent?,
         duration,
         partials,
         partials_duration,
         min_segment_duration
       ) do
    partials != [] and independent? and duration + partials_duration > min_segment_duration
  end

  @spec maybe_finalize_current_segment(t()) :: {Changeset.t(), t}
  defp maybe_finalize_current_segment(%__MODULE__{finished?: false, segments: segments} = track) do
    case Qex.pop_back(segments) do
      {:empty, _segments} -> {%Changeset{}, track}
      {{:value, %Segment{parts: []}}, _segments} -> {%Changeset{}, track}
      {{:value, %Segment{type: :partial}}, _segments} -> finalize_current_segment(track)
      _else -> {%Changeset{}, track}
    end
  end

  defp maybe_finalize_current_segment(track), do: {%Changeset{}, track}

  @spec finalize_current_segment(t()) :: {Changeset.t(), t}
  defp finalize_current_segment(%__MODULE__{finished?: false} = track) do
    {%Segment{type: :partial, parts: parts} = last_segment, segments} =
      Qex.pop_back!(track.segments)

    {byte_size, duration, payload} =
      Enum.reduce(parts, {0, 0, <<>>}, fn part, {size, duration, payload} ->
        {size + part.byte_size, duration + part.duration,
         IO.iodata_to_binary([payload, part.payload])}
      end)

    parts = Enum.map(parts, &Map.put(&1, :payload, nil))

    last_segment = %Segment{
      last_segment
      | duration: duration,
        byte_size: byte_size,
        type: :full,
        parts: parts
    }

    {elements_to_remove, track} =
      track
      |> Map.replace!(:segments, Qex.push(segments, last_segment))
      |> Map.update!(:window_duration, &(&1 + duration))
      |> Map.update!(:target_segment_duration, &if(&1 > duration, do: &1, else: duration))
      |> maybe_pop_stale_segments_and_headers()

    {segment_sn, track} = get_segment_sn(track, true)
    metadata = %{duration: duration, sequence_number: segment_sn}

    changeset = %Changeset{
      to_add: [%Changeset.Segment{metadata: metadata, name: last_segment.name, payload: payload}],
      to_remove: elements_to_remove
    }

    {changeset, track}
  end

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
  Marks the track as finished. After this action, it won't be possible to add any new segments to the track.
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

  defp maybe_pop_stale_segments_and_headers(track) do
    {stale_segments, stale_headers, track} = pop_stale_segments_and_headers(track)

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

    track = %{track | current_seq_num: Enum.count(stale_segments)}

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

  defp creation_time(:live), do: [{:creation_time, DateTime.utc_now()}]
  defp creation_time(:vod), do: []

  defp get_partial_sn(%{segment_sequencer: {segment_sn, partial_sn}} = track, true),
    do: {partial_sn, %{track | segment_sequencer: {segment_sn, partial_sn + 1}}}

  defp get_partial_sn(%{segment_sequencer: {_segment_sn, partial_sn}} = track, _update?),
    do: {partial_sn, track}

  defp get_segment_sn(%{segment_sequencer: {segment_sn, _partial_sn}} = track, true),
    do: {segment_sn, %{track | segment_sequencer: {segment_sn + 1, 0}}}

  defp get_segment_sn(%{segment_sequencer: {segment_sn, _partial_sn}} = track, _update?),
    do: {segment_sn, track}
end
