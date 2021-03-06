defmodule Membrane.HTTPAdaptiveStream.Manifest.Track do
  @moduledoc """
  Struct representing a state of a single manifest track and functions to operate
  on it.
  """
  defmodule Config do
    @moduledoc """
    Track configuration.
    """

    alias Membrane.HTTPAdaptiveStream.Manifest.Track

    @enforce_keys [
      :id,
      :content_type,
      :header_extension,
      :segment_extension,
      :target_segment_duration
    ]
    defstruct @enforce_keys ++
                [
                  target_window_duration: nil,
                  persist?: false
                ]

    @typedoc """
    Track configuration consists of the following fields:
    - `id` - identifies the track, will be serialized and attached to names of manifests, headers and segments
    - `content_type` - either audio or video
    - `header_extension` - extension of the header file (for example .mp4 for CMAF)
    - `segment_extension` - extension of the segment files (for example .m4s for CMAF)
    - `target_segment_duration` - expected duration of each segment
    - `target_window_duration` - track manifest duration is kept above that time, while the oldest segments
                are removed whenever possible
    - `persist?` - determines whether the entire track contents should be available after the streaming finishes
    """
    @type t :: %__MODULE__{
            id: Track.id_t(),
            content_type: :audio | :video,
            header_extension: String.t(),
            segment_extension: String.t(),
            target_segment_duration: Membrane.Time.t() | Ratio.t(),
            target_window_duration: Membrane.Time.t() | Ratio.t(),
            persist?: boolean
          }
  end

  @config_keys Config.__struct__() |> Map.from_struct() |> Map.keys()
  defstruct @config_keys ++
              [
                :id_string,
                :header_name,
                current_seq_num: 0,
                current_discontinuity_seq_num: 0,
                segments: Qex.new(),
                stale_segments: Qex.new(),
                stale_headers: Qex.new(),
                finished?: false,
                window_duration: 0,
                discontinuities_counter: 0
              ]

  @typedoc """
  The struct representing a track.

  Consists of all the fields from `Config.t` and also:
  - `id_string` - serialized `id`
  - `header_name` - name of the header file
  - `current_seq_num` - the number to identify the next segment
  - `current_discontinuity_seq_num` - number of current discontinuity sequence.
  - `segments` - segments' names and durations
  - `stale_segments` - stale segments' names and durations, kept empty unless `persist?` is set to true
  - `stale_headers` - stale headers' names, kept empty unless `persist?` is set to true
  - `finished?` - determines whether the track is finished
  - `window_duration` - current window duration
  """
  @type t :: %__MODULE__{
          id: id_t,
          content_type: :audio | :video,
          header_extension: String.t(),
          segment_extension: String.t(),
          target_segment_duration: segment_duration_t,
          target_window_duration: Membrane.Time.t() | Ratio.t(),
          persist?: boolean,
          id_string: String.t(),
          header_name: String.t(),
          current_seq_num: non_neg_integer,
          current_discontinuity_seq_num: non_neg_integer,
          segments: segments_t,
          stale_segments: segments_t,
          finished?: boolean,
          window_duration: non_neg_integer,
          discontinuities_counter: non_neg_integer
        }

  @type id_t :: any
  @type segments_t ::
          Qex.t(%{
            name: String.t(),
            duration: segment_duration_t(),
            attributes: list(segment_attribute_t())
          })
  @type segment_attribute_t :: any()
  @type segment_duration_t :: Membrane.Time.t() | Ratio.t()

  @type to_remove_names_t :: [segment_names: [String.t()], header_names: [String.t()]]

  @spec new(Config.t()) :: t
  def new(%Config{} = config) do
    id_string = config.id |> :erlang.term_to_binary() |> Base.url_encode64(padding: false)

    %__MODULE__{
      header_name: header_name(config, 0),
      id_string: id_string
    }
    |> Map.merge(Map.from_struct(config))
  end

  @spec add_segment(t, segment_duration_t, list(segment_attribute_t())) ::
          {{to_add_name :: String.t(), to_remove_names :: to_remove_names_t()}, t}
  def add_segment(%__MODULE__{finished?: false} = track, duration, attributes \\ []) do
    use Ratio, comparison: true

    name =
      "#{track.content_type}_segment_#{track.current_seq_num}_" <>
        "#{track.id_string}#{track.segment_extension}"

    {stale_segments, stale_headers, track} =
      track
      |> Map.update!(
        :segments,
        &Qex.push(&1, %{name: name, duration: duration, attributes: attributes})
      )
      |> Map.update!(:current_seq_num, &(&1 + 1))
      |> Map.update!(:window_duration, &(&1 + duration))
      |> Map.update!(:target_segment_duration, &if(&1 > duration, do: &1, else: duration))
      |> pop_stale_segments_and_headers()

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

    {{name, [segment_names: to_remove_segment_names, header_names: to_remove_header_names]},
     %__MODULE__{track | stale_segments: stale_segments, stale_headers: stale_headers}}
  end

  @spec discontinue(t()) ::
          {{header_name :: String.t(), discontinuity_seq_num :: non_neg_integer()}, t()}
  def discontinue(%__MODULE__{finished?: false, discontinuities_counter: counter} = track) do
    header = header_name(track, counter + 1)

    {{header, counter + 1}, %__MODULE__{track | discontinuities_counter: counter + 1}}
  end

  defp header_name(%{} = config, counter) do
    id_string = config.id |> :erlang.term_to_binary() |> Base.url_encode64(padding: false)

    "#{config.content_type}_header_#{id_string}_part#{counter}_#{config.header_extension}"
  end

  @spec finish(t) :: t
  def finish(track) do
    %__MODULE__{track | finished?: true}
  end

  @spec from_beginning(t) :: t
  def from_beginning(%__MODULE__{persist?: true} = track) do
    %__MODULE__{
      track
      | segments: Qex.join(track.stale_segments, track.segments),
        current_seq_num: 0
    }
  end

  @spec all_segments(t) :: [segment_name :: String.t()]
  def all_segments(%__MODULE__{} = track) do
    Qex.join(track.stale_segments, track.segments) |> Enum.map(& &1.name)
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
      case segment.attributes |> Enum.find(&match?({:discontinuity, _, _}, &1)) do
        {:discontinuity, new_header, seq_number} ->
          {new_header, seq_number}

        _ ->
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
end
