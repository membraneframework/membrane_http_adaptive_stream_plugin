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
    - `target_window_duration` - track manifest duration is keept above that time, while the oldest segments
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
                segments: Qex.new(),
                stale_segments: Qex.new(),
                finished?: false,
                window_duration: 0
              ]

  @typedoc """
  The struct representing a track.

  Consists of all the fields from `Config.t` and also:
  - `id_string` - serialized `id`
  - `header_name` - name of the header file
  - `current_seq_num` - the number to identify the next segment
  - `segments` - segments' names and durations
  - `stale_segments` - stale segments' names and durations, kept empty unless `persist?` is set to true
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
          segments: segments_t,
          stale_segments: segments_t,
          finished?: boolean,
          window_duration: non_neg_integer
        }

  @type id_t :: any
  @type segments_t :: Qex.t({name :: String.t(), segment_duration_t})
  @type segment_duration_t :: Membrane.Time.t() | Ratio.t()

  @spec new(Config.t()) :: t
  def new(%Config{} = config) do
    id_string = config.id |> :erlang.term_to_binary() |> Base.url_encode64(padding: false)

    %__MODULE__{
      header_name: "#{config.content_type}_header_#{id_string}#{config.header_extension}",
      id_string: id_string
    }
    |> Map.merge(Map.from_struct(config))
  end

  @spec add_segment(t, segment_duration_t) ::
          {{to_add_name :: String.t(), to_remove_names :: [String.t()]}, t}
  def add_segment(%__MODULE__{finished?: false} = track, duration) do
    use Ratio, comparison: true

    name =
      "#{track.content_type}_segment_#{track.current_seq_num}_" <>
        "#{track.id_string}#{track.segment_extension}"

    {stale_segments, track} =
      track
      |> Map.update!(:segments, &Qex.push(&1, %{name: name, duration: duration}))
      |> Map.update!(:current_seq_num, &(&1 + 1))
      |> Map.update!(:window_duration, &(&1 + duration))
      |> Map.update!(:target_segment_duration, &if(&1 > duration, do: &1, else: duration))
      |> pop_stale_segments()

    {to_remove_names, stale_segments} =
      if track.persist? do
        {[], Qex.join(track.stale_segments, Qex.new(stale_segments))}
      else
        {Enum.map(stale_segments, & &1.name), track.stale_segments}
      end

    {{name, to_remove_names}, %__MODULE__{track | stale_segments: stale_segments}}
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

  defp pop_stale_segments(%__MODULE__{target_window_duration: :infinity} = track) do
    {[], track}
  end

  defp pop_stale_segments(track) do
    %__MODULE__{
      segments: segments,
      window_duration: window_duration,
      target_window_duration: target_window_duration
    } = track

    {to_remove, segments, window_duration} =
      do_pop_stale_segments(segments, window_duration, target_window_duration, [])

    track = %__MODULE__{track | segments: segments, window_duration: window_duration}
    {to_remove, track}
  end

  defp do_pop_stale_segments(segments, window_duration, target_window_duration, acc) do
    use Ratio, comparison: true
    {segment, new_segments} = Qex.pop!(segments)
    new_window_duration = window_duration - segment.duration

    if new_window_duration >= target_window_duration and new_window_duration > 0 do
      do_pop_stale_segments(
        new_segments,
        new_window_duration,
        target_window_duration,
        [segment | acc]
      )
    else
      {Enum.reverse(acc), segments, window_duration}
    end
  end
end
