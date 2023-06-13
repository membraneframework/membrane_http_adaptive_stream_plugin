defmodule Membrane.HTTPAdaptiveStream.EncodingInfo do
  @moduledoc """
  Helper module to retrieve codecs info from CMAF track
  ## currently supporting following codecs:
  - H264 (avc1)
  - AAC (mp4a)
  """

  alias Membrane.MP4.Container

  @spec from_cmaf_track(Membrane.CMAF.Track.t()) :: %{(:avc1 | :mp4a) => map()}
  def from_cmaf_track(%Membrane.CMAF.Track{header: header}) do
    case header do
      <<>> -> %{}
      _other -> parse_header(header)
    end
  end

  defp parse_header(header) do
    case Container.parse(header) do
      {:ok, parsed, ""} ->
        moov_children = parsed |> (fn el -> get_in(el, [:moov, :children]) end).()
        get_track_data(moov_children)

      _other ->
        raise "Failed to parse mp4 header"
    end
  end

  defp get_track_data(moov_children) do
    fields = [
      :children,
      :mdia,
      :children,
      :minf,
      :children,
      :stbl,
      :children,
      :stsd,
      :children
    ]

    moov_children
    |> Keyword.get_values(:trak)
    |> Enum.flat_map(&get_in(&1, fields))
    |> Enum.map(&parse_media_section/1)
    |> Map.new()
  end

  defp parse_media_section({:avc1, %{children: children}}) do
    avcc = get_in(children, [:avcC, :content])

    {:avc1, parse_avcc(avcc)}
  end

  defp parse_media_section({:mp4a, %{children: children}}) do
    esds = get_in(children, [:esds, :fields, :elementary_stream_descriptor])

    {:mp4a, parse_esds(esds)}
  end

  defp parse_avcc(<<1, profile, compatibility, level, _rest::binary>>) do
    %{
      profile: profile,
      compatibility: compatibility,
      level: level
    }
  end

  defp parse_esds(esds) do
    with <<_elementary_stream_id::16, _priority::8, rest::binary>> <- find_esds_section(3, esds),
         <<_section_4::binary-size(13), rest::binary>> <- find_esds_section(4, rest),
         <<aot_id::5, frequency_id::4, channel_config_id::4, _rest::bitstring>> <-
           find_esds_section(5, rest) do
      %{
        aot_id: aot_id,
        channels: channel_config_id,
        frequency: get_frequency(frequency_id)
      }
    else
      _other ->
        raise "Failed to parse esds"
    end
  end

  defp get_frequency(id) do
    %{
      0 => 96_000,
      1 => 88_200,
      2 => 64_000,
      3 => 48_000,
      4 => 44_100,
      5 => 32_000,
      6 => 24_000,
      7 => 22_050,
      8 => 16_000,
      9 => 12_000,
      10 => 11_025,
      11 => 8_000,
      12 => 7_350,
      15 => :explicit
    }
    |> Map.fetch!(id)
  end

  defp find_esds_section(section_number, payload) do
    case payload do
      <<^section_number::8, 128, 128, 128, section_size::8, payload::binary-size(section_size),
        __rest::binary>> ->
        payload

      <<_other_section::8, 128, 128, 128, section_size::8, _payload::binary-size(section_size),
        rest::binary>> ->
        find_esds_section(section_number, rest)

      _other ->
        nil
    end
  end
end
