defmodule Membrane.HTTPAdaptiveStream.CMAFHeaderParser do
  @moduledoc """
  Helper module responsible for parsing the CMAF media header to obtain the media metadata.

  ## Supported codecs
  - AVC1 (H264)
  - AAC
  """

  @spec parse(binary()) :: %{(:audio | :video) => %{codec: :avc1 | :mp4a, params: map()}}
  def parse(header)

  def parse(<<>>), do: %{}

  def parse(header) do
    case Membrane.MP4.Container.parse(header) do
      {:ok, parsed, ""} ->
        parsed
        |> get_in([:moov, :children])
        |> Keyword.get_values(:trak)
        |> Enum.flat_map(fn track ->
          field = [
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

          get_in(track, field)
        end)
        |> Enum.map(&parse_media_section/1)
        |> Map.new(fn {type, codec, params} -> {type, %{codec: codec, params: params}} end)

      _other ->
        raise "Failed to parse mp4 header"
    end
  end

  @spec codec_string(%{codec: :avc1 | :mp4a, params: map()}) :: String.t()
  def codec_string(codec_map)

  def codec_string(%{codec: :avc1, params: params}) do
    %{profile: profile, compatibility: compatibility, level: level} = params

    [profile, compatibility, level]
    |> Enum.map(&Integer.to_string(&1, 16))
    |> Enum.map_join(&String.pad_leading(&1, 2, "0"))
    |> then(&"avc1.#{&1}")
    |> String.downcase()
  end

  def codec_string(%{codec: :mp4a, params: params}) do
    %{aot_id: aot_id} = params
    String.downcase("mp4a.40.#{aot_id}")
  end

  defp parse_media_section({:avc1, %{children: children}}) do
    avcc = get_in(children, [:avcC, :content])

    {:video, :avc1, parse_avcc(avcc)}
  end

  defp parse_media_section({:mp4a, %{children: children}}) do
    esds = get_in(children, [:esds, :fields, :elementary_stream_descriptor])

    {:audio, :mp4a, parse_esds(esds)}
  end

  defp parse_avcc(<<1, profile, compatibility, level, _rest::binary>>) do
    %{
      profile: profile,
      compatibility: compatibility,
      level: level
    }
  end

  # the process is reversed from this code:
  # https://github.com/membraneframework/membrane_mp4_plugin/blob/b3188727750ffc7330581e51e4b4bacbe361e927/lib/membrane_mp4/payloader/aac.ex#L51
  defp parse_esds(esds) do
    with <<_elementary_stream_id::16, _priority::8, rest::binary>> <- find_esds_section(3, esds),
         <<_section_4::binary-size(13), rest::binary>> <- find_esds_section(4, rest),
         <<aot_id::5, frequency_id::4, channel_config_id::4, _rest::bitstring>> <-
           find_esds_section(5, rest) do
      %{
        aot_id: aot_id,
        channels: channel_config_id,
        frequency: frequency_from_id(frequency_id)
      }
    else
      _other ->
        raise "Failed to parse esds"
    end
  end

  defp frequency_from_id(0), do: 96_000
  defp frequency_from_id(1), do: 88_200
  defp frequency_from_id(2), do: 64_000
  defp frequency_from_id(3), do: 48_000
  defp frequency_from_id(4), do: 44_100
  defp frequency_from_id(5), do: 32_000
  defp frequency_from_id(6), do: 24_000
  defp frequency_from_id(7), do: 22_050
  defp frequency_from_id(8), do: 16_000
  defp frequency_from_id(9), do: 12_000
  defp frequency_from_id(10), do: 11_025
  defp frequency_from_id(11), do: 8_000
  defp frequency_from_id(12), do: 7_350
  defp frequency_from_id(15), do: :explicit

  defp find_esds_section(section_no, payload) do
    case payload do
      <<^section_no::8, 128, 128, 128, section_size::8, payload::binary-size(section_size),
        __rest::binary>> ->
        payload

      <<_other_section::8, 128, 128, 128, section_size::8, _payload::binary-size(section_size),
        rest::binary>> ->
        find_esds_section(section_no, rest)

      _other ->
        nil
    end
  end
end
