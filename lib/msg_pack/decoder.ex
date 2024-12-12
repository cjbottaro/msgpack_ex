defmodule MsgPack.Decoder do
  @moduledoc false

  @spec decode(module, binary, Keyword.t) :: {:ok, term, binary} | {:error, binary}

  # nil
  def decode(_mod, <<0xc0, rest::binary>>, _), do: {:ok, nil, rest}

  # bool
  def decode(_mod, <<0xc2, rest::binary>>, _), do: {:ok, false, rest}
  def decode(_mod, <<0xc3, rest::binary>>, _), do: {:ok, true, rest}

  # positive fixint
  def decode(_mod, <<0::1, n::7, rest::binary>>, _), do: {:ok, n, rest}

  # negative fixint
  def decode(_mod, <<0b111::3, n::5, rest::binary>>, _), do: {:ok, n - 0b100000, rest}

  # unsigned int
  def decode(_mod, <<0xcc, n::8-unsigned, rest::binary>>, _), do: {:ok, n, rest}
  def decode(_mod, <<0xcd, n::16-unsigned, rest::binary>>, _), do: {:ok, n, rest}
  def decode(_mod, <<0xce, n::32-unsigned, rest::binary>>, _), do: {:ok, n, rest}
  def decode(_mod, <<0xcf, n::64-unsigned, rest::binary>>, _), do: {:ok, n, rest}

  # signed int
  def decode(_mod, <<0xd0, n::8-signed, rest::binary>>, _), do: {:ok, n, rest}
  def decode(_mod, <<0xd1, n::16-signed, rest::binary>>, _), do: {:ok, n, rest}
  def decode(_mod, <<0xd2, n::32-signed, rest::binary>>, _), do: {:ok, n, rest}
  def decode(_mod, <<0xd3, n::64-signed, rest::binary>>, _), do: {:ok, n, rest}

  # float32
  def decode(_mod, <<0xca, 0x7FC00000::32, _rest::binary>>, _), do: {:error, :nan}
  def decode(_mod, <<0xca, 0x7F800000::32, _rest::binary>>, _), do: {:error, :infinity}
  def decode(_mod, <<0xca, 0xFF800000::32, _rest::binary>>, _), do: {:error, :neg_infinity}
  def decode(_mod, <<0xca, f::32-float, rest::binary>>, _), do: {:ok, f, rest}

  # float64
  def decode(_mod, <<0xcb, 0x7FF8::16, 0::48, _rest::binary>>, _), do: {:error, :nan}
  def decode(_mod, <<0xcb, 0x7FF0::16, 0::48, _rest::binary>>, _), do: {:error, :infinity}
  def decode(_mod, <<0xcb, 0xFFF0::16, 0::48, _rest::binary>>, _), do: {:error, :neg_infinity}
  def decode(_mod, <<0xcb, f::64-float, rest::binary>>, _), do: {:ok, f, rest}

  # fixstr
  def decode(_mod, <<0b101::3, n::5, rest::binary>>, _) do
    case rest do
      <<str::binary-size(n), rest::binary>> -> {:ok, str, rest}
      _ -> {:error, "EOF while parsing fixstr"}
    end
  end

  # str8
  def decode(_mod, <<0xd9, n::8, rest::binary>>, _) do
    case rest do
      <<str::binary-size(n), rest::binary>> -> {:ok, str, rest}
      _ -> {:error, "EOF while parsing str8"}
    end
  end

  # str16
  def decode(_mod, <<0xda, n::16, rest::binary>>, _) do
    case rest do
      <<str::binary-size(n), rest::binary>> -> {:ok, str, rest}
      _ -> {:error, "EOF while parsing str16"}
    end
  end

  # str32
  def decode(_mod, <<0xdb, n::32, rest::binary>>, _) do
    case rest do
      <<str::binary-size(n), rest::binary>> -> {:ok, str, rest}
      _ -> {:error, "EOF while parsing str32"}
    end
  end

  # bin8
  def decode(_mod, <<0xc4, n::8, rest::binary>>, opts) do
    case rest do
      <<bin::binary-size(n), rest::binary>> ->
        bin = if opts[:bin_as_string], do: bin, else: MsgPack.Bin.new(bin)
        {:ok, bin, rest}
      _ ->
        {:error, "EOF while parsing bin8"}
    end
  end

  # bin16
  def decode(_mod, <<0xc5, n::16, rest::binary>>, opts) do
    case rest do
      <<bin::binary-size(n), rest::binary>> ->
        bin = if opts[:bin_as_string], do: bin, else: MsgPack.Bin.new(bin)
        {:ok, bin, rest}
      _ ->
        {:error, "EOF while parsing bin16"}
    end
  end

  # bin32
  def decode(_mod, <<0xc6, n::32, rest::binary>>, opts) do
    case rest do
      <<bin::binary-size(n), rest::binary>> ->
        bin = if opts[:bin_as_string], do: bin, else: MsgPack.Bin.new(bin)
        {:ok, bin, rest}
      _ ->
        {:error, "EOF while parsing bin32"}
    end
  end

  # array
  def decode(mod, <<0b1001::4, n::4, rest::binary>>, opts), do: decode_array(mod, n, [], rest, opts)
  def decode(mod, <<0xdc, n::16, rest::binary>>, opts), do: decode_array(mod, n, [], rest, opts)
  def decode(mod, <<0xdd, n::32, rest::binary>>, opts), do: decode_array(mod, n, [], rest, opts)

  # map
  def decode(mod, <<0b1000::4, n::4, rest::binary>>, opts), do: decode_map(mod, n, %{}, rest, opts)
  def decode(mod, <<0xde, n::16, rest::binary>>, opts), do: decode_map(mod, n, %{}, rest, opts)
  def decode(mod, <<0xdf, n::32, rest::binary>>, opts), do: decode_map(mod, n, %{}, rest, opts)

  # ext
  def decode(mod, <<0xd4, type::8-signed, rest::binary>>, opts), do: decode_ext(mod, 1, type, rest, opts)
  def decode(mod, <<0xd5, type::8-signed, rest::binary>>, opts), do: decode_ext(mod, 2, type, rest, opts)
  def decode(mod, <<0xd6, type::8-signed, rest::binary>>, opts), do: decode_ext(mod, 4, type, rest, opts)
  def decode(mod, <<0xd7, type::8-signed, rest::binary>>, opts), do: decode_ext(mod, 8, type, rest, opts)
  def decode(mod, <<0xd8, type::8-signed, rest::binary>>, opts), do: decode_ext(mod, 16, type, rest, opts)
  def decode(mod, <<0xc7, size::8, type::8-signed, rest::binary>>, opts), do: decode_ext(mod, size, type, rest, opts)
  def decode(mod, <<0xc8, size::16, type::8-signed, rest::binary>>, opts), do: decode_ext(mod, size, type, rest, opts)
  def decode(mod, <<0xc9, size::32, type::8-signed, rest::binary>>, opts), do: decode_ext(mod, size, type, rest, opts)

  # errors
  def decode(_mod, <<t::8, _::binary>>, _opts), do: {:error, "undefined type: #{t}"}
  def decode(_mod, _data, _opts), do: {:error, "incomplete data"}

  @spec decode_array(module, non_neg_integer, list, binary, Keyword.t) :: {:ok, iodata} | {:error, binary}
  @compile {:inline, [decode_array: 5]}

  def decode_array(_mod, 0, items, rest, _opts), do: {:ok, Enum.reverse(items), rest}
  def decode_array(mod, n, items, rest, opts) do
    case mod.decode(rest, opts) do
      {:ok, item, rest} -> decode_array(mod, n-1, [item | items], rest, opts)
      error -> error
    end
  end

  @spec decode_map(module, non_neg_integer, map, binary, Keyword.t) :: {:ok, iodata} | {:error, binary}
  @compile {:inline, [decode_map: 5]}

  def decode_map(_mod, 0, items, rest, _opts), do: {:ok, items, rest}
  def decode_map(mod, n, items, rest, opts) do
    with {:ok, k, rest} <- mod.decode(rest, opts),
      {:ok, v, rest} <- mod.decode(rest, opts)
    do
      decode_map(mod, n-1, Map.put(items, k, v), rest, opts)
    end
  end

  @spec decode_ext(module, non_neg_integer, integer, binary, Keyword.t) :: {:ok, iodata} | {:error, binary}
  @compile {:inline, [decode_ext: 5]}

  def decode_ext(mod, size, type, rest, _opts) do
    case rest do
      <<data::binary-size(size), rest::binary>> ->
        case mod.decode_ext(type, data) do
          {:ok, term} -> {:ok, term, rest}
          error -> error
        end

      _ -> {:error, "EOF while parsing ext#{size}"}
    end
  end
end
