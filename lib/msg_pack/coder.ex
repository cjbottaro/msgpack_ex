defmodule MsgPack.Coder do

  @type reason :: binary
  @type type_id :: integer

  @callback encode(term, Keyword.t) :: {:ok, list | binary} | {:error, reason}
  @callback decode(binary, Keyword.t) :: {:ok, term, binary} | {:error, reason}
  @callback encode_ext(iodata, type_id) :: {:ok, iodata} | {:error, reason}
  @callback decode_ext(type_id, binary) :: {:ok, term} | {:error, reason}

  defmacro __using__(opts \\ []) do
    ext_timestamp = case Keyword.get(opts, :ext_timestamp) do
      nil -> false
      false -> false
      true -> -1
      type_id when is_integer(type_id) -> type_id
    end

    quote do
      @behaviour MsgPack.Coder
      @before_compile MsgPack.Coder
      @ext_timestamp unquote(ext_timestamp)
    end
  end

  defmacro __before_compile__(_env) do
    quote do

      if @ext_timestamp do
        def encode(%DateTime{} = dt, _opts) do
          MsgPack.Coder.encode_timestamp(dt)
          |> encode_ext(@ext_timestamp)
        end

        def decode_ext(@ext_timestamp, data) do
          case MsgPack.Coder.decode_timestamp(data) do
            {:ok, dt} -> {:ok, dt}
            :error -> {:error, "invalid format: #{@ext_timestamp} (timestamp)"}
          end
        end
      end

      def encode(term, opts), do: MsgPack.Coder.encode(__MODULE__, term, opts)
      def decode(data, opts), do: MsgPack.Coder.decode(__MODULE__, data, opts)
      def encode_ext(data, type_id), do: MsgPack.Coder.encode_ext(__MODULE__, data, type_id)
      def decode_ext(type_id, _data), do: {:error, "unknown ext type #{type_id}"}

    end
  end

  @spec encode(module, term, Keyword.t) :: {:ok, iodata} | {:error, binary}
  @compile {:inline, [encode: 3]}

  def encode(_mod, nil, _), do: {:ok, <<0xc0>>}

  def encode(_mod, false, _), do: {:ok, <<0xc2>>}
  def encode(_mod, true, _), do: {:ok, <<0xc3>>}

  def encode(mod, a, opts) when is_atom(a) do
    mod.encode(Atom.to_string(a), opts)
  end

  def encode(_mod, n, _) when is_integer(n) do
    if n < 0 do
      cond do
        n >= -32 -> {:ok, [0x100 + n]}
        n >= -128 -> {:ok, [0xd0, 0x100 + n]}
        n >= -0x8000 -> {:ok, <<0xd1, n::16>>}
        n >= -0x80000000 -> {:ok, <<0xd2, n::32>>}
        n >= -0x8000000000000000 -> {:ok, <<0xd3, n::64>>}
        true -> {:error, "int too small"}
      end
    else
      cond do
        n < 128 -> {:ok, [n]}
        n < 256 -> {:ok, [0xcc, n]}
        n < 0x10000 -> {:ok, <<0xcd, n::16>>}
        n < 0x100000000 -> {:ok, <<0xce, n::32>>}
        n < 0x10000000000000000 -> {:ok, <<0xcf, n::64>>}
        true -> {:error, "int too big"}
      end
    end
  end

  def encode(_mod, f, _) when is_float(f), do: {:ok, <<0xcb, f::64-float>>}

  def encode(_mod, b, _) when is_binary(b) do
    size = byte_size(b)

    marker = cond do
      size < 32 -> 0b10100000 + size
      size < 256 -> [0xd9, size]
      size < 0x10000 -> <<0xda, size::16>>
      size < 0x100000000 -> <<0xdb, size::32>>
      true -> nil
    end

    if marker do
      {:ok, [marker | b]}
    else
      {:error, "string too big"}
    end
  end

  def encode(_mod, %MsgPack.Bin{data: data}, _) do
    size = byte_size(data)

    marker = cond do
      size < 256 -> [0xc4, size]
      size < 0x10000 -> <<0xc5, size::16>>
      size < 0x100000000 -> <<0xc6, size::32>>
      true -> nil
    end

    if marker do
      {:ok, [marker | data]}
    else
      {:error, "binary too big"}
    end
  end

  def encode(mod, list, opts) when is_list(list) do
    len = length(list)

    marker = cond do
      len < 16 -> 0b10010000 + len
      len < 0x10000 -> <<0xdc, len::16>>
      len < 0x100000000 -> <<0xdd, len::32>>
      true -> nil
    end

    if marker do
      Enum.reduce_while(list, {:ok, [marker]}, fn item, {:ok, acc} ->
        case mod.encode(item, opts) do
          {:ok, data} -> {:cont, {:ok, [acc, data]}}
          error -> {:halt, error}
        end
      end)
    else
      {:error, "list too big"}
    end
  end

  def encode(mod, map, opts) when is_map(map) do
    size = map_size(map)

    marker = cond do
      size < 16 -> 0b10000000 + size
      size < 0x10000 -> <<0xde, size::16>>
      size < 0x100000000 -> <<0xdf, size::32>>
      true -> nil
    end

    if marker do
      Enum.reduce_while(map, {:ok, [marker]}, fn {k, v}, {:ok, acc} ->
        with {:ok, key_data} <- mod.encode(k, opts),
          {:ok, value_data} <- mod.encode(v, opts)
        do
          {:cont, {:ok, [acc, key_data, value_data]}}
        else
          error -> {:halt, error}
        end
      end)
    else
      {:error, "map too big"}
    end
  end

  def encode(_mod, term, _opts) do
    {:error, "undefined encoder for #{inspect term}"}
  end

  @spec decode(module, binary, Keyword.t) :: {:ok, term, binary} | {:error, binary}
  @compile {:inline, [decode: 3]}

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

  def decode(_mod, <<0b101::3, n::5, rest::binary>>, _) do
    case rest do
      <<str::binary-size(n), rest::binary>> -> {:ok, str, rest}
      _ -> {:error, "EOF while parsing fixstr"}
    end
  end

  def decode(_mod, <<0xd9, n::8, rest::binary>>, _) do
    case rest do
      <<str::binary-size(n), rest::binary>> -> {:ok, str, rest}
      _ -> {:error, "EOF while parsing str8"}
    end
  end

  def decode(_mod, <<0xda, n::16, rest::binary>>, _) do
    case rest do
      <<str::binary-size(n), rest::binary>> -> {:ok, str, rest}
      _ -> {:error, "EOF while parsing str16"}
    end
  end

  def decode(_mod, <<0xdb, n::32, rest::binary>>, _) do
    case rest do
      <<str::binary-size(n), rest::binary>> -> {:ok, str, rest}
      _ -> {:error, "EOF while parsing str32"}
    end
  end

  def decode(_mod, <<0xc4, n::8, rest::binary>>, opts) do
    case rest do
      <<bin::binary-size(n), rest::binary>> ->
        bin = if opts[:binary], do: MsgPack.Bin.new(bin), else: bin
        {:ok, bin, rest}
      _ ->
        {:error, "EOF while parsing bin8"}
    end
  end

  def decode(_mod, <<0xc5, n::16, rest::binary>>, opts) do
    case rest do
      <<bin::binary-size(n), rest::binary>> ->
        bin = if opts[:binary], do: MsgPack.Bin.new(bin), else: bin
        {:ok, bin, rest}
      _ ->
        {:error, "EOF while parsing bin16"}
    end
  end

  def decode(_mod, <<0xc6, n::32, rest::binary>>, opts) do
    case rest do
      <<bin::binary-size(n), rest::binary>> ->
        bin = if opts[:binary], do: MsgPack.Bin.new(bin), else: bin
        {:ok, bin, rest}
      _ ->
        {:error, "EOF while parsing bin32"}
    end
  end

  def decode(mod, <<0b1001::4, n::4, rest::binary>>, opts), do: decode_array(mod, n, [], rest, opts)
  def decode(mod, <<0xdc, n::16, rest::binary>>, opts), do: decode_array(mod, n, [], rest, opts)
  def decode(mod, <<0xdd, n::32, rest::binary>>, opts), do: decode_array(mod, n, [], rest, opts)

  def decode(mod, <<0b1000::4, n::4, rest::binary>>, opts), do: decode_map(mod, n, %{}, rest, opts)
  def decode(mod, <<0xde, n::16, rest::binary>>, opts), do: decode_map(mod, n, %{}, rest, opts)
  def decode(mod, <<0xdf, n::32, rest::binary>>, opts), do: decode_map(mod, n, %{}, rest, opts)

  def decode(mod, <<0xd4, type::8-signed, rest::binary>>, opts), do: decode_ext(mod, 1, type, rest, opts)
  def decode(mod, <<0xd5, type::8-signed, rest::binary>>, opts), do: decode_ext(mod, 2, type, rest, opts)
  def decode(mod, <<0xd6, type::8-signed, rest::binary>>, opts), do: decode_ext(mod, 4, type, rest, opts)
  def decode(mod, <<0xd7, type::8-signed, rest::binary>>, opts), do: decode_ext(mod, 8, type, rest, opts)
  def decode(mod, <<0xd8, type::8-signed, rest::binary>>, opts), do: decode_ext(mod, 16, type, rest, opts)
  def decode(mod, <<0xc7, size::8, type::8-signed, rest::binary>>, opts), do: decode_ext(mod, size, type, rest, opts)
  def decode(mod, <<0xc8, size::16, type::8-signed, rest::binary>>, opts), do: decode_ext(mod, size, type, rest, opts)
  def decode(mod, <<0xc9, size::32, type::8-signed, rest::binary>>, opts), do: decode_ext(mod, size, type, rest, opts)

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

  @spec encode_ext(module, iodata, integer) :: {:ok, iodata} | {:error, binary}

  def encode_ext(_mod, data, type) do
    size = IO.iodata_length(data)
    cond do
      size ==  1 -> {:ok, [<<0xd4, type::8-signed>>, data]}
      size ==  2 -> {:ok, [<<0xd5, type::8-signed>>, data]}
      size ==  4 -> {:ok, [<<0xd6, type::8-signed>>, data]}
      size ==  8 -> {:ok, [<<0xd7, type::8-signed>>, data]}
      size == 16 -> {:ok, [<<0xd8, type::8-signed>>, data]}
      size < 0x100 -> {:ok, [<<0xc7, size::8, type::8-signed>>, data]}
      size < 0x10000 -> {:ok, [<<0xc8, size::16, type::8-signed>>, data]}
      size < 0x100000000 -> {:ok, [<<0xc9, size::32, type::8-signed>>, data]}
      true -> {:error, "ext too big"}
    end
  end

  @doc false
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

  @doc false
  @spec encode_timestamp(DateTime.t) :: binary

  def encode_timestamp(%DateTime{} = datetime) do
    import Bitwise

    total_nanoseconds = DateTime.to_unix(datetime, :nanosecond)
    seconds = Integer.floor_div(total_nanoseconds, 1_000_000_000)
    nanoseconds = Integer.mod(total_nanoseconds, 1_000_000_000)

    if seconds >>> 34 == 0 do
      content = nanoseconds <<< 34 ||| seconds

      if (content &&& 0xFFFFFFFF00000000) == 0 do
        <<content::32>>
      else
        <<content::64>>
      end
    else
      <<nanoseconds::32, seconds::64>>
    end
  end

  @nanosecond_range -62_167_219_200_000_000_000..253_402_300_799_999_999_999

  @doc false
  @spec decode_timestamp(binary) :: {:ok, DateTime.t} | :error

  def decode_timestamp(data) do
    case data do
      <<seconds::32>> ->
        DateTime.from_unix(seconds)

      <<nanoseconds::30, seconds::34>> ->
        total_nanoseconds = seconds * 1_000_000_000 + nanoseconds
        DateTime.from_unix(total_nanoseconds, :nanosecond)

      <<nanoseconds::32, seconds::64-signed>> ->
        total_nanoseconds = seconds * 1_000_000_000 + nanoseconds

        if total_nanoseconds in @nanosecond_range do
          DateTime.from_unix(total_nanoseconds, :nanosecond)
        else
          :error
        end

      _ ->
        :error
    end
  end
end
