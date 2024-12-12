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

    quote [location: :keep] do
      @behaviour MsgPack.Coder
      @before_compile MsgPack.Coder
      @ext_timestamp unquote(ext_timestamp)

      def encode(v, opts) when is_nil(v), do: encode_nil(v, opts)
      def encode(v, opts) when is_boolean(v), do: encode_boolean(v, opts)
      def encode(v, opts) when is_atom(v), do: encode_atom(v, opts)
      def encode(v, opts) when is_float(v), do: encode_float(v, opts)
      def encode(v, opts) when is_integer(v), do: encode_integer(v, opts)
      def encode(v, opts) when is_binary(v), do: encode_binary(v, opts)
      def encode(v, opts) when is_bitstring(v), do: encode_bitstring(v, opts)
      def encode(v, opts) when is_tuple(v), do: encode_tuple(v, opts)
      def encode(v, opts) when is_list(v), do: encode_list(v, opts)
      def encode(v, opts) when is_struct(v), do: encode_struct(v, opts)
      def encode(v, opts) when is_map(v), do: encode_map(v, opts)

      @compile [:inline, [encode_ext: 2]]

      def encode_ext(data, type) do
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

      def decode(data, opts) do
        MsgPack.Decoder.decode(__MODULE__, data, opts)
      end

    end
  end

  defmacro __before_compile__(_env) do
    quote do

      @compile {:inline, [
        encode_nil: 2,
        encode_boolean: 2,
        encode_atom: 2,
        encode_float: 2,
        encode_integer: 2,
        encode_binary: 2,
        encode_bitstring: 2,
        encode_list: 2,
        encode_struct: 2,
        encode_map: 2,
      ]}

      def encode_nil(nil, _opts), do: {:ok, <<0xc0>>}

      def encode_boolean(false, _opts), do: {:ok, <<0xc2>>}
      def encode_boolean(true, _opts), do: {:ok, <<0xc3>>}

      def encode_atom(a, opts), do: encode_binary(Atom.to_string(a), opts)

      def encode_float(f, _opts), do: {:ok, <<0xcb, f::64-float>>}

      def encode_integer(n, opts) do
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

      def encode_binary(b, opts) do
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

      def encode_bin(b, opts) do
        size = byte_size(b)

        marker = cond do
          size < 256 -> [0xc4, size]
          size < 0x10000 -> <<0xc5, size::16>>
          size < 0x100000000 -> <<0xc6, size::32>>
          true -> nil
        end

        if marker do
          {:ok, [marker | b]}
        else
          {:error, "binary too big"}
        end
      end

      def encode_bitstring(_b, _opts), do: {:error, "encode_bitstring not implemented"}

      def encode_tuple(_t, _opts) do
        {:error, "encode_tuple not implemented"}
      end

      def encode_list(l, opts) do
        len = length(l)

        marker = cond do
          len < 16 -> 0b10010000 + len
          len < 0x10000 -> <<0xdc, len::16>>
          len < 0x100000000 -> <<0xdd, len::32>>
          true -> nil
        end

        if marker do
          encode_list(l, opts, [marker])
        else
          {:error, "list too big"}
        end
      end

      @compile {:inline, [encode_list: 3]}

      defp encode_list([], _opts, acc), do: {:ok, acc}
      defp encode_list([item | rest], opts, acc) do
        with {:ok, item_data} <- encode(item, opts) do
          encode_list(rest, opts, [acc, item_data])
        end
      end

      def encode_struct(%MsgPack.Bin{data: data}, opts) do
        if opts[:bin_as_string] do
          encode_binary(data, opts)
        else
          encode_bin(data, opts)
        end
      end

      if @ext_timestamp do

        def encode_struct(%DateTime{} = dt, _opts) do
          MsgPack.Coder.encode_timestamp(dt)
          |> encode_ext(@ext_timestamp)
        end

      end

      def encode_struct(s, opts) do
        {:error, "encode_struct not implemented for #{inspect s.__struct__}"}
      end

      def encode_map(m, opts) do
        size = map_size(m)

        marker = cond do
          size < 16 -> 0b10000000 + size
          size < 0x10000 -> <<0xde, size::16>>
          size < 0x100000000 -> <<0xdf, size::32>>
          true -> nil
        end

        if marker do
          encode_map(Map.to_list(m), opts, [marker])
        else
          {:error, "map too big"}
        end
      end

      @compile {:inline, [encode_map: 3]}

      defp encode_map([], _opts, acc), do: {:ok, acc}
      defp encode_map([{k, v} | rest], opts, acc) do
        with {:ok, kd} <- encode(k, opts),
          {:ok, vd} <- encode(v, opts)
        do
          encode_map(rest, opts, [acc, kd, vd])
        end
      end

      @compile [:inline, [decode_ext: 2]]

      if @ext_timestamp do
        def decode_ext(@ext_timestamp, data) do
          case MsgPack.Coder.decode_timestamp(data) do
            {:ok, dt} -> {:ok, dt}
            :error -> {:error, "invalid format: #{@ext_timestamp} (timestamp)"}
          end
        end
      end

      def decode_ext(type_id, _data), do: {:error, "unknown ext type #{type_id}"}

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
