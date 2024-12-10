defmodule MsgPack.Coder do

  @type reason :: binary
  @type type_id :: integer

  @callback encode(term, Keyword.t) :: {:ok, list | binary} | {:error, reason}
  @callback decode(binary, Keyword.t) :: {:ok, term, binary} | {:error, reason}
  @callback encode_ext(iodata, type_id) :: {:ok, iodata} | {:error, reason}
  @callback decode_ext(type_id, binary) :: {:ok, term} | {:error, reason}

  defmacro __using__(_opts \\ []) do
    quote do
      @behaviour MsgPack.Coder
      @before_compile MsgPack.Coder
    end
  end

  defmacro __before_compile__(_env) do
    quote do

      def encode(nil, _), do: {:ok, <<0xc0>>}

      def encode(true, _), do: {:ok, <<0xc2>>}
      def encode(false, _), do: {:ok, <<0xc3>>}

      def encode(a, opts) when is_atom(a) do
        encode(Atom.to_string(a), opts)
      end

      def encode(n, _) when is_integer(n) do
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

      def encode(f, _) when is_float(f), do: {:ok, <<0xcb, f::64-float>>}

      def encode(b, _) when is_binary(b) do
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

      def encode(%MsgPack.Bin{data: data}, _) do
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

      def encode(list, opts) when is_list(list) do
        len = length(list)

        marker = cond do
          len < 16 -> 0b10010000 + len
          len < 0x10000 -> <<0xdc, len::16>>
          len < 0x100000000 -> <<0xdd, len::32>>
          true -> nil
        end

        if marker do
          Enum.reduce_while(list, {:ok, [marker]}, fn item, {:ok, acc} ->
            case encode(item, opts) do
              {:ok, data} -> {:cont, {:ok, [acc, data]}}
              error -> {:halt, error}
            end
          end)
        else
          {:error, "list too big"}
        end
      end

      def encode(map, opts) when is_map(map) do
        size = map_size(map)

        marker = cond do
          size < 16 -> 0b10000000 + size
          size < 0x10000 -> <<0xde, size::16>>
          size < 0x100000000 -> <<0xdf, size::32>>
          true -> nil
        end

        if marker do
          Enum.reduce_while(map, {:ok, [marker]}, fn {k, v}, {:ok, acc} ->
            with {:ok, key_data} <- encode(k, opts),
              {:ok, value_data} <- encode(v, opts)
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

      def decode(<<0xc0, rest::binary>>, _), do: {:ok, nil, rest}

      def decode(<<0xc2, rest::binary>>, _), do: {:ok, true, rest}
      def decode(<<0xc3, rest::binary>>, _), do: {:ok, false, rest}

      def decode(<<0::1, n::7, rest::binary>>, _), do: {:ok, n, rest}
      def decode(<<0b111::3, n::5-signed, rest::binary>>, _), do: {:ok, n, rest}

      def decode(<<0xcc, n::8-unsigned, rest::binary>>, _), do: {:ok, n, rest}
      def decode(<<0xcd, n::16-unsigned, rest::binary>>, _), do: {:ok, n, rest}
      def decode(<<0xce, n::32-unsigned, rest::binary>>, _), do: {:ok, n, rest}
      def decode(<<0xcf, n::64-unsigned, rest::binary>>, _), do: {:ok, n, rest}

      def decode(<<0xd0, n::8-signed, rest::binary>>, _), do: {:ok, n, rest}
      def decode(<<0xd1, n::16-signed, rest::binary>>, _), do: {:ok, n, rest}
      def decode(<<0xd2, n::32-signed, rest::binary>>, _), do: {:ok, n, rest}
      def decode(<<0xd3, n::64-signed, rest::binary>>, _), do: {:ok, n, rest}

      def decode(<<0xca, f::32-float, rest::binary>>, _), do: {:ok, f, rest}
      def decode(<<0xcb, f::64-float, rest::binary>>, _), do: {:ok, f, rest}

      def decode(<<0b101::3, n::5, rest::binary>>, _) do
        case rest do
          <<str::binary-size(n), rest::binary>> -> {:ok, str, rest}
          _ -> {:error, "EOF while parsing fixstr"}
        end
      end

      def decode(<<0xd9, n::8, rest::binary>>, _) do
        case rest do
          <<str::binary-size(n), rest::binary>> -> {:ok, str, rest}
          _ -> {:error, "EOF while parsing str8"}
        end
      end

      def decode(<<0xda, n::16, rest::binary>>, _) do
        case rest do
          <<str::binary-size(n), rest::binary>> -> {:ok, str, rest}
          _ -> {:error, "EOF while parsing str16"}
        end
      end

      def decode(<<0xd9, n::32, rest::binary>>, _) do
        case rest do
          <<str::binary-size(n), rest::binary>> -> {:ok, str, rest}
          _ -> {:error, "EOF while parsing str32"}
        end
      end

      def decode(<<0xc4, n::8, rest::binary>>, _) do
        case rest do
          <<bin::binary-size(n), rest::binary>> -> {:ok, bin, rest}
          _ -> {:error, "EOF while parsing bin8"}
        end
      end

      def decode(<<0xc5, n::16, rest::binary>>, _) do
        case rest do
          <<bin::binary-size(n), rest::binary>> -> {:ok, bin, rest}
          _ -> {:error, "EOF while parsing bin16"}
        end
      end

      def decode(<<0xc6, n::32, rest::binary>>, _) do
        case rest do
          <<bin::binary-size(n), rest::binary>> -> {:ok, bin, rest}
          _ -> {:error, "EOF while parsing bin32"}
        end
      end

      def decode(<<0b1001::4, n::4, rest::binary>>, opts), do: decode_array(n, [], rest, opts)
      def decode(<<0xdc, n::16, rest::binary>>, opts), do: decode_array(n, [], rest, opts)
      def decode(<<0xdd, n::32, rest::binary>>, opts), do: decode_array(n, [], rest, opts)

      def decode(<<0b1000::4, n::4, rest::binary>>, opts), do: decode_map(n, %{}, rest, opts)
      def decode(<<0xde, n::16, rest::binary>>, opts), do: decode_map(n, %{}, rest, opts)
      def decode(<<0xdf, n::32, rest::binary>>, opts), do: decode_map(n, %{}, rest, opts)

      def decode(<<0xd4, type::8-signed, rest::binary>>, opts), do: decode_ext(1, type, rest, opts)
      def decode(<<0xd5, type::8-signed, rest::binary>>, opts), do: decode_ext(2, type, rest, opts)
      def decode(<<0xd6, type::8-signed, rest::binary>>, opts), do: decode_ext(4, type, rest, opts)
      def decode(<<0xd7, type::8-signed, rest::binary>>, opts), do: decode_ext(8, type, rest, opts)
      def decode(<<0xd8, type::8-signed, rest::binary>>, opts), do: decode_ext(16, type, rest, opts)
      def decode(<<0xc7, size::8, type::8-signed, rest::binary>>, opts), do: decode_ext(size, type, rest, opts)
      def decode(<<0xc8, size::16, type::8-signed, rest::binary>>, opts), do: decode_ext(size, type, rest, opts)
      def decode(<<0xc9, size::32, type::8-signed, rest::binary>>, opts), do: decode_ext(size, type, rest, opts)

      @spec encode_ext(iodata, integer) :: {:ok, iodata} | {:error, binary}

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

      @spec decode_ext(integer, iodata) :: {:ok, term} | {:error, binary}

      def decode_ext(type, _data), do: {:error, "no ext decoder found for type: #{type}"}

      @spec decode_array(non_neg_integer, list, binary, Keyword.t) :: {:ok, iodata} | {:error, binary}

      defp decode_array(0, items, rest, _opts), do: {:ok, Enum.reverse(items), rest}
      defp decode_array(n, items, rest, opts) do
        case decode(rest, opts) do
          {:ok, item, rest} -> decode_array(n-1, [item | items], rest, opts)
          error -> error
        end
      end

      @spec decode_map(non_neg_integer, map, binary, Keyword.t) :: {:ok, iodata} | {:error, binary}

      defp decode_map(0, items, rest, _opts), do: {:ok, items, rest}
      defp decode_map(n, items, rest, opts) do
        with {:ok, k, rest} <- decode(rest, opts),
          {:ok, v, rest} <- decode(rest, opts)
        do
          decode_map(n-1, Map.put(items, k, v), rest, opts)
        end
      end

      @spec decode_ext(non_neg_integer, integer, binary, Keyword.t) :: {:ok, iodata} | {:error, binary}
      @dialyzer {:nowarn_function, decode_ext: 4}

      defp decode_ext(size, type, rest, _opts) do
        case rest do
          <<data::binary-size(size), rest::binary>> ->
            case decode_ext(type, data) do
              {:ok, term} -> {:ok, term, rest}
              error -> error
            end

          _ -> {:error, "EOF while parsing ext#{size}"}
        end
      end

    end
  end

end
