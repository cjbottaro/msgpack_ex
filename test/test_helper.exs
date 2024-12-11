ExUnit.start()

defmodule MsgPack.Case do
  use ExUnit.CaseTemplate

  using _ do
    quote do
      import unquote(__MODULE__)
    end
  end

  defmacro assert_format(data, format) do
    round_trip(data, format, data, [])
  end

  defmacro assert_format(input, format, {output, opts}) do
    round_trip(input, format, output, opts)
  end

  defmacro assert_format(input, format, output) do
    round_trip(input, format, output, [])
  end

  defp round_trip(input, format, output, options) do
    quote do
      assert {:ok, packed} = MsgPack.encode(unquote(input))
      assert <<unquote(format), _::bytes>> = packed
      assert {:ok, unpacked} = MsgPack.decode(packed, unquote(options))
      assert unpacked == unquote(output)
    end
  end
end

defmodule TimestampCoder do
  use MsgPack.Coder, ext_timestamp: true
end
