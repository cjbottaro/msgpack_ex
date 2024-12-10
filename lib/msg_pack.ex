defmodule MsgPack do

  @coder Application.compile_env(:msg_pack, __MODULE__, [])
  |> Keyword.get(:coder, MsgPack.DefaultCoder)

  def encode(input, opts \\ []) do
    coder = Keyword.get(opts, :coder, @coder)
    with {:ok, iodata} <- coder.encode(input, opts) do
      {:ok, IO.iodata_to_binary(iodata)}
    end
  end

  def encode!(input, opts \\ []) do
    case encode(input, opts) do
      {:ok, output} -> output
      {:error, reason} -> raise(reason)
    end
  end

  def decode(input, opts \\ []) do
    coder = Keyword.get(opts, :coder, @coder)
    case coder.decode(input, opts) do
      {:ok, output, _rest} -> {:ok, output}
      error -> error
    end
  end

  def decode!(input, opts \\ []) do
    case decode(input, opts) do
      {:ok, output} -> output
      {:error, reason} -> raise(reason)
    end
  end

end
