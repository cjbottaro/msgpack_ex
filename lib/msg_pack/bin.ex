defmodule MsgPack.Bin do
  defstruct [:data]

  def new(data) do
    %__MODULE__{data: data}
  end
end
