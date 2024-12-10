defmodule MsgPackTest do
  use ExUnit.Case
  doctest MsgPack

  test "greets the world" do
    assert MsgPack.hello() == :world
  end
end
