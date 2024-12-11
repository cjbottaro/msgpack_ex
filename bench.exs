test_data = %{
  "string" => String.duplicate("hello", 1000),
  "binary" => :crypto.strong_rand_bytes(1024),
  "integer" => 42,
  "float" => 3.14159,
  "boolean" => true,
  "list" => Enum.to_list(1..1000),
  "map" => Map.new(1..1000, fn i -> {"key_#{i}", i} end),
  "nested" => %{
    "outer" => [
      %{"inner_key" => "inner_val", "nums" => Enum.to_list(1..100)},
      %{"another_inner" => %{"deep" => "value"}}
    ]
  }
}

# Pre-encode once to have something to decode during benchmarks
{:ok, encoded} = MsgPack.encode(test_data)
{:ok, packed} = Msgpax.pack(test_data, iodata: false)

Benchee.run(
  [
    {"encode", fn -> MsgPack.encode(test_data) end},
    {"pack", fn -> Msgpax.pack(test_data) end},
  ],
  time: 10,
  parallel: 2,
  formatters: [
    {Benchee.Formatters.Console, extended_statistics: false}
  ]
)

Benchee.run(
  [
    {"decode", fn -> MsgPack.decode(encoded) end},
    {"unpack", fn -> Msgpax.unpack(packed) end}
  ],
  time: 10,
  parallel: 2,
  formatters: [
    {Benchee.Formatters.Console, extended_statistics: false}
  ]
)
