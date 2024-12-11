defprotocol MsgPack.Struct do

  def to_map(struct)

end

defmodule MsgPack.Struct.Callbacks do

  defmacro __after_compile__(env, _bytecode) do
    dbg(env)
    # dbg(__MODULE__.__schema__(:fields))
    quote do
      dbg(__MODULE__)
      dbg(__MODULE__.__schema__(:fields))
    end
  end
end

defimpl MsgPack.Struct, for: Any do

  defmacro __deriving__(module, struct, opts) do
    fields = cond do
      fields = opts[:only] ->
        fields

      fields = opts[:except] ->
        Map.drop(struct, fields) |> Map.keys()

      is_map_key(struct, :__meta__) ->
        :ecto_schema

      true ->
        Map.keys(struct)
    end

    quote do
      defimpl MsgPack.Struct, for: unquote(module) do
        def to_map(arg) do
          fields = case unquote(fields) do
            :ecto_schema ->
              [:__struct__ | unquote(module).__schema__(:fields)]
            fields ->
              fields
          end
          Map.take(arg, fields)
        end
      end
    end
  end

  def to_map(arg) do
    {:error, "protocol not defined for #{arg}"}
  end

end
