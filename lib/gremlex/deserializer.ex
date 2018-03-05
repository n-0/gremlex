defmodule Gremlex.Deserializer do
  @moduledoc """
  Deserializer module for deserializing data returned back from Gremlin.
  """
  alias Gremlex.{Edge, Vertex, VertexProperty}

  def deserialize(response) do
    %{"result" => result} = response

    case result["data"] do
      nil ->
        nil

      %{"@type" => type, "@value" => value} ->
        deserialize(type, value)
    end
  end

  def deserialize("g:List", value) do
    Enum.map(value, fn
      %{"@type" => type, "@value" => value} ->
        deserialize(type, value)

      value ->
        value
    end)
  end

  def deserialize("g:Set", value) do
    value
    |> Enum.map(fn
      %{"@type" => type, "@value" => value} ->
        deserialize(type, value)

      value ->
        value
    end)
    |> MapSet.new()
  end

  def deserialize("g:Vertex", value) do
    Vertex.from_response(value)
  end

  def deserialize("g:VertexProperty", value) do
    VertexProperty.from_response(value)
  end

  def deserialize("g:Edge", value) do
    Edge.from_response(value)
  end

  def deserialize("g:Int64", value) when is_number(value), do: value

  def deserialize("g:Int32", value), do: value

  def deserialize("g:Double", value) when is_number(value), do: value

  def deserialize("g:Float", value) when is_number(value), do: value

  def deserialize("g:UUID", value), do: value

  def deserialize("g:Date", value) do
    DateTime.from_unix!(value, :microsecond)
  end

  def deserialize("g:Timestamp", value) do
    DateTime.from_unix!(value, :microsecond)
  end

  def deserialize("g:Int64", value) when is_binary(value) do
    case Integer.parse(value) do
      {val, ""} ->
        val

      :error ->
        0
    end
  end

  def deserialize(_type, value), do: value
end
