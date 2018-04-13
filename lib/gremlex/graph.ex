defmodule Gremlex.Graph do

  @moduledoc """
  Functions for traversing and mutating the Graph.

  Graph operations are stored in a queue which can be created with `g/0`.
  Mosts functions return the queue so that they can be chained together
  similar to how Gremlin queries work.

  Example:
  ```
  g.V(1).values("name")
  ```
  Would translate to
  ```
  g |> v(1) |> values("name")
  ```

  Note: This module doesn't actually execute any queries, it just allows you to build one.
  For query execution see `Gremlex.Client.query/1`
  """
  require Logger
  alias :queue, as: Queue

  @type t :: {[], []}

  @doc """
  Start of graph traversal. All graph operations are stored in a queue.
  """
  @spec g :: Gremlex.Graph.t()
  def g, do: Queue.new()

  def __ do
    enqueue(Queue.new(), "__", nil)
  end

  @doc """
  Appends an addV command to the traversal.
  Returns a graph to allow chaining.
  """
  @spec add_v(Gremlex.Graph.t(), any()) :: Gremlex.Graph.t()
  def add_v(graph, id) do
    enqueue(graph, "addV", [id])
  end

  @doc """
  Appends an addE command to the traversal.
  Returns a graph to allow chaining.
  """
  @spec add_e(Gremlex.Graph.t(), any()) :: Gremlex.Graph.t()
  def add_e(graph, edge) do
    enqueue(graph, "addE", [edge])
  end

  @spec has_label(Gremlex.Graph.t(), any()) :: Gremlex.Graph.t()
  def has_label(graph, label) do
    enqueue(graph, "hasLabel", [label])
  end

  @spec has(Gremlex.Graph.t(), any(), any()) :: Gremlex.Graph.t()
  def has(graph, key, value) do
    enqueue(graph, "has", [key, value])
  end

  @doc """
  Appends property command to the traversal.
  Returns a graph to allow chaining.
  """
  @spec property(Gremlex.Graph.t(), String.t(), any()) :: Gremlex.Graph.t()
  def property(graph, key, value) do
    enqueue(graph, "property", [key, value])
  end

  @doc """
  Appends values command to the traversal.
  Returns a graph to allow chaining.
  """
  @spec values(Gremlex.Graph.t(), String.t()) :: Gremlex.Graph.t()
  def values(graph, key) do
    enqueue(graph, "values", [key])
  end

  @doc """
  Appends values the `V` command allowing you to select a vertex.
  Returns a graph to allow chaining.
  """
  @spec v(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def v({h, t} = graph) when is_list(h) and is_list(t) do
    enqueue(graph, "V", [])
  end

  @spec v(number()) :: Gremlex.Vertex.t()
  def v(id) do
    %Gremlex.Vertex{id: id, label: ""}
  end

  @spec v(Gremlex.Graph.t(), Gremlex.Vertex.t()) :: Gremlex.Graph.t()
  def v(graph, %Gremlex.Vertex{id: id}) do
    enqueue(graph, "V", [id])
  end

  @doc """
  Appends values the `V` command allowing you to select a vertex.
  Returns a graph to allow chaining.
  """
  @spec v(Gremlex.Graph.t(), number()) :: Gremlex.Graph.t()
  def v(graph, id) when is_number(id) do
    enqueue(graph, "V", [id])
  end

  @spec out_e(Gremlex.Graph.t(), String.t()) :: Gremlex.Graph.t()
  def out_e(graph, edge) do
    enqueue(graph, "outE", [edge])
  end

  @spec out_e(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def out_e(graph) do
    enqueue(graph, "outE", [])
  end

  def repeat(graph, args) do
    enqueue(graph, "repeat", args)
  end

  def out(edge) do
    {"out", [edge]}
  end

  @spec out(Gremlex.Graph.t(), String.t()) :: Gremlex.Graph.t()
  def out(graph, edge) do
    enqueue(graph, "out", [edge])
  end

  @spec out(Gremlex.Graph.t(), String.t()) :: Gremlex.Graph.t()
  def out(graph) do
    enqueue(graph, "out", [])
  end

  @spec in_(Gremlex.Graph.t(), String.t()) :: Gremlex.Graph.t()
  def in_(graph, edge) do
    enqueue(graph, "in", [edge])
  end

  @spec in_(Gremlex.Graph.t(), String.t()) :: Gremlex.Graph.t()
  def in_(graph) do
    enqueue(graph, "in", [])
  end

  @spec or_(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def or_(graph) do
    enqueue(graph, "or", [])
  end

  @spec and_(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def and_(graph) do
    enqueue(graph, "and", [])
  end

  @spec in_v(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def in_v(graph) do
    enqueue(graph, "inV", [])
  end

  @spec out_v(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def out_v(graph) do
    enqueue(graph, "outV", [])
  end

  @spec both_v(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def both_v(graph) do
    enqueue(graph, "bothV", [])
  end

  @spec dedup(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def dedup(graph) do
    enqueue(graph, "dedup", [])
  end

  @spec to(Gremlex.Graph.t(), String.t()) :: Gremlex.Graph.t()
  def to(graph, target) do
    enqueue(graph, "to", [target])
  end

  @spec has_next(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def has_next(graph) do
    enqueue(graph, "hasNext", [])
  end

  @spec next(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def next(graph) do
    enqueue(graph, "next", [])
  end

  def emit(graph) do
    enqueue(graph, "emit", [])
  end

  def emit(graph, args) do
    enqueue(graph, "emit", [args])
  end

  def as(graph, label) do
    enqueue(graph, "as", [label])
  end

  def in_node(graph, label) do
    enqueue(graph, "in", [label])
  end

  def has_id(id) do
    {"hasId", [id]}
  end

  @spec next(Gremlex.Graph.t(), number()) :: Gremlex.Graph.t()
  def next(graph, num_results) do
    enqueue(graph, "next", [num_results])
  end

  def select(graph, label) do
    enqueue(graph, "select", [label])
  end

  def limit(graph, num_results) do
    enqueue(graph, "limit", [num_results])
  end

  @spec try_next(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def try_next(graph) do
    enqueue(graph, "tryNext", [])
  end

  @spec to_list(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def to_list(graph) do
    enqueue(graph, "toList", [])
  end

  @spec to_set(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def to_set(graph) do
    enqueue(graph, "toSet", [])
  end

  @spec to_bulk_set(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def to_bulk_set(graph) do
    enqueue(graph, "toBulkSet", [])
  end

  @spec drop(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def drop(graph) do
    enqueue(graph, "drop", [])
  end

  @spec iterate(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def iterate(graph) do
    enqueue(graph, "iterate", [])
  end

  defp enqueue(graph, op, args) do
    Queue.in({op, args}, graph)
  end

  @doc """
  Appends values the `E` command allowing you to select an edge.
  Returns a graph to allow chaining.
  """
  @spec e(Gremlex.Graph.t()) :: Gremlex.Graph.t()
  def e(graph) do
    enqueue(graph, "E", [])
  end

  @spec e(Gremlex.Graph.t(), number) :: Gremlex.Graph.t()
  def e(graph, id) when is_number(id) do
    enqueue(graph, "E", [id])
  end

  @doc """
  Compiles a graph into the Gremlin query.
  """
  @spec encode(Gremlex.Graph.t()) :: String.t()
  def encode(graph) do
    encode(graph, "g")
  end

  defp encode({[], []}, acc), do: acc

  defp encode(graph, acc) do
    {{:value, {op, args}}, remainder} = :queue.out(graph)

    # IO.inspect op, label: "op"
    # IO.inspect args, label: "args"
    # IO.inspect acc, label: "acc"

    args =
      case args do
        {inner_args_tail, inner_args_head} = inner_graph when is_list(inner_args_tail) and is_list(inner_args_head) ->
          encode(inner_graph, "")

        {inner_op, inner_args} ->
          args =
            inner_args
            |> Enum.map(&convert_arg/1)
            |> Enum.join(", ")

          "#{inner_op}(#{args})"

        args when is_list(args) ->
          args
          |> Enum.map(&convert_arg/1)
          |> Enum.join(", ")

        nil ->
          nil
      end

    encoded_op =
      case args do
        nil ->
          op
        args ->
          ".#{op}(#{args})"
      end

    encode(remainder, acc <> encoded_op)
  end

  def convert_arg(%Gremlex.Vertex{id: id}) do
    "V(#{id})"
  end

  def convert_arg(arg) when is_number(arg) do
    "#{arg}"
  end

  def convert_arg({op, args}) do
    args =
      args
      |> Enum.map(&convert_arg/1)
      |> Enum.join(", ")

    "#{op}(#{args})"
  end

  def convert_arg(arg) do
    # IO.inspect arg, label: "convert_arg/1::arg:"
    "'#{arg}'"
  end
end
