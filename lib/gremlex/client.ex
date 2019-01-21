defmodule Gremlex.Client do
  @moduledoc """
  Gremlin Websocket Client
  """

  @type state :: %{socket: Socket.Web.t()}

  @type response ::
          {:ok, list()}
          | {:error, :unauthorized, String.t()}
          | {:error, :malformed_request, String.t()}
          | {:error, :invalid_request_arguments, String.t()}
          | {:error, :server_error, String.t()}
          | {:error, :script_evaluation_error, String.t()}
          | {:error, :server_timeout, String.t()}
          | {:error, :server_serialization_error, String.t()}

  require Logger
  alias Gremlex.Request
  alias Gremlex.Deserializer
  use Retry

  @spec start_link({String.t(), number(), String.t(), boolean()}) :: pid()
  def start_link({host, port, path, secure}) do
    case Socket.Web.connect(host, port, path: path, secure: secure) do
      {:ok, socket} ->
        state = %{socket: socket, host: host, port: port, path: path, secure: secure}
        GenServer.start_link(__MODULE__, state, [])

      error ->
        Logger.error("Error establishing connection to server: #{inspect(error)}")
        state = %{socket: %{}, host: host, port: port, path: path, secure: secure}
        GenServer.start_link(__MODULE__, state, [])
    end
  end

  @spec init(Socket.Web.t()) :: state
  def init(state) do
    GenServer.cast(self(), :monitor)
    {:ok, state}
  end

  # Public Methods

  @doc """
  Accepts a graph which it converts into a query and queries the database.

  Params:
  * query - A `Gremlex.Graph.t` or raw String query
  * timeout (Default: 5000ms) - Timeout in milliseconds to pass to GenServer and Task.await call
  """
  @spec query(Gremlex.Graph.t() | String.t(), number() | :infinity) :: Response
  def query(query, timeout \\ 5000) do
    payload =
      query
      |> Request.new()
      |> Poison.encode!()

    :poolboy.transaction(:gremlex, fn worker_pid ->
      GenServer.call(worker_pid, {:query, payload, timeout}, timeout)
    end)
  end

  # Server Methods
  @spec handle_call({:query, String.t(), number() | :infinity}, pid(), state) ::
          {:reply, response, state}
  def handle_call({:query, payload, timeout}, _from, %{socket: socket} = state) do
    Socket.Web.send!(socket, {:text, payload})

    task = Task.async(fn -> recv(socket) end)
    result = Task.await(task, timeout)

    {:reply, result, state}
  end

  def handle_cast(:monitor, %{socket: socket, host: host, path: path, port: port, secure: secure} = state) do
    case Socket.Web.recv!(socket) do
      {:ping, _} ->
        Socket.Web.send!(socket, {:pong, ""})
        GenServer.cast(self(), :monitor)
        {:noreply, state}
      {:close, _, _} ->
        backoff = exponential_backoff() |> cap(10_000)
        socket =
          retry with: backoff do
            Socket.Web.connect(host, port, path: path, secure: secure)
          after
            {:ok, socket} -> socket
          else
            _ -> %{}
          end
        state = %{state | socket: socket}
        GenServer.cast(self(), :monitor)
        {:noreply, state}
    end
  end

  # Private Methods
  @spec recv(Socket.Web.t(), list()) :: response
  defp recv(socket, acc \\ []) do
    case Socket.Web.recv!(socket) do
      {:close, _, _} ->
        {:ok, []}
      {:text, data} ->
        response = Poison.decode!(data)
        result = Deserializer.deserialize(response)
        status = response["status"]["code"]
        error_message = response["status"]["message"]
        # Continue to block until we receive a 200 status code
        case status do
          200 ->
            {:ok, acc ++ result}

          204 ->
            {:ok, []}

          206 ->
            recv(socket, acc ++ result)

          401 ->
            {:error, :unauthorized, error_message}

          409 ->
            {:error, :malformed_request, error_message}

          499 ->
            {:error, :invalid_request_arguments, error_message}

          500 ->
            {:error, :server_error, error_message}

          597 ->
            {:error, :script_evaluation_error, error_message}

          598 ->
            {:error, :server_timeout, error_message}

          599 ->
            {:error, :server_serialization_error, error_message}
        end

      {:ping, _} ->
        # Keep the connection alive
        Socket.Web.send!(socket, {:pong, ""})
        recv(socket, acc)
    end
  end
end
