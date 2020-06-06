defmodule Gremlex.Connection do
  @moduledoc """
  An expansion of the gremlex lib
  to implement the db_connection 
  protocol.
  """
  use DBConnection

  defmodule Error do
    defexception [:reason, :message]

    def exception(reason) do
      message = "error #{format_error(reason)}"
      %Error{reason: reason, message: message}
    end

    defp format_error(:disconnect) do
      "connection was closed"
    end

    defp format_error(x) do
      # IO.inspect x
      "Unknown error"
    end
  end

  defmodule Query do
    @moduledoc """
    Query is a proxy module
    to create queries that are
    handled by the Gremlex client internally.
    """
    defstruct [:kind]
  end

  defimpl DBConnection.Query, for: Gremlex.Client.Query do
    alias Gremlex.Client.Query

    # check if kind is valid and let handle other (encode, decode etc.) the rest
    def parse(%Query{kind: tag} = query, _noidea) when tag in [:send, recv], do: query

    # encode query in json format
    def encode(%Query{kind: :send} = query, data, _) do
      data
      |> Request.new()
      |> Poison.encode!()
    end

    # just let handle_execute do the work
    def encode(%Query{kind: :recv} = query, _opts, _iguessstate), do: query

    # have to find out how frames are built up
    def decode(_query, result, _opts) do
      # IO.inspect result
      result
    end
  end

  @impl true
  def handle_execute(%Query{kind: :send} = query, data, _opts, conn) do
    :gun.ws_send(conn, {:binary, data})
    {:ok, query, :ok, conn}
  end

  # TODO handle ws frames that are too big if
  # gun doesn't already
  @impl true
  def handle_execute(%Query{kind: :recv} = query, _opts, _, conn) do
    receive do
      {:gun_ws, _pid, _stream_ref, frame} ->
        case frame do
          :close -> {:disconnect, Error.exception(:disconnect), conn}
          {:close, _message} -> {:disconnect, Error.exception(:disconnect), conn}
          {:close, _status, _message} -> {:disconnect, Error.exception(:disconnect), conn}
          {:binary, data} -> {:ok, query, data, conn}
        end

      x ->
        {:error, Error.excepction(x), conn}
    end
  end

  @impl true
  @doc """
  Builds a connection to a gremlin graph db
  and adds it to the state.
  """
  @spec connect(%ConnOpts{}) :: {:ok, any()} :: {:error, any()}
  def connect(opts) do
    host = Keyword.fetch!(opts, :hostanme)
    port = String.to_integer(Keyword.fetch!(opts, :port))

    transport =
      case Keyword.fetch(opts, :secure) do
        "true" -> :tls
        _ -> tcp
      end

    path = Keyword.fetch!(opts, :path)
    {:ok, conn} = :gun.open(host, port, %{transport: transport})

    case :gun.await_up(conn) do
      {:ok, _} ->
        :gun.ws_upgrade(conn, path)

        receive do
          {:gun_upgrade, _, ["websocket"]} -> {:ok, conn}
          {:gun_error, _, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  @doc """
  Disconnects one worker from gremlex db.
  """
  @spec disconnect(error :: Exception.t(), state :: any()) :: :ok
  def disconnect(_, conn) do
    :gun.close(conn)
  end

  @doc """
  Retrieves the login credentials of a 
  user for database from environment.
  """
  @spec get_creds() :: {:ok, String.t(), String.t()} | :error
  defp get_creds() do
    username =
      case Gremlex.Application.get_env(:username) do
        :not_set -> :error
        x -> x
      end

    password =
      case Gremlex.Application.get_env(:username) do
        :not_set -> :error
        x -> x
      end

    cond do
      username == :error ->
        :error

      password == :error ->
        :error

      true ->
        {:ok, username, password}
    end
  end

  @doc """
  authenticate allows sasl based authentication
  with credentials from config on 401 response.
  """
  @spec authenticate(Socket.Web.t(), String.t()) :: response
  defp authenticate(socket, request_id) do
    content_type = '!application/vnd.gremlin-v2.0+json'
    creds = get_creds()

    case creds do
      :error ->
        {:error, :unauthorized, "Missing user credentials"}

      {username, password} ->
        sasl = ("\0" <> username <> "\0" <> password) |> Base.encode64()
        # this struct should be introduced to
        # Gremlex.Request (explicit requestId and variable ops)
        req =
          %{
            "requestId" => %{"@type" => "g:UUID", "@value" => request_id},
            "op" => "authentication",
            "processor" => "traversal",
            "args" => %{
              "sasl" => sasl
            }
          }
          |> Poison.encode!()

        req = content_type <> req
        Socket.Web.send!(socket, {:text, req})
        recv(socket)
    end
  end
end