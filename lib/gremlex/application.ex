defmodule Gremlex.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  defp parse_port(port) when is_number(port), do: port

  defp parse_port(""), do: 8182

  defp parse_port(port_string) when is_binary(port_string) do
    case Integer.parse(port_string) do
      {port, ""} ->
        port
      _ ->
        raise ArgumentError, message: "Invalid Port: #{port_string}"
    end
  end

  def start(_type, _args) do
    # List all child processes to be supervised
    host = Confex.fetch_env!(:gremlex, :host)
    port =
      :gremlex
      |> Confex.fetch_env!(:port)
      |> parse_port()
    path = Confex.fetch_env!(:gremlex, :path)
    pool_size = Confex.fetch_env!(:gremlex, :pool_size)

    pool_options = [
      name: {:local, :gremlex},
      worker_module: Gremlex.Client,
      size: pool_size,
      max_overflow: 10
    ]

    children = [
      :poolboy.child_spec(:gremlex, pool_options, {host, port, path})
      # Starts a worker by calling: Gremlex.Worker.start_link(arg)
      # {Gremlex.Worker, arg},
    ]

    # case :hackney_pool.start_pool(:gremlex_pool, [timeout: 15000, max_connections: pool_size]) do
    #   :ok ->
    #     nil
    #   :error ->
    #     raise "Cannot create HTTP request pool"
    # end

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Gremlex.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
