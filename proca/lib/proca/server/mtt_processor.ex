defmodule Proca.Server.MTTProcessor do
  @moduledoc """
    Server which processes MTT messages.
  """
  alias Proca.Pipes.Connection

  defmodule State do
    @moduledoc false
    defstruct target_id: nil, messages: [], start_time: nil, interval: nil
  end

  use GenServer

  @max_interval :timer.seconds(60)

  def start_link(target_id, messages) do
    GenServer.start_link(__MODULE__, {target_id, messages})
  end

  @impl true
  def init({tid, messages}) do
    schedule_work(:timer.seconds(1))

    {:ok,
     %State{
       target_id: tid,
       messages: messages,
       start_time: DateTime.utc_now(),
       interval: div(@max_interval, length(messages) + 1)
     }}
  end

  @impl true
  def handle_info(:work, %State{messages: []} = state) do
    {:stop, :normal, state}
  end

  @impl true
  def handle_info(:work, %State{messages: [next_message | rest]} = state) do
    IO.puts(
      "message from worker: #{state.target_id} at #{DateTime.utc_now()}: #{inspect(next_message)}"
    )

    Connection.publish(
      %{message: next_message, target_id: state.target_id},
      "org.1.send",
      "mailjet"
    )

    if DateTime.diff(DateTime.utc_now(), state.start_time, :minute) < 60 do
      schedule_work(state.interval)
      {:noreply, %State{state | messages: rest}}
    else
      {:stop, :normal, state}
    end
  end

  defp schedule_work(interval) do
    Process.send_after(self(), :work, interval)
  end
end
