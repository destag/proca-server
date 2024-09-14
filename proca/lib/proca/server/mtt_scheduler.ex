defmodule Proca.Server.MTTScheduler do
  @moduledoc """
    Server which runs `Proca.Server.MTT` every 30 seconds or more.
  """

  use GenServer

  # @schedule_interval :timer.hours(1)
  @schedule_interval :timer.seconds(60)

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(state) do
    Process.flag(:trap_exit, true)
    :timer.seconds(10) |> schedule_work()
    {:ok, state}
  end

  @impl true
  def handle_info(:process_messages, state) do
    process_messages()
    schedule_work()
    {:noreply, state}
  end

  @impl true
  def handle_info({:EXIT, _pid, _reason}, state) do
    {:noreply, state}
  end

  defp schedule_work(interval \\ @schedule_interval) do
    Process.send_after(self(), :process_messages, interval)
  end

  defp process_messages() do
    Time.utc_now() |> hourly_goal() |> dbg()

    fetch_messages()
    |> Enum.group_by(& &1.target_id)
    |> Enum.each(fn {target_id, messages_batch} ->
      start_processor(target_id, messages_batch)
    end)
  end

  defp start_processor(target_id, messages_batch) do
    Proca.Server.MTTProcessor.start_link(target_id, messages_batch)
  end

  defp fetch_messages() do
    # Repo.all(from m in Message, select: m)
    [
      %{
        target_id: 1,
        content: "hello"
      },
      %{
        target_id: 2,
        content: "hello"
      },
      %{
        target_id: 1,
        content: "hello w"
      },
      %{
        target_id: 1,
        content: "msg1"
      },
      %{
        target_id: 1,
        content: "msg2"
      },
      %{
        target_id: 1,
        content: "msg3"
      },
      %{
        target_id: 1,
        content: "msg4"
      },
      %{
        target_id: 2,
        content: "hello w"
      },
      %{
        target_id: 313,
        content: "hello wo"
      },
      %{
        target_id: 1,
        content: "nue"
      }
    ]
  end

  @spec hourly_goal(Time.t()) :: integer()
  defp hourly_goal(time) do
    case time.hour do
      0 -> 2
      1 -> 3
      2 -> 4
      3 -> 5
      4 -> 6
      5 -> 10
      6 -> 15
      7 -> 25
      8 -> 50
      9 -> 80
      10 -> 100
      11 -> 90
      12 -> 70
      13 -> 70
      14 -> 90
      15 -> 100
      16 -> 90
      17 -> 70
      18 -> 50
      19 -> 25
      20 -> 20
      21 -> 10
      22 -> 5
      23 -> 2
      _ -> 0
    end
  end
end
