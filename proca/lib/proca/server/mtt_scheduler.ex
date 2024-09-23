defmodule Proca.Server.MTTScheduler do
  @moduledoc """
    Server which runs `Proca.Server.MTT` every 30 seconds or more.
  """

  use GenServer

  import Ecto.Query

  alias Proca.Repo
  alias Proca.Campaign
  alias Proca.MTT

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
    goal = Time.utc_now() |> hourly_goal()

    fetch_messages()
    |> Enum.group_by(& &1.target_id)
    |> Enum.map(fn {target_id, messages_batch} -> {target_id, Enum.take(messages_batch, goal)} end)
    |> Enum.each(fn {target_id, messages_batch} ->
      start_processor(target_id, messages_batch)
    end)
  end

  defp start_processor(target_id, messages_batch) do
    Proca.Server.MTTProcessor.start_link(target_id, messages_batch)
  end

  defp fetch_messages() do
    running_mtts =
      from(c in Campaign,
        join: mtt in MTT,
        on: mtt.campaign_id == c.id,
        where: mtt.start_at <= from_now(0, "day") and mtt.end_at >= from_now(0, "day"),
        preload: [:mtt]
      )
      |> Repo.all()

    sendable_target_ids =
      from(t in Proca.Target,
        join: c in assoc(t, :campaign),
        join: te in assoc(t, :emails),
        where: c.id in ^running_mtts and c.id == ^id and te.email_status in [:active, :none],
        distinct: t.id,
        select: t.id
      )
      |> Repo.all()

    Proca.Action.Message.select_by_targets(sendable_target_ids, false, false)
    |> Repo.all()
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
