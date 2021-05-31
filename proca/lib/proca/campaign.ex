defmodule Proca.Campaign do
  @moduledoc """
  Campaign represents a political goal and consists of many action pages. Belongs to one Org (so called "leader org").
  """

  use Ecto.Schema
  alias Proca.{Repo, Campaign, ActionPage}
  import Ecto.Changeset
  import Ecto.Query

  schema "campaigns" do
    field :name, :string
    field :external_id, :integer
    field :title, :string
    field :force_delivery, :boolean
    field :public_actions, {:array, :string}, default: []
    field :contact_schema, ContactSchema, default: :basic
    field :config, :map

    belongs_to :org, Proca.Org
    has_many :action_pages, Proca.ActionPage

    timestamps()
  end

  @doc false
  def changeset(campaign, attrs) do
    campaign
    |> cast(attrs, [:name, :title, :external_id, :config, :contact_schema])
    |> validate_required([:name, :title, :contact_schema])
    |> validate_format(:name, ~r/^([\w\d_-]+$)/)
    |> unique_constraint(:name)
  end

  def upsert(org, attrs = %{external_id: id}) when not is_nil(id) do
    (Repo.get_by(Campaign, external_id: id, org_id: org.id) || %Campaign{contact_schema: org.contact_schema})
    |> Campaign.changeset(attrs)
    |> put_change(:org_id, org.id)
  end

  def upsert(org, attrs = %{name: cname}) do
    (Repo.get_by(Campaign, name: cname, org_id: org.id) || %Campaign{contact_schema: org.contact_schema})
    |> Campaign.changeset(attrs)
    |> put_change(:org_id, org.id)
  end

  def select_by_org(org) do
    from(c in Campaign,
      left_join: ap in ActionPage,
      on: c.id == ap.campaign_id,
      where: ap.org_id == ^org.id or c.org_id == ^org.id
    )
    |> distinct(true)
  end

  def get_with_local_pages(campaign_id) when is_integer(campaign_id) do 
    from(c in Campaign, where: c.id == ^campaign_id,
      left_join: a in assoc(c, :action_pages),
      where: a.org_id == c.org_id,
      order_by: [desc: a.id],
      preload: [:org, action_pages: a])
    |> Repo.one()
  end

  def get_with_local_pages(campaign_name) when is_bitstring(campaign_name) do 
    from(c in Campaign, where: c.name == ^campaign_name,
      left_join: a in assoc(c, :action_pages),
      where: a.org_id == c.org_id,
      order_by: [desc: a.id],
      preload: [:org, action_pages: a])
    |> Repo.one()
  end
end
