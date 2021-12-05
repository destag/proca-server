defmodule Proca.Supporter do
  @moduledoc """
  Supporter is the actor that does actions.
  Has associated contacts, which contain personal data dediacted to every receiving org
  """
  use Ecto.Schema
  use Proca.Schema, module: __MODULE__
  alias Proca.Repo
  alias Proca.{Supporter, Contact, ActionPage, Org, Action}
  alias Proca.Contact.Data
  alias Proca.Supporter.Privacy
  import Ecto.Changeset
  import Ecto.Query

  schema "supporters" do
    has_many :contacts, Proca.Contact

    belongs_to :campaign, Proca.Campaign
    belongs_to :action_page, Proca.ActionPage
    belongs_to :source, Proca.Source

    field :fingerprint, :binary
    has_many :actions, Proca.Action

    field :first_name, :string
    field :email, :string
    field :area, :string

    field :processing_status, ProcessingStatus, default: :new
    field :email_status, EmailStatus, default: :none

    timestamps()
  end

  @doc false
  def changeset(supporter, attrs) do
    supporter
    |> cast(attrs, [])
    |> validate_required([])
  end

  def new_supporter(data, action_page = %ActionPage{}) do
    %Supporter{}
    |> change(Map.take(data, Supporter.Privacy.cleartext_fields(action_page)))
    |> change(%{fingerprint: Data.fingerprint(data)})
    |> put_assoc(:campaign, action_page.campaign)
    |> put_assoc(:action_page, action_page)
  end

  # @spec add_contacts(Ecto.Changeset.t(Supporter), Ecto.Changeset.t(Contact), %ActionPage{}, %Privacy{}) :: Ecto.Changeset.t(Supporter)
  def add_contacts(
    new_supporter = %Ecto.Changeset{},
    new_contact = %Ecto.Changeset{},
    action_page = %ActionPage{},
    privacy = %Privacy{}
  ) do

    consents = Privacy.consents(action_page, privacy)
    contacts = Contact.spread(new_contact, consents)

    new_supporter
    |> put_assoc(:contacts, contacts)
  end

  def confirm(sup = %Supporter{}) do 
    case sup.processing_status do 
      :new -> {:error, "not allowed"}
      :confirming -> Repo.update(change(sup, processing_status: :accepted))
      :rejected -> {:error, "supporter data already rejected"}
      :accepted -> {:noop, "supporter data already processed"}
      :delivered -> {:noop, "supporter data already processed"}
    end
  end

  def reject(sup = %Supporter{}) do 
    case sup.processing_status do 
      :new -> {:error, "not allowed"}
      :confirming -> Repo.update(change(sup, processing_status: :rejected))
      :rejected -> {:noop, "supporter data already rejected"}
      :accepted -> {:noop, "supporter data already processed"}
      :delivered -> {:error, "supporter data already processed"}
    end
  end

  def privacy_defaults(p = %{opt_in: _opt_in, lead_opt_in: _lead_opt_in}) do
    p
  end

  def privacy_defaults(p = %{opt_in: _opt_in}) do
    Map.put(p, :lead_opt_in, false)
  end

  @doc "Returns %Supporter{} or nil"
  def find_by_fingerprint(fingerprint, org_id) do
    query =
      from(s in Supporter,
        join: ap in ActionPage,
        on: s.action_page_id == ap.id,
        where: ap.org_id == ^org_id and s.fingerprint == ^fingerprint,
        order_by: [desc: :inserted_at],
        limit: 1,
        preload: [:contacts]
      )

    Repo.one(query)
  end

  def base_encode(data) when is_bitstring(data) do
    Base.url_encode64(data, padding: false)
  end

  def base_decode(encoded) when is_bitstring(encoded) do
    Base.url_decode64(encoded, padding: false)
  end

  def decode_ref(changeset = %Ecto.Changeset{}, field) do 
    case get_change(changeset, field) do 
      nil -> changeset
      base -> case base_decode(base) do 
        {:ok, val} -> put_change(changeset, field, val)
        :error -> add_error(changeset, field, "Cannot decode from Base64url")
      end
    end
  end

  def handle_bounce(args) do
    supporter = get_by_action_id(args.id)
    reject(supporter)

    supporter = change(supporter, email_status: args.reason)

    Repo.update!(supporter)
  end

  def get_by_action_id(action_id) do
    query = from(
      s in Supporter,
      join: a in Action,
      on: a.supporter_id == s.id,
      where: a.id == ^action_id,
      order_by: [desc: :inserted_at],
      limit: 1
    )

    Repo.one(query)
  end


# XXX rename this to something like "clear_transient_fields"
  def clear_transient_fields_query(supporter) do
    clear_fields = Supporter.Privacy.transient_supporter_fields(supporter.action_page)
    |> Enum.map(fn f -> {f, nil} end)

    from(s in Supporter,
      where: s.id == ^supporter.id,
      update: [set: ^clear_fields]
    )
  end
end
