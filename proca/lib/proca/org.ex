defmodule Proca.Org do
  @moduledoc """
  Represents an organisation in Proca. `Org` can have many `Staffers`, `Campaigns` and `ActionPage`'s.

  Org can have one or more `PublicKey`'s. Only one of them is active at a particular time. Others are expired.
  """
  use Ecto.Schema
  use Proca.Schema, module: __MODULE__
  import Ecto.Changeset
  import Ecto.Query
  alias Proca.Org
  alias Proca.Service.EmailTemplateDirectory

  schema "orgs" do
    field :name, :string
    field :title, :string
    has_many :public_keys, Proca.PublicKey, on_delete: :delete_all
    has_many :staffers, Proca.Staffer, on_delete: :delete_all
    has_many :campaigns, Proca.Campaign, on_delete: :nilify_all
    # XXX
    has_many :action_pages, Proca.ActionPage, on_delete: :nilify_all

    field :contact_schema, ContactSchema, default: :basic
    field :action_schema_version, :integer, default: 2

    # avoid storing transient data in clear
    # XXX rename to a more adequate :strict_privacy
    # XXX also maybe move to campaign level
    field :high_security, :boolean, default: false

    # services and delivery options
    has_many :services, Proca.Service, on_delete: :delete_all
    belongs_to :email_backend, Proca.Service
    field :email_from, :string
    belongs_to :template_backend, Proca.Service

    # double opt in configuration
    # XXX maybe move these two into config
    field :email_opt_in, :boolean, default: false
    field :email_opt_in_template, :string

    # confirming and delivery configuration
    field :custom_supporter_confirm, :boolean
    field :custom_action_confirm, :boolean
    field :custom_action_deliver, :boolean
    field :system_sqs_deliver, :boolean

    field :config, :map

    timestamps()
  end

  @doc false
  def changeset(org, attrs) do
    org
    |> cast(attrs, [
      :name, :title, 
      :contact_schema, 
      :email_from,
      :email_opt_in, :email_opt_in_template, 
      :config, 
      :high_security
    ])
    |> validate_required([:name, :title])
    |> validate_format(:name, ~r/^[[:alnum:]_-]+$/)
    |> unique_constraint(:name)
    |> Proca.Contact.Input.validate_email(:email_from)
    |> validate_change(:email_opt_in_template, fn f, tmpl_name ->
      case EmailTemplateDirectory.ref_by_name_reload(org, tmpl_name) do 
        {:ok, _ref} -> []
        :not_found -> [{f, "template not found"}]
        :not_configured -> [{f, "templating not configured"}]
      end
    end)
    |> cast_email_backend(org, attrs)
  end

  def cast_email_backend(chset, org, %{email_backend: srv_name}) 
    when srv_name in [:mailjet, :ses] do 
    case Proca.Service.get_one_for_org(srv_name, org) do 
      nil -> add_error(chset, :email_backend, "no such service")
      %{id: id} -> chset 
        |> put_change(:email_backend_id, id)
        |> put_change(:template_backend_id, id)
    end
  end

  def cast_email_backend(ch, _org, %{email_backend: srv_name}) 
    when is_atom(srv_name) do 
      add_error(ch, :email_backend, "service does not support email")
  end

  def cast_email_backend(ch, _org, _a), do: ch 



  def all(q, [{:name, name} | kw]), do: where(q, [o], o.name == ^name) |> all(kw) 
  def all(q, [:instance | kw]), do: all(q, [{:name, instance_org_name()} | kw]) 
  def all(q, [{:id, id} | kw]), do: where(q, [o], o.id == ^id) |> all(kw) 

  def all(q, [:active_public_keys | kw]) do 
    q
    |> join(:left, [o], k in assoc(o, :public_keys), 
      on: k.active)
    |> order_by([o, k], asc: k.inserted_at)
    |> preload([o, k], [public_keys: k])
    |> all(kw)
  end

  def get_by_name(name, preload \\ []) do
    {preload, select_active_keys} =
      if Enum.member?(preload, :active_public_keys) do
        {
          [:public_keys | List.delete(preload, :active_public_keys)],
          true
        }
      else
        {preload, false}
      end

    q = from o in Proca.Org, where: o.name == ^name, preload: ^preload
    org = Proca.Repo.one(q)

    if not is_nil(org) and select_active_keys do
      %{
        org
        | public_keys:
            org.public_keys
            |> Enum.filter(& &1.active)
            |> Enum.sort(fn a, b -> a.inserted_at > b.inserted_at end)
      }
    else
      org
    end
  end

  def get_by_id(id, preload \\ []) do
    Proca.Repo.one(from o in Proca.Org, where: o.id == ^id, preload: ^preload)
  end

  def instance_org_name do
    Application.get_env(:proca, Proca)[:org_name]
  end

  def list(preloads \\ []) do
    all([preload: preloads])
  end

  @spec active_public_keys([Proca.PublicKey]) :: [Proca.PublicKey]
  def active_public_keys(public_keys) do
    public_keys
    |> Enum.filter(& &1.active)
    |> Enum.sort(fn a, b -> a.inserted_at < b.inserted_at end)
  end

  @spec active_public_keys(Proca.Org) :: Proca.PublicKey | nil
  def active_public_key(org) do
    Proca.Repo.one from(pk in Ecto.assoc(org, :public_keys), order_by: [asc: pk.id], limit: 1)
  end


  def put_service(%Org{} = org, service), do: put_service(change(org), service)

  def put_service(%Ecto.Changeset{} = ch, %Proca.Service{name: name} = service)
    when name in [:mailjet, :testmail]
    do
    ch
    |> put_assoc(:email_backend, service)
    |> put_assoc(:template_backend, service)
  end
end
