//! intxt's operations, run as the operator until intxt's nodule can run them itself (ledger D41).
//!
//! Creating an intxt creates a drawer, and a nodule cannot create a drawer yet: the nodule service
//! grants a nodule's actor access only to drawers that already exist. Once the operator can
//! consent to a nodule creating drawers, `new` becomes intxt's `createIntxt` operation, called with
//! `day x`, and this module goes. `add` follows it into the nodule once the nodule can be lent
//! the reach to enter a document in a drawer. `install` goes once cartridges are installed through
//! `day` in general.

use crate::client::NodeClient;
use crate::interlude::*;

use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::AddDoc;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::CreateLayout;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cabinet_core::reference::DatumReference;
use big_cabinet_core::reference::ReferenceDocId;
use big_nodule_core::protocol::AdmitNodule;
use big_nodule_core::protocol::SetNoduleEnabled;
use intxt_core::generic;
use intxt_core::work;
use method_core::Method;

#[derive(Debug, clap::Subcommand)]
pub enum IntxtCommands {
    /// Admit intxt's cartridge to this node and enable it, which registers intxt's records
    Install,
    /// Create an intxt: a drawer marked as one, with a title and an empty body
    ///
    /// Prints the new drawer's id. Entries (worktrees, sessions, repositories) are added to it
    /// later, as datums on the drawer's descriptor document.
    New {
        /// The intxt's title
        title: String,
        /// A longer description, in Markdown
        #[arg(long)]
        description: Option<String>,
    },
    /// Include a captured part in an intxt: a worktree, a repository or an agent session
    ///
    /// Writes the part's entry datum on the intxt's descriptor, appends it to the intxt's body,
    /// and enters the part's document in the intxt's drawer so its members can read it.
    Add {
        /// The intxt's drawer id
        intxt: String,
        /// The part: a `dtxt:datum:` reference to a `worktree`, `gitRepo` or `agentSession`
        /// datum
        part: String,
        /// A drawer the part's document is already in, which lends the access the intxt's
        /// drawer passes on to its members
        #[arg(long)]
        from: String,
    },
}

pub async fn run(command: IntxtCommands) -> Res<ExitCode> {
    let client = lazy::node().await?;
    match command {
        IntxtCommands::Install => {
            install(&client).await?;
            println!("intxt is installed");
        }
        IntxtCommands::New { title, description } => {
            println!("{}", create(&client, title, description).await?);
        }
        IntxtCommands::Add { intxt, part, from } => {
            let intxt: DrawerId = intxt
                .parse()
                .map_err(|error| eyre::eyre!("`{intxt}` is not a drawer id: {error}"))?;
            let part: DatumReference = part
                .parse()
                .map_err(|error| eyre::eyre!("`{part}` is not a datum reference: {error}"))?;
            let from: DrawerId = from
                .parse()
                .map_err(|error| eyre::eyre!("`{from}` is not a drawer id: {error}"))?;
            let entry = add(&client, &intxt, &part, &from).await?;
            println!("{entry}");
        }
    }
    Ok(ExitCode::SUCCESS)
}

pub(crate) async fn install(client: &NodeClient) -> Res<()> {
    let cartridge = intxt_nodule::cartridge();
    let cartridge_id = cartridge.id.clone();
    call(
        client,
        AdmitNodule {
            cartridge,
            drawers: None,
        },
    )
    .await?;
    call(
        client,
        SetNoduleEnabled {
            cartridge_id,
            enabled: true,
        },
    )
    .await?;
    Ok(())
}

/// Creates the drawer, then writes the descriptor datums of ADR 036 §2 onto its descriptor
/// document: the `intxt` marker, the title, and an empty `body`. Two steps, so a failure between
/// them leaves a plain drawer, which a reader does not show as an intxt.
pub(crate) async fn create(
    client: &NodeClient,
    title: String,
    description: Option<String>,
) -> Res<DrawerId> {
    let drawers = call(
        client,
        MintDrawerCapability {
            scope: DrawerScope {
                drawers: Default::default(),
                create: true,
            },
            rights: Rights::USE,
            subject: None,
        },
    )
    .await?
    .capability_id;
    let drawer_id = call(
        client,
        CreateDrawer {
            capability: drawers,
            command: command("intxt-new"),
            layout: CreateLayout::SingleGroup,
        },
    )
    .await?
    .drawer_id;
    let descriptor = call(
        client,
        MintDocCapability {
            scope: DocScope {
                write: [drawer_id.clone()].into(),
                ..DocScope::default()
            },
            rights: Rights::USE,
            subject: None,
        },
    )
    .await?
    .capability_id;
    call(
        client,
        WriteDatums {
            capability: descriptor,
            command: Some(command("intxt-descriptor")),
            doc_id: drawer_id.clone(),
            mode: WriteMode::AtCurrent,
            set: [
                datum(&generic::Intxt::default())?,
                datum(&generic::Title {
                    title,
                    description,
                    extra: Default::default(),
                })?,
                datum(&generic::Body {
                    order: Vec::new(),
                    extra: Default::default(),
                })?,
            ]
            .into(),
            remove: Default::default(),
        },
    )
    .await?;
    Ok(drawer_id)
}

/// Writes the entry for `part` and appends it to the body in one write, based on the heads the
/// body was read at, so two writers adding different parts both land in the body's list; then
/// enters the part's document in the intxt's drawer at Read. The entry's slot is the part's own
/// identity (ADR 040: the worktree's UUID, the repository datum's slot, the harness's session id),
/// so two writers adding the same part write one entry. Answers the entry's reference.
pub(crate) async fn add(
    client: &NodeClient,
    intxt: &DrawerId,
    part: &DatumReference,
    from: &DrawerId,
) -> Res<DatumReference> {
    let ReferenceDocId::Concrete(part_doc) = part.document() else {
        eyre::bail!("a part is named by its document id, not `self`");
    };
    let part_value = read(client, part_doc, part.key()).await?;
    let tag = part.key().tag().to_string();
    let (entry_key, entry) = if tag == work::Worktree::ID {
        let worktree: work::Worktree = serde_json::from_value(part_value)?;
        datum_at(
            &worktree.id,
            &work::WorktreeEntry {
                worktree: part.clone(),
                extra: Default::default(),
            },
        )?
    } else if tag == work::GitRepo::ID {
        datum_at(
            &String::from_utf8(part.key().slot_bytes().to_vec())?,
            &work::RepoEntry {
                repo: part.clone(),
                extra: Default::default(),
            },
        )?
    } else if tag == work::AgentSession::ID {
        let session: work::AgentSession = serde_json::from_value(part_value)?;
        datum_at(
            &session.session_id,
            &work::SessionEntry {
                session: part.clone(),
                extra: Default::default(),
            },
        )?
    } else {
        eyre::bail!(
            "`{tag}` is not a part an intxt includes: name a worktree, gitRepo or agentSession datum"
        );
    };

    let body_key: DatumKey = format!("{}/main", generic::Body::ID).parse()?;
    let (body_heads, body_value) = read_at(client, intxt, &body_key).await?;
    let mut body: generic::Body = serde_json::from_value(body_value)?;
    let entry_ref = DatumReference::new(ReferenceDocId::SelfDoc, entry_key.clone());
    if !body.order.contains(&entry_ref) {
        body.order.push(entry_ref.clone());
    }
    let descriptor = doc_capability(
        client,
        DocScope {
            write: [intxt.clone()].into(),
            ..DocScope::default()
        },
    )
    .await?;
    call(
        client,
        WriteDatums {
            capability: descriptor,
            command: Some(command("intxt-add")),
            doc_id: intxt.clone(),
            mode: WriteMode::AtHeads { heads: body_heads },
            set: [(entry_key, entry), datum(&body)?].into(),
            remove: Default::default(),
        },
    )
    .await?;

    let drawers = call(
        client,
        MintDrawerCapability {
            scope: DrawerScope {
                drawers: [
                    (intxt.clone(), DrawerAccess::Write),
                    (from.clone(), DrawerAccess::Read),
                ]
                .into(),
                create: false,
            },
            rights: Rights::USE,
            subject: None,
        },
    )
    .await?
    .capability_id;
    call(
        client,
        AddDoc {
            capability: drawers,
            reach: drawers,
            command: command("intxt-add-doc"),
            drawer_id: intxt.clone(),
            doc_id: part_doc.clone(),
            access: DrawerAccess::Read,
        },
    )
    .await?;
    Ok(DatumReference::new(
        ReferenceDocId::Concrete(intxt.clone()),
        entry_ref.key().clone(),
    ))
}

pub(crate) async fn doc_capability(client: &NodeClient, scope: DocScope) -> Res<CapId> {
    Ok(call(
        client,
        MintDocCapability {
            scope,
            rights: Rights::USE,
            subject: None,
        },
    )
    .await?
    .capability_id)
}

/// The accepted value of `key` in `doc` now.
async fn read(client: &NodeClient, doc: &DocId, key: &DatumKey) -> Res<serde_json::Value> {
    Ok(read_at(client, doc, key).await?.1)
}

/// The document's heads and the accepted value of `key` in it at those heads.
pub(crate) async fn read_at(
    client: &NodeClient,
    doc: &DocId,
    key: &DatumKey,
) -> Res<(Vec<big_cabinet_core::doc::ChangeHash>, serde_json::Value)> {
    let capability = doc_capability(
        client,
        DocScope {
            read: [doc.clone()].into(),
            ..DocScope::default()
        },
    )
    .await?;
    let mut read = call(
        client,
        ReadDatums {
            capability,
            doc_id: doc.clone(),
            at: At::Current,
            datums: Some([key.clone()].into()),
            optimistic: false,
        },
    )
    .await?;
    match read.datums.remove(key) {
        Some(DatumRead::Accepted { value, .. }) => Ok((read.heads, value)),
        other => eyre::bail!("{key} in {doc} is not readable: {other:?}"),
    }
}

/// `value` at its record's `main` slot; the key names the type, so the value carries no tag.
fn datum<T: DatumMember + Serialize>(value: &T) -> Res<(DatumKey, serde_json::Value)> {
    datum_at("main", value)
}

/// `value` at `slot` of its record; the key names the type, so the value carries no tag.
fn datum_at<T: DatumMember + Serialize>(
    slot: &str,
    value: &T,
) -> Res<(DatumKey, serde_json::Value)> {
    Ok((
        DatumKey::new(T::ID, slot.as_bytes())?,
        serde_json::to_value(value)?,
    ))
}

/// A command id no earlier write used: the node deduplicates writes by command id.
pub(crate) fn command(kind: &str) -> CommandId {
    let nonce: [u8; 16] = rand::random();
    CommandId::try_from(format!("{kind}-{}", data_encoding::HEXLOWER.encode(&nonce)))
        .expect("a kind and a hex nonce make a command id")
}

/// Calls `input`'s operation and decodes its answer; a non-success answer is the operation's own
/// error, reported as the node gave it.
pub(crate) async fn call<M>(client: &NodeClient, input: M) -> Res<M::Output>
where
    M: Method,
    M::Output: serde::de::DeserializeOwned,
{
    let reply = client
        .call(M::XRPC.nsid, M::XRPC.method, &serde_json::to_value(input)?)
        .await?;
    eyre::ensure!(
        reply.is_success(),
        "{} failed with {}: {}",
        M::XRPC.nsid,
        reply.status,
        reply.body
    );
    Ok(serde_json::from_value(reply.body)?)
}
