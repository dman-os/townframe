//! intxt's operations, run as the operator until intxt's nodule can run them itself (ledger D41).
//!
//! Creating an intxt creates a drawer, and a nodule cannot create a drawer yet: the nodule service
//! grants a nodule's actor access only to drawers that already exist. Once the operator can
//! consent to a nodule creating drawers, `new` becomes intxt's `createIntxt` operation, called with
//! `day x`, and this module goes. `install` goes once cartridges are installed through `day` in
//! general.

use crate::client::NodeClient;
use crate::interlude::*;

use big_cabinet_core::DatumKey;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::CreateLayout;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_nodule_core::protocol::AdmitNodule;
use big_nodule_core::protocol::SetNoduleEnabled;
use intxt_core::generic;
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

/// `value` at its record's `main` slot, tagged with the record's `$type`.
fn datum<T: DatumMember + Serialize>(value: &T) -> Res<(DatumKey, serde_json::Value)> {
    let mut json = serde_json::to_value(value)?;
    json["$type"] = T::ID.into();
    Ok((format!("{}/main", T::ID).parse()?, json))
}

/// A command id no earlier write used: the node deduplicates writes by command id.
fn command(kind: &str) -> CommandId {
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
