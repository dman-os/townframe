use crate::client::NodeClient;
use crate::interlude::*;

use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::GetDrawer;
use big_cabinet_core::drawer::protocol::ListDocs;
use big_cabinet_core::drawer::protocol::ListKnownDrawers;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cabinet_core::reference::DatumKey;
use big_cartridges_core::protocol::ListCartridgeGrants;
use big_cartridges_core::protocol::ListCartridgeInstalls;
use big_nodule_core::protocol::ListNodules;
use std::collections::BTreeSet;

/// A raw debug view of what the node holds: pretty JSON on stdout, unpolished on purpose.
///
/// Every command reads through the node's existing methods as the operator, minting the
/// narrowest handle each read needs. What no method exposes is absent from the output, not
/// guessed at: the node's own local drawers (present, originated, config) are not listed by
/// any method, and drawers that are not on the known-drawers list are unreachable to `node`
/// (name them to `drawer`).
#[derive(Debug, clap::Subcommand)]
pub enum DumpCommands {
    /// Everything: node info, installed cartridges and their grants, and every known drawer with
    /// its documents and their datums
    Node,
    /// One drawer: its descriptor, members, documents and their datums
    Drawer {
        /// The drawer's id
        drawer: String,
    },
    /// One document's datums, or one datum by key
    Doc {
        /// The document's id
        doc: String,
        /// A datum key, such as `note/main`
        key: Option<String>,
    },
    /// What cartridges may do: every grant of every cartridge
    Grants,
    /// Installed cartridges and their nodules
    Carts,
}

pub async fn run(command: DumpCommands) -> Res<ExitCode> {
    let client = lazy::node().await?;
    let dumped = match command {
        DumpCommands::Node => node(&client).await?,
        DumpCommands::Drawer { drawer } => drawer_dump(&client, &parse_doc(&drawer)?).await?,
        DumpCommands::Doc { doc, key } => {
            let key = key
                .map(|key| {
                    key.parse::<DatumKey>()
                        .map_err(|error| eyre::eyre!("`{key}` is not a datum key: {error}"))
                })
                .transpose()?;
            let doc = parse_doc(&doc)?;
            let capability = mint_read(&client, [doc.clone()]).await?;
            read_datums(
                &client,
                capability,
                doc,
                key.map(|key| BTreeSet::from([key])),
            )
            .await?
        }
        DumpCommands::Grants => grants(&client).await?,
        DumpCommands::Carts => carts(&client).await?,
    };
    println!("{}", serde_json::to_string_pretty(&dumped)?);
    Ok(ExitCode::SUCCESS)
}

fn parse_doc(doc: &str) -> Res<DocId> {
    doc.parse()
        .map_err(|error| eyre::eyre!("`{doc}` is not a document or drawer id: {error}"))
}

/// Everything the operator reaches. A drawer that cannot be read is reported in its place in the
/// output, so one unsynced drawer does not hide the rest.
async fn node(client: &NodeClient) -> Res<serde_json::Value> {
    let info = super::status::node_info(client).await?;
    let known = client
        .call(ListKnownDrawers {
            capability: super::drawers::known_drawers_handle(client).await?,
        })
        .await
        .wrap_err("listing the known drawers failed")?;
    let mut drawers = serde_json::Map::new();
    for drawer in &known.drawers {
        let dump = match drawer_dump(client, &drawer.drawer_id).await {
            Ok(dump) => dump,
            Err(error) => json!({ "error": format!("{error:#}") }),
        };
        drawers.insert(
            drawer.drawer_id.to_string(),
            json!({ "mirror": drawer.mirror, "dump": dump }),
        );
    }
    Ok(json!({
        "info": info,
        "carts": carts(client).await?,
        "grants": grants(client).await?,
        "drawers": drawers,
    }))
}

/// The drawer's view and roster, and the datums of its documents and of the drawer's own
/// document, read with one handle. A document that cannot be read is reported in its place.
async fn drawer_dump(client: &NodeClient, drawer: &DocId) -> Res<serde_json::Value> {
    let capability = client
        .call(MintDrawerCapability {
            scope: DrawerScope {
                drawers: [(drawer.clone(), DrawerAccess::Admin)].into(),
                create: false,
            },
            rights: Rights::USE,
            subject: None,
        })
        .await
        .wrap_err("minting a handle on the drawer failed")?
        .capability_id;
    let view = client
        .call(GetDrawer {
            capability,
            drawer_id: drawer.clone(),
        })
        .await
        .wrap_err("reading the drawer failed")?;
    let listed = client
        .call(ListDocs {
            capability,
            drawer_id: drawer.clone(),
            offset: 0,
            limit: None,
        })
        .await
        .wrap_err("listing the drawer's documents failed")?;

    let ids: Vec<DocId> = listed
        .docs
        .iter()
        .map(|doc| doc.doc_id.clone())
        .chain([drawer.clone()])
        .collect();
    let read_capability = mint_read(client, ids.iter().cloned()).await?;
    let mut datums = serde_json::Map::new();
    for id in ids {
        let read = match read_datums(client, read_capability, id.clone(), None).await {
            Ok(read) => read,
            Err(error) => json!({ "error": format!("{error:#}") }),
        };
        datums.insert(id.to_string(), read);
    }
    Ok(json!({
        "drawerId": drawer,
        "view": view,
        "docs": listed.docs,
        "datums": datums,
    }))
}

/// A handle that reads every datum kind of `docs`, which is as much as a dump asks for.
async fn mint_read(client: &NodeClient, docs: impl IntoIterator<Item = DocId>) -> Res<CapId> {
    Ok(client
        .call(MintDocCapability {
            scope: DocScope {
                read: DocScope::read_all(docs),
                ..default()
            },
            rights: Rights::USE,
            subject: None,
        })
        .await
        .wrap_err("minting a read handle failed")?
        .capability_id)
}

async fn read_datums(
    client: &NodeClient,
    capability: CapId,
    doc_id: DocId,
    datums: Option<BTreeSet<DatumKey>>,
) -> Res<serde_json::Value> {
    let read = client
        .call(ReadDatums {
            capability,
            doc_id,
            at: At::Current,
            datums,
            optimistic: false,
        })
        .await
        .wrap_err("reading the document's datums failed")?;
    Ok(serde_json::to_value(read)?)
}

async fn grants(client: &NodeClient) -> Res<serde_json::Value> {
    let grants = client
        .call(ListCartridgeGrants { cartridge_id: None })
        .await
        .wrap_err("listing the grants failed")?;
    Ok(serde_json::to_value(grants)?)
}

async fn carts(client: &NodeClient) -> Res<serde_json::Value> {
    let installs = client
        .call(ListCartridgeInstalls {})
        .await
        .wrap_err("listing the installs failed")?;
    let nodules = client
        .call(ListNodules {})
        .await
        .wrap_err("listing the nodules failed")?;
    Ok(json!({ "installs": installs, "nodules": nodules }))
}
