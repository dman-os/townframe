use super::*;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::drawer::DrawerAccess as Access;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::CreateLayout;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

#[expect(
    clippy::too_many_arguments,
    reason = "each argument is a distinct boot dependency handed to the blob workers; a params struct would only hide the wiring"
)]
pub(super) async fn boot(
    sql: Database,
    blobs: &big_blobs::Blobs,
    docs: &DocsRepo,
    drawers: &Drawers,
    access: &Arc<DrawerAccess>,
    repo: &SharedBigRepo,
    datum_set: Arc<big_cabinet::index::DatumSetStore>,
    signer: CapId,
) -> Res<(
    big_blobs::workers::BlobWorkers,
    DocId,
    service_utils_rs::StopToken,
)> {
    let context = RequestContext::root();
    let drawer_cap = drawers
        .client
        .call(
            context.child(),
            MintDrawerCapability {
                authorization: Authorization::identity(signer),
                scope: DrawerScope {
                    create: true,
                    drawers: BTreeMap::new(),
                },
                rights: Rights::USE,
            },
        )
        .await?;
    let drawer = drawers
        .client
        .call(
            context.child(),
            CreateDrawer {
                capability: drawer_cap.capability_id,
                command: "big-blobs-inventory-drawer-v1".to_owned().try_into()?,
                layout: CreateLayout::SingleGroup,
            },
        )
        .await?;
    // The node-local inventory is a drawer meta doc (ADR 026 §5): DrawerService
    // creates it, grants the meta group, and writes the slot; the workers' fallback
    // pins land there for docs no drawer claims an inventory for.
    let meta_cap = drawers
        .client
        .call(
            context.child(),
            MintDrawerCapability {
                authorization: Authorization::identity(signer),
                scope: DrawerScope {
                    create: false,
                    drawers: BTreeMap::from([(drawer.drawer_id.clone(), Access::Admin)]),
                },
                rights: Rights::USE,
            },
        )
        .await?;
    let inventory = drawers
        .client
        .call(
            context.child(),
            big_cabinet_core::drawer::protocol::CreateMetaDoc {
                capability: meta_cap.capability_id,
                command: "big-blobs-inventory-document-v1".to_owned().try_into()?,
                drawer_id: drawer.drawer_id.clone(),
                kind: big_blobs::workers::INVENTORY_KIND.to_owned(),
            },
        )
        .await?;
    let inventory_doc = inventory.doc_id;
    let local = drawers.local();
    let capability = docs
        .client
        .call(
            context,
            MintDocCapability {
                authorization: Authorization::identity(signer),
                scope: DocScope {
                    read: BTreeSet::from([inventory_doc.clone()]),
                    write: BTreeSet::from([inventory_doc.clone()]),
                    drawers: BTreeMap::from([
                        (local.present_id.clone(), Access::Read),
                        (local.originated_id.clone(), Access::Write),
                    ]),
                    ..Default::default()
                },
                rights: Rights::USE,
            },
        )
        .await?;
    let parts: big_repo::SharedPartStore = Arc::new(
        big_sync::SqlitePartStore::new(
            sql.clone(),
            "big-blobs-v1",
            big_sync_core::BuckId::MAX_LEVEL,
        )
        .await?,
    );
    let (workers, stop) = big_blobs::workers::boot(big_blobs::workers::WorkerConfig {
        sql,
        service: Arc::clone(&blobs.service),
        docs: docs.client.clone(),
        capability: capability.capability_id,
        inventory: inventory_doc,
        drawers: vec![local.present_id.clone(), local.originated_id.clone()],
        datum_set,
        parts,
        repo: Arc::clone(repo),
        policy: access
            .subscribe_changes()
            .expect("drawer access publishes changes"),
    })
    .await?;
    Ok((workers, drawer.drawer_id, stop))
}
