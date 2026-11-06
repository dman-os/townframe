use super::*;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::drawer::DrawerAccess as Access;
use big_cabinet_core::drawer::protocol::DrawerId;
use service_client_native_rs::Client;
use std::collections::BTreeMap;

#[expect(
    clippy::too_many_arguments,
    reason = "each argument is a distinct boot dependency handed to the blob workers; a params struct would only hide the wiring"
)]
pub(super) async fn boot(
    sql: Database,
    blobs: &big_blobs::Blobs,
    docs: &DocsRepo,
    drawers: &Drawers,
    drawers_actor: &Actor,
    plugs_actor: &Actor,
    access: &Arc<DrawerAccess>,
    repo: &SharedBigRepo,
    scope_key: Arc<str>,
    datum_set: Arc<big_cabinet::index::DatumSetStore>,
    stop: &mut StopToken,
) -> Res<(big_blobs::workers::BlobWorkers, DrawerId)> {
    let signer = drawers_actor.capability;
    let registry_drawer =
        ensure_registry_drawer(drawers, drawers_actor, plugs_actor, access, sql.clone()).await?;
    let local = drawers.local();
    let capability = docs
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(signer).metadata(),
            MintDocCapability {
                scope: DocScope {
                    drawers: BTreeMap::from([
                        (local.present_id.clone(), Access::Read),
                        (local.originated_id.clone(), Access::Write),
                        // The config drawer is a self-doc drawer: the nodule service keeps its
                        // handler artifacts as `Blob` datums in the drawer's own document, so
                        // the workers retain them and write their cipherBlob/jwk representation
                        // there like in any other document they maintain.
                        (local.config_id.clone(), Access::Write),
                        // The plug registry's grouped drawer: its documents carry the `Blob`
                        // datums for plug artifacts.
                        (registry_drawer.clone(), Access::Write),
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
    let (workers, workers_stop) = big_blobs::workers::boot(big_blobs::workers::WorkerConfig {
        metadata_observer: drawers.metadata_observer(),
        metadata_capability: metadata_capability(sql.clone(), docs.client.clone(), signer),
        sql,
        service: Arc::clone(&blobs.service),
        docs: docs.client.clone(),
        capability: capability.capability_id,
        drawers: vec![
            local.present_id.clone(),
            local.originated_id.clone(),
            local.config_id.clone(),
            registry_drawer.clone(),
        ],
        datum_set,
        parts,
        repo: Arc::clone(repo),
        scope_key,
        policy: access
            .subscribe_changes()
            .expect("drawer access publishes changes"),
    })
    .await?;
    stop.stages.push(Stage::Service(workers_stop));
    Ok((workers, registry_drawer))
}

/// Creates the plug registry drawer on the first boot (owned by the drawers actor, which is also
/// the identity the blob workers sign as) and grants the plugs actor admin over it.
async fn ensure_registry_drawer(
    drawers: &Drawers,
    drawers_actor: &Actor,
    plugs_actor: &Actor,
    access: &DrawerAccess,
    sql: Database,
) -> Res<DrawerId> {
    let drawer = big_plugs::ensure_registry_drawer(RegistryDrawerConfig {
        drawers,
        actor: drawers_actor,
        access,
        sql,
    })
    .await?;
    grant_drawer_admin(drawers, drawers_actor, plugs_actor, &drawer).await?;
    Ok(drawer)
}

pub(super) fn metadata_capability(
    sql: Database,
    docs: Client<DocsProtocol>,
    signer: CapId,
) -> big_blobs::workers::MetadataCapability {
    Arc::new(move |scope| {
        let sql = sql.clone();
        let docs = docs.clone();
        Box::pin(async move {
            if !scope.create.is_empty()
                || !scope.drawers.is_empty()
                || (scope.read.is_empty() && scope.write.is_empty())
            {
                return Err(service_utils_rs::ClientError::Operation(DocsError::Denied));
            }
            for document in scope.read.union(&scope.write) {
                let known = sql.query(
                    "SELECT EXISTS (
                         SELECT 1 FROM big_cabinet_drawers
                          WHERE (drawer_id = ?1 OR json_extract(descriptor_json, '$.layout.sensitiveRoot') = ?1)
                            AND json_extract(descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
                         UNION ALL
                         SELECT 1 FROM big_cabinet_drawer_meta_docs WHERE doc_id = ?1
                     )",
                    (document.to_string(),),
                ).await
                    .map_err(|error| service_utils_rs::ClientError::Operation(DocsError::Repository(error.to_string())))?
                    .one::<bool>()
                    .map_err(|error| service_utils_rs::ClientError::Operation(DocsError::Repository(error.to_string())))?;
                if !known {
                    return Err(service_utils_rs::ClientError::Operation(DocsError::Denied));
                }
            }
            docs.call_with(
                RequestContext::root(),
                Authorization::identity(signer).metadata(),
                MintDocCapability {
                    scope,
                    rights: Rights::USE,
                },
            )
            .await
            .map(|issued| issued.capability_id)
        })
    })
}
