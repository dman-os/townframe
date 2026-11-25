//! Host-local SQLite state for [`crate::NoduleService`]: durable actor bindings and applied
//! service-owned drawer grant bookkeeping.
//!
//! Portable desired configuration (the `CartridgeInstall` pins in the node's config drawer
//! document, and the settings and grants in each cartridge's config document) lives in the
//! documents; SQLite stores only the id of the shared config drawer this node created, the host's `(cartridge_id, actor_uuid)`
//! identity binding, the `(cartridge_id, drawer_id) -> access` grants this service has applied, and the
//! `cartridge_id -> (scope, capability)` dispatch capability minted from each nodule's operation grants,
//! the `(cartridge_id, init key) -> revision` a [`Once`](big_nodule_core::RunMode::Once)
//! initializer last succeeded at on this node, and the `(cartridge_id, binding) -> provider` each
//! operation request resolved to (the resolution lockfile). The lockfile is host-local, not a
//! config datum: it records which providers *this* node has, which a node mirroring the config
//! document must not inherit (config syncs for backup and activates nothing elsewhere), and the
//! operator's consent to a provider is node-specific.

use crate::sqlite;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Revision;
use big_dispatch_core::DispatchScope;
use big_nodule_core::protocol::NodulesError;
use big_nodule_core::providers::LockedProvider;
use sqlite_rs::Database;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub(crate) struct ActorRow {
    pub(crate) cartridge_id: CartridgeId,
    pub(crate) actor_id: Uuid7,
}

#[derive(Debug, Clone)]
pub(crate) struct AppliedGrantRow {
    pub(crate) cartridge_id: CartridgeId,
    pub(crate) drawer_id: DrawerId,
    pub(crate) access: DrawerAccess,
    pub(crate) capability_id: CapId,
}

#[derive(Debug, Clone)]
pub(crate) struct AppliedOperationsRow {
    pub(crate) cartridge_id: CartridgeId,
    pub(crate) scope: DispatchScope,
    pub(crate) capability_id: CapId,
}

fn storage(error: impl std::fmt::Display) -> NodulesError {
    NodulesError::Storage(error.to_string())
}

pub(crate) async fn migrate(database: &Database) -> Result<(), NodulesError> {
    database
        .apply_migrations(sqlite::MIGRATION_TABLE, sqlite::MIGRATIONS)
        .await
        .map_err(storage)
}

/// The one path segment a cartridge's actor lives at below the nodules actor. A cartridge id cannot be
/// the segment itself: it holds `/` and `:`, and an actor segment is only ASCII letters, digits,
/// `-`, `_` and `.`. The name keeps it readable in actor URLs and the 128-bit blake3 prefix of
/// the full id keeps it unique across namespaces, so the segment is derived and never stored.
pub(crate) fn actor_segment(cartridge_id: &CartridgeId) -> String {
    let digest = blake3::hash(cartridge_id.to_string().as_bytes()).to_hex();
    format!("{}-{}", cartridge_id.name, &digest.as_str()[..32])
}

pub(crate) async fn actors(database: &Database) -> Result<Vec<ActorRow>, NodulesError> {
    let transaction = database.begin_read().await.map_err(storage)?;
    let rows = sqlite::actors(&transaction).await.map_err(storage)?;
    rows.into_iter()
        .map(|row| {
            let cartridge_id: CartridgeId = row.cartridge_id.parse().map_err(|error| {
                storage(format!(
                    "actor row cartridge_id `{}` is invalid: {error}",
                    row.cartridge_id
                ))
            })?;
            let actor_uuid: utils_rs::prelude::uuid::Uuid =
                row.actor_uuid.parse().map_err(|_| {
                    storage(format!(
                        "actor `{}` of `{}` is not a uuid",
                        row.actor_uuid, row.cartridge_id
                    ))
                })?;
            Ok(ActorRow {
                cartridge_id,
                actor_id: Uuid7::try_from(actor_uuid).map_err(storage)?,
            })
        })
        .collect()
}

pub(crate) async fn upsert_actor(
    database: &Database,
    cartridge_id: &CartridgeId,
    actor_id: Uuid7,
) -> Result<(), NodulesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::upsert_actor(&transaction, cartridge_id.to_string(), actor_id.to_string())
        .await
        .map_err(storage)?;
    transaction.commit().await.map_err(storage)
}

pub(crate) async fn applied_grants(
    database: &Database,
) -> Result<Vec<AppliedGrantRow>, NodulesError> {
    let transaction = database.begin_read().await.map_err(storage)?;
    let rows = sqlite::applied_grants(&transaction)
        .await
        .map_err(storage)?;
    rows.into_iter()
        .map(|row| {
            let cartridge_id: CartridgeId = row.cartridge_id.parse().map_err(|error| {
                storage(format!(
                    "applied grant cartridge_id `{}` is invalid: {error}",
                    row.cartridge_id
                ))
            })?;
            let drawer_id: DrawerId = row.drawer_id.parse().map_err(|error| {
                storage(format!(
                    "applied grant drawer_id `{}` for `{}` is invalid: {error}",
                    row.drawer_id, row.cartridge_id
                ))
            })?;
            let access: DrawerAccess = row.access.parse().map_err(|error| {
                storage(format!(
                    "applied grant access `{}` for `{}` is invalid: {error}",
                    row.access, row.cartridge_id
                ))
            })?;
            let capability_bytes: [u8; 16] =
                row.capability_id.try_into().map_err(|bytes: Vec<u8>| {
                    storage(format!(
                        "applied grant capability_id for `{}` has length {}, expected 16",
                        row.cartridge_id,
                        bytes.len()
                    ))
                })?;
            Ok(AppliedGrantRow {
                cartridge_id,
                drawer_id,
                access,
                capability_id: CapId::from_bytes(capability_bytes),
            })
        })
        .collect()
}

pub(crate) async fn upsert_applied_grant(
    database: &Database,
    cartridge_id: &CartridgeId,
    drawer_id: &DrawerId,
    access: DrawerAccess,
    capability_id: CapId,
) -> Result<(), NodulesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::upsert_applied_grant(
        &transaction,
        cartridge_id.to_string(),
        drawer_id.to_string(),
        access.as_str().to_owned(),
        capability_id.as_bytes().to_vec(),
    )
    .await
    .map_err(storage)?;
    transaction.commit().await.map_err(storage)
}

pub(crate) async fn delete_applied_grant(
    database: &Database,
    cartridge_id: &CartridgeId,
    drawer_id: &DrawerId,
) -> Result<(), NodulesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::delete_applied_grant(
        &transaction,
        cartridge_id.to_string(),
        drawer_id.to_string(),
    )
    .await
    .map_err(storage)?;
    transaction.commit().await.map_err(storage)
}

pub(crate) async fn applied_operations(
    database: &Database,
) -> Result<Vec<AppliedOperationsRow>, NodulesError> {
    let transaction = database.begin_read().await.map_err(storage)?;
    let rows = sqlite::applied_operations(&transaction)
        .await
        .map_err(storage)?;
    rows.into_iter()
        .map(|row| {
            let cartridge_id: CartridgeId = row.cartridge_id.parse().map_err(|error| {
                storage(format!(
                    "applied operations cartridge_id `{}` is invalid: {error}",
                    row.cartridge_id
                ))
            })?;
            let scope: DispatchScope = serde_json::from_str(&row.scope).map_err(|error| {
                storage(format!(
                    "applied operations scope for `{}` is invalid: {error}",
                    row.cartridge_id
                ))
            })?;
            let capability_bytes: [u8; 16] =
                row.capability_id.try_into().map_err(|bytes: Vec<u8>| {
                    storage(format!(
                        "applied operations capability_id for `{}` has length {}, expected 16",
                        row.cartridge_id,
                        bytes.len()
                    ))
                })?;
            Ok(AppliedOperationsRow {
                cartridge_id,
                scope,
                capability_id: CapId::from_bytes(capability_bytes),
            })
        })
        .collect()
}

pub(crate) async fn upsert_applied_operations(
    database: &Database,
    cartridge_id: &CartridgeId,
    scope: &DispatchScope,
    capability_id: CapId,
) -> Result<(), NodulesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::upsert_applied_operations(
        &transaction,
        cartridge_id.to_string(),
        serde_json::to_string(scope).expect("a dispatch scope serializes"),
        capability_id.as_bytes().to_vec(),
    )
    .await
    .map_err(storage)?;
    transaction.commit().await.map_err(storage)
}

pub(crate) async fn delete_applied_operations(
    database: &Database,
    cartridge_id: &CartridgeId,
) -> Result<(), NodulesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::delete_applied_operations(&transaction, cartridge_id.to_string())
        .await
        .map_err(storage)?;
    transaction.commit().await.map_err(storage)
}

pub(crate) async fn init_done(
    database: &Database,
) -> Result<BTreeMap<CartridgeId, BTreeMap<String, Revision>>, NodulesError> {
    let transaction = database.begin_read().await.map_err(storage)?;
    let rows = sqlite::init_done(&transaction).await.map_err(storage)?;
    let mut done: BTreeMap<CartridgeId, BTreeMap<String, Revision>> = BTreeMap::new();
    for row in rows {
        let cartridge_id: CartridgeId = row.cartridge_id.parse().map_err(|error| {
            storage(format!(
                "init done cartridge_id `{}` is invalid: {error}",
                row.cartridge_id
            ))
        })?;
        let revision: Revision = row.revision.parse().map_err(|error| {
            storage(format!(
                "init done revision `{}` for `{}` is invalid: {error}",
                row.revision, row.cartridge_id
            ))
        })?;
        done.entry(cartridge_id)
            .or_default()
            .insert(row.init_key, revision);
    }
    Ok(done)
}

pub(crate) async fn upsert_init_done(
    database: &Database,
    cartridge_id: &CartridgeId,
    init_key: &str,
    revision: Revision,
) -> Result<(), NodulesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::upsert_init_done(
        &transaction,
        cartridge_id.to_string(),
        init_key.to_owned(),
        revision.to_string(),
    )
    .await
    .map_err(storage)?;
    transaction.commit().await.map_err(storage)
}

pub(crate) async fn delete_init_done(
    database: &Database,
    cartridge_id: &CartridgeId,
) -> Result<(), NodulesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::delete_init_done(&transaction, cartridge_id.to_string())
        .await
        .map_err(storage)?;
    transaction.commit().await.map_err(storage)
}

pub(crate) async fn resolution_locks(
    database: &Database,
) -> Result<BTreeMap<CartridgeId, BTreeMap<String, LockedProvider>>, NodulesError> {
    let transaction = database.begin_read().await.map_err(storage)?;
    let rows = sqlite::resolution_locks(&transaction)
        .await
        .map_err(storage)?;
    let mut locks: BTreeMap<CartridgeId, BTreeMap<String, LockedProvider>> = BTreeMap::new();
    for row in rows {
        {
            let cartridge_id: CartridgeId = row.cartridge_id.parse().map_err(|error| {
                storage(format!(
                    "resolution lock cartridge_id `{}` is invalid: {error}",
                    row.cartridge_id
                ))
            })?;
            let version = row
                .provider_version
                .map(|version| {
                    version.parse().map_err(|error| {
                        storage(format!(
                            "resolution lock version `{version}` for `{}` is invalid: {error}",
                            row.cartridge_id
                        ))
                    })
                })
                .transpose()?;
            let lexicon_revision = row
                .lexicon_revision
                .map(|revision| {
                    u32::try_from(revision).map_err(|error| {
                        storage(format!(
                            "resolution lock revision {revision} for `{}` is invalid: {error}",
                            row.cartridge_id
                        ))
                    })
                })
                .transpose()?;
            locks.entry(cartridge_id).or_default().insert(
                row.binding,
                LockedProvider {
                    contract: row.contract,
                    provider: row.provider,
                    version,
                    lexicon_revision,
                },
            );
        }
    }
    Ok(locks)
}

/// Replaces every resolution lock of `cartridge_id` with `locks` (by binding) in one transaction;
/// an empty map removes them.
pub(crate) async fn replace_resolution_locks(
    database: &Database,
    cartridge_id: &CartridgeId,
    locks: &BTreeMap<String, LockedProvider>,
) -> Result<(), NodulesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::delete_resolution_locks(&transaction, cartridge_id.to_string())
        .await
        .map_err(storage)?;
    for (binding, locked) in locks {
        sqlite::upsert_resolution_lock(
            &transaction,
            cartridge_id.to_string(),
            binding.clone(),
            locked.contract.clone(),
            locked.provider.clone(),
            locked.version.as_ref().map(ToString::to_string),
            locked.lexicon_revision.map(i64::from),
        )
        .await
        .map_err(storage)?;
    }
    transaction.commit().await.map_err(storage)
}

pub(crate) async fn shared_config_drawer(
    database: &Database,
) -> Result<Option<DrawerId>, NodulesError> {
    let transaction = database.begin_read().await.map_err(storage)?;
    let drawer = sqlite::shared_config_drawer(&transaction)
        .await
        .map_err(storage)?;
    Ok(drawer.map(|drawer| {
        drawer
            .parse()
            .expect("the shared config drawer id this node stored is a doc id")
    }))
}

pub(crate) async fn set_shared_config_drawer(
    database: &Database,
    drawer: &DrawerId,
) -> Result<(), NodulesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::set_shared_config_drawer(&transaction, drawer.to_string())
        .await
        .map_err(storage)?;
    transaction.commit().await.map_err(storage)
}

/// The config document this node created or found for each cartridge. A document is findable in
/// the shared config drawer by its identity datum, but that scan reads an index that trails the
/// documents, so the node keeps what it has already made or found: a reinstall never misses a
/// document of its own, and never makes a second.
pub(crate) async fn config_docs(
    database: &Database,
) -> Result<BTreeMap<CartridgeId, DocId>, NodulesError> {
    let transaction = database.begin_read().await.map_err(storage)?;
    let rows = sqlite::config_docs(&transaction).await.map_err(storage)?;
    rows.into_iter()
        .map(|row| {
            let cartridge_id: CartridgeId = row.cartridge_id.parse().map_err(|error| {
                storage(format!(
                    "config doc cartridge_id `{}` is invalid: {error}",
                    row.cartridge_id
                ))
            })?;
            let doc_id: DocId = row.doc_id.parse().map_err(|error| {
                storage(format!(
                    "config doc id `{}` of `{}` is invalid: {error}",
                    row.doc_id, row.cartridge_id
                ))
            })?;
            Ok((cartridge_id, doc_id))
        })
        .collect()
}

pub(crate) async fn upsert_config_doc(
    database: &Database,
    cartridge_id: &CartridgeId,
    doc_id: &DocId,
) -> Result<(), NodulesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::upsert_config_doc(&transaction, cartridge_id.to_string(), doc_id.to_string())
        .await
        .map_err(storage)?;
    transaction.commit().await.map_err(storage)
}
