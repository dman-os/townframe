//! Host-local SQLite state for [`crate::NoduleService`]: durable actor bindings and applied
//! service-owned drawer grant bookkeeping.
//!
//! Portable desired configuration (`NoduleInstall`, `NoduleSettings`, `NoduleGrants`) lives in the
//! node's config drawer document; SQLite stores only the host's `(plug_id, nodule_id, actor_uuid)`
//! identity binding, the `(plug_id, drawer_id) -> access` grants this service has applied, and the
//! `plug_id -> (scope, capability)` dispatch capability minted from each nodule's operation grants.

use crate::sqlite;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_nodule_core::dispatch::DispatchScope;
use big_nodule_core::protocol::NodulesError;
use big_plugs_core::PlugId;
use sqlite_rs::Database;

#[derive(Debug, Clone)]
pub(crate) struct ActorRow {
    pub(crate) plug_id: PlugId,
    pub(crate) nodule_id: String,
    pub(crate) actor_id: Uuid7,
}

#[derive(Debug, Clone)]
pub(crate) struct AppliedGrantRow {
    pub(crate) plug_id: PlugId,
    pub(crate) drawer_id: DrawerId,
    pub(crate) access: DrawerAccess,
    pub(crate) capability_id: CapId,
}

#[derive(Debug, Clone)]
pub(crate) struct AppliedOperationsRow {
    pub(crate) plug_id: PlugId,
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

pub(crate) async fn actors(database: &Database) -> Result<Vec<ActorRow>, NodulesError> {
    let transaction = database.begin_read().await.map_err(storage)?;
    let rows = sqlite::actors(&transaction).await.map_err(storage)?;
    rows.into_iter()
        .map(|row| {
            let plug_id: PlugId = row.plug_id.parse().map_err(|error| {
                storage(format!(
                    "actor row plug_id `{}` is invalid: {error}",
                    row.plug_id
                ))
            })?;
            let actor_uuid: utils_rs::prelude::uuid::Uuid =
                row.actor_uuid.parse().map_err(|_| {
                    storage(format!(
                        "actor `{}` of `{}` is not a uuid",
                        row.actor_uuid, row.plug_id
                    ))
                })?;
            Ok(ActorRow {
                plug_id,
                nodule_id: row.nodule_id,
                actor_id: Uuid7::try_from(actor_uuid).map_err(storage)?,
            })
        })
        .collect()
}

pub(crate) async fn upsert_actor(
    database: &Database,
    plug_id: &PlugId,
    nodule_id: &str,
    actor_id: Uuid7,
) -> Result<(), NodulesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::upsert_actor(
        &transaction,
        plug_id.to_string(),
        nodule_id.to_owned(),
        actor_id.to_string(),
    )
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
            let plug_id: PlugId = row.plug_id.parse().map_err(|error| {
                storage(format!(
                    "applied grant plug_id `{}` is invalid: {error}",
                    row.plug_id
                ))
            })?;
            let drawer_id: DrawerId = row.drawer_id.parse().map_err(|error| {
                storage(format!(
                    "applied grant drawer_id `{}` for `{}` is invalid: {error}",
                    row.drawer_id, row.plug_id
                ))
            })?;
            let access: DrawerAccess = row.access.parse().map_err(|error| {
                storage(format!(
                    "applied grant access `{}` for `{}` is invalid: {error}",
                    row.access, row.plug_id
                ))
            })?;
            let capability_bytes: [u8; 16] =
                row.capability_id.try_into().map_err(|bytes: Vec<u8>| {
                    storage(format!(
                        "applied grant capability_id for `{}` has length {}, expected 16",
                        row.plug_id,
                        bytes.len()
                    ))
                })?;
            Ok(AppliedGrantRow {
                plug_id,
                drawer_id,
                access,
                capability_id: CapId::from_bytes(capability_bytes),
            })
        })
        .collect()
}

pub(crate) async fn upsert_applied_grant(
    database: &Database,
    plug_id: &PlugId,
    drawer_id: &DrawerId,
    access: DrawerAccess,
    capability_id: CapId,
) -> Result<(), NodulesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::upsert_applied_grant(
        &transaction,
        plug_id.to_string(),
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
    plug_id: &PlugId,
    drawer_id: &DrawerId,
) -> Result<(), NodulesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::delete_applied_grant(&transaction, plug_id.to_string(), drawer_id.to_string())
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
            let plug_id: PlugId = row.plug_id.parse().map_err(|error| {
                storage(format!(
                    "applied operations plug_id `{}` is invalid: {error}",
                    row.plug_id
                ))
            })?;
            let scope: DispatchScope = serde_json::from_str(&row.scope).map_err(|error| {
                storage(format!(
                    "applied operations scope for `{}` is invalid: {error}",
                    row.plug_id
                ))
            })?;
            let capability_bytes: [u8; 16] =
                row.capability_id.try_into().map_err(|bytes: Vec<u8>| {
                    storage(format!(
                        "applied operations capability_id for `{}` has length {}, expected 16",
                        row.plug_id,
                        bytes.len()
                    ))
                })?;
            Ok(AppliedOperationsRow {
                plug_id,
                scope,
                capability_id: CapId::from_bytes(capability_bytes),
            })
        })
        .collect()
}

pub(crate) async fn upsert_applied_operations(
    database: &Database,
    plug_id: &PlugId,
    scope: &DispatchScope,
    capability_id: CapId,
) -> Result<(), NodulesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::upsert_applied_operations(
        &transaction,
        plug_id.to_string(),
        serde_json::to_string(scope).expect("a dispatch scope serializes"),
        capability_id.as_bytes().to_vec(),
    )
    .await
    .map_err(storage)?;
    transaction.commit().await.map_err(storage)
}

pub(crate) async fn delete_applied_operations(
    database: &Database,
    plug_id: &PlugId,
) -> Result<(), NodulesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::delete_applied_operations(&transaction, plug_id.to_string())
        .await
        .map_err(storage)?;
    transaction.commit().await.map_err(storage)
}
