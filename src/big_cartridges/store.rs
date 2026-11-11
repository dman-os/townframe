//! Host-local SQLite state for the registry: the id of the registry drawer this node created, and
//! the node's first-seen record of every published version.
//!
//! The registry itself is derived from the drawer and is rebuilt from it on every boot. The
//! first-seen record is not derivable: it is what the node saw first for a version label, which
//! is what lets it keep serving that content after a synced write changes the datum.

use crate::sqlite;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Revision;
use big_cartridges_core::Version;
use big_cartridges_core::protocol::CartridgesError;
use sqlite_rs::Database;

/// A version label's first-seen content.
#[derive(Debug, Clone)]
pub(crate) struct FirstSeen {
    pub(crate) revision: Revision,
    pub(crate) envelope: Cartridge,
}

fn storage(error: impl std::fmt::Display) -> CartridgesError {
    CartridgesError::Storage(error.to_string())
}

pub(crate) async fn migrate(database: &Database) -> Result<(), CartridgesError> {
    database
        .apply_migrations(sqlite::MIGRATION_TABLE, sqlite::MIGRATIONS)
        .await
        .map_err(storage)
}

pub(crate) async fn registry_drawer(
    database: &Database,
) -> Result<Option<DrawerId>, CartridgesError> {
    let transaction = database.begin_read().await.map_err(storage)?;
    let drawer = sqlite::registry_drawer(&transaction)
        .await
        .map_err(storage)?;
    Ok(drawer.map(|drawer| {
        drawer
            .parse()
            .expect("the registry drawer id this node stored is a doc id")
    }))
}

pub(crate) async fn set_registry_drawer(
    database: &Database,
    drawer: &DrawerId,
) -> Result<(), CartridgesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::set_registry_drawer(&transaction, drawer.to_string())
        .await
        .map_err(storage)?;
    transaction.commit().await.map_err(storage)
}

pub(crate) async fn first_seen(
    database: &Database,
) -> Result<Vec<(CartridgeId, Version, FirstSeen)>, CartridgesError> {
    let transaction = database.begin_read().await.map_err(storage)?;
    let rows = sqlite::first_seen(&transaction).await.map_err(storage)?;
    // Every column was written from a validated value by `upsert_first_seen`, so a row that does
    // not parse back is a corrupt database, not an input to recover from.
    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.cartridge_id
                    .parse()
                    .expect("stored cartridge id is valid"),
                row.version.parse().expect("stored version is valid"),
                FirstSeen {
                    revision: row.revision.parse().expect("stored revision is valid"),
                    envelope: serde_json::from_str(&row.envelope_json)
                        .expect("stored envelope is valid"),
                },
            )
        })
        .collect())
}

pub(crate) async fn upsert_first_seen(
    database: &Database,
    envelope: &Cartridge,
) -> Result<(), CartridgesError> {
    let transaction = database.begin_write().await.map_err(storage)?;
    sqlite::upsert_first_seen(
        &transaction,
        envelope.id.to_string(),
        envelope.version.to_string(),
        envelope.revision().to_string(),
        serde_json::to_string(envelope).expect("an envelope serializes"),
    )
    .await
    .map_err(storage)?;
    transaction.commit().await.map_err(storage)
}
