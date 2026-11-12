use big_blobs_core::BlobId;
use big_cabinet_core::DocId;
use big_sync_core::PartKey;
use big_sync_core::PeerKey;
use sqlite_rs::Database;
use sqlite_rs::Tx;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::watch;
use utils_rs::prelude::Res;

mod coordinator;
mod discovery;
mod producers;
mod project;
mod runtime;
pub(crate) mod sqlite;

pub use discovery::Coverage;
pub use discovery::CoverageGap;
pub(crate) use discovery::DrawerSnapshot;
pub use discovery::InventorySelection;
pub use runtime::InventoryMirrors;

pub(crate) use coordinator::Admission;

#[derive(Clone)]
struct State {
    sql: Database,
    changed: Arc<Notify>,
    changes: watch::Sender<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Attempt {
    pub inventory: DocId,
    pub digest: BlobId,
    pub generation: u64,
    pub authorization: u64,
    pub peer: PeerKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PairState {
    pub selected: bool,
    pub writer: bool,
    pub pinned: bool,
    pub observed: bool,
    pub readable: bool,
    pub bytes: bool,
    pub generation: u64,
    pub authorization: u64,
    pub fetching: bool,
}

impl PairState {
    pub fn fetch_eligible(&self) -> bool {
        self.selected && self.pinned && self.observed && self.readable
    }

    pub fn advertisable(&self) -> bool {
        (self.writer || self.selected)
            && self.pinned
            && (self.writer || self.observed)
            && self.readable
            && self.bytes
    }

    pub fn retains_bytes(&self) -> bool {
        self.bytes && (self.writer || self.observed)
    }
}

impl State {
    pub async fn new(sql: Database) -> Res<Self> {
        sql.apply_migrations(
            "big_blobs_admission_migrations",
            sqlite::queries::MIGRATIONS,
        )
        .await?;
        let state = Self {
            sql,
            changed: Arc::new(Notify::new()),
            changes: watch::channel(0).0,
        };
        state.initialize_discovery().await?;
        Ok(state)
    }

    pub async fn register(&self, inventory: &DocId) -> Res<()> {
        let part = crate::workers::inventory_part(inventory);
        sqlite::queries::register_inventory(
            &self.sql,
            inventory.to_string(),
            part.as_bytes().to_vec(),
        )
        .await?;
        Ok(())
    }

    pub async fn inventory(&self, part: &PartKey) -> Res<Option<DocId>> {
        sqlite::queries::inventory_for_part(&self.sql, part.as_bytes().to_vec())
            .await?
            .into_iter()
            .next()
            .map(|id| Ok(id.parse()?))
            .transpose()
    }

    pub async fn recover(&self) -> Res<()> {
        let tx = self.sql.begin_write().await?;
        let inventories = sqlite::queries::recover_inventories(&tx)
            .await?
            .rows_affected;
        let pairs = sqlite::queries::recover_pairs(&tx).await?.rows_affected;
        tx.commit().await?;
        if inventories > 0 || pairs > 0 {
            self.changes.send_modify(|revision| *revision += 1);
            self.changed.notify_one();
        }
        Ok(())
    }

    pub async fn select(&self, inventory: &DocId, owner: &str, selected: bool) -> Res<()> {
        self.register(inventory).await?;
        let tx = self.sql.begin_write().await?;
        let exists = !sqlite::queries::claim_exists(&tx, inventory.to_string(), owner.to_owned())
            .await?
            .is_empty();
        if exists == selected {
            tx.rollback().await?;
            return Ok(());
        }
        if selected {
            sqlite::queries::insert_claim(&tx, inventory.to_string(), owner.to_owned()).await?;
        } else {
            sqlite::queries::delete_claim(&tx, inventory.to_string(), owner.to_owned()).await?;
        }
        sqlite::queries::rearm_inventory(&tx, inventory.to_string()).await?;
        tx.commit().await?;
        self.changes.send_modify(|revision| *revision += 1);
        self.changed.notify_one();
        Ok(())
    }

    pub async fn authorize(&self, inventory: &DocId, readable: bool) -> Res<()> {
        self.register(inventory).await?;
        let tx = self.sql.begin_write().await?;
        let previous = sqlite::queries::read_authorization(&tx, inventory.to_string())
            .await?
            .into_iter()
            .next()
            .expect("registered inventory has authorization state");

        if previous == readable {
            tx.rollback().await?;
            return Ok(());
        }
        sqlite::queries::update_authorization(&tx, readable, inventory.to_string()).await?;
        sqlite::queries::rearm_inventory(&tx, inventory.to_string()).await?;
        tx.commit().await?;
        self.changes.send_modify(|revision| *revision += 1);
        self.changed.notify_one();
        Ok(())
    }

    pub async fn pins(
        &self,
        inventory: &DocId,
        revision: u64,
        pins: &BTreeMap<BlobId, u64>,
        complete: bool,
    ) -> Res<()> {
        self.register(inventory).await?;
        let tx = self.sql.begin_write().await?;
        let previous = sqlite::queries::read_pin_state(&tx, inventory.to_string())
            .await?
            .into_iter()
            .next()
            .expect("registered inventory has pin state");
        let previously_complete = previous.pins_complete;
        let previous = u64::try_from(previous.pin_revision)?;
        if revision < previous || (revision == previous && (previously_complete || !complete)) {
            tx.rollback().await?;
            return Ok(());
        }
        if complete {
            let previous = sqlite::queries::pinned_digests(&tx, inventory.to_string()).await?;
            for digest in previous {
                if !pins.contains_key(&digest.parse()?) {
                    sqlite::queries::clear_pin(&tx, inventory.to_string(), digest).await?;
                }
            }
        }
        for (digest, length) in pins {
            sqlite::queries::upsert_pin(
                &tx,
                inventory.to_string(),
                digest.to_string(),
                i64::try_from(*length)?,
            )
            .await?;
        }
        sqlite::queries::update_pin_state(
            &tx,
            i64::try_from(revision)?,
            complete,
            inventory.to_string(),
        )
        .await?;
        tx.commit().await?;
        self.changes.send_modify(|revision| *revision += 1);
        self.changed.notify_one();
        Ok(())
    }

    pub async fn observe(
        &self,
        inventory: &DocId,
        digest: BlobId,
        peer: &PeerKey,
        _revision: u64,
        present: bool,
        payload: Option<&serde_json::Value>,
    ) -> Res<()> {
        self.register(inventory).await?;
        let tx = self.sql.begin_write().await?;
        let payload = payload.map(serde_json::to_string).transpose()?;
        let previous = sqlite::queries::observation(
            &tx,
            inventory.to_string(),
            digest.to_string(),
            peer.as_bytes().to_vec(),
        )
        .await?;
        let changed = previous.as_ref().is_none_or(|previous| {
            previous.present != present
                || payload
                    .as_ref()
                    .is_some_and(|payload| previous.payload.as_ref() != Some(payload))
        });
        if !changed {
            tx.rollback().await?;
            return Ok(());
        }
        sqlite::queries::upsert_observation(
            &tx,
            inventory.to_string(),
            digest.to_string(),
            peer.as_bytes().to_vec(),
            present,
            payload,
        )
        .await?;

        Self::touch(&tx, inventory, digest).await?;
        tx.commit().await?;
        self.changes.send_modify(|revision| *revision += 1);
        self.changed.notify_one();
        Ok(())
    }

    async fn touch(tx: &Tx, inventory: &DocId, digest: BlobId) -> Res<()> {
        sqlite::queries::touch_pair(tx, inventory.to_string(), digest.to_string()).await?;
        Ok(())
    }

    #[cfg(test)]
    pub async fn produce(
        &self,
        inventory: &DocId,
        digest: BlobId,
        owner: &str,
        active: bool,
    ) -> Res<()> {
        self.register(inventory).await?;
        let tx = self.sql.begin_write().await?;
        let exists = !sqlite::queries::producer_claim_exists(
            &tx,
            inventory.to_string(),
            digest.to_string(),
            owner.to_owned(),
        )
        .await?
        .is_empty();
        if exists == active {
            tx.rollback().await?;
            return Ok(());
        }
        if active {
            sqlite::queries::insert_producer_claim(
                &tx,
                inventory.to_string(),
                digest.to_string(),
                owner.to_owned(),
            )
            .await?;
        } else {
            sqlite::queries::delete_producer_claim(
                &tx,
                inventory.to_string(),
                digest.to_string(),
                owner.to_owned(),
            )
            .await?;
        }
        Self::touch(&tx, inventory, digest).await?;
        if active {
            sqlite::queries::mark_bytes(&tx, inventory.to_string(), digest.to_string()).await?;
        }
        tx.commit().await?;
        self.changes.send_modify(|revision| *revision += 1);
        self.changed.notify_one();
        Ok(())
    }

    async fn state_in(tx: &Tx, inventory: &DocId, digest: BlobId) -> Res<Option<PairState>> {
        Ok(
            sqlite::queries::state(tx, inventory.to_string(), digest.to_string())
                .await?
                .map(|row| PairState {
                    selected: row.selected,
                    writer: row.writer,
                    pinned: row.pinned,
                    observed: row.observed,
                    readable: row.readable,
                    bytes: row.bytes,
                    generation: u64::try_from(row.generation).expect("generation is non-negative"),
                    authorization: u64::try_from(row.authorization)
                        .expect("authorization is non-negative"),
                    fetching: row.fetching,
                }),
        )
    }

    pub async fn state(&self, inventory: &DocId, digest: BlobId) -> Res<Option<PairState>> {
        let tx = self.sql.begin_write().await?;
        let state = Self::state_in(&tx, inventory, digest).await?;
        tx.commit().await?;
        Ok(state)
    }

    pub async fn available(&self, inventory: &DocId, digest: BlobId, available: bool) -> Res<()> {
        let changed = sqlite::queries::set_available(
            &self.sql,
            available,
            inventory.to_string(),
            digest.to_string(),
            available,
        )
        .await?
        .rows_affected;
        if changed > 0 {
            self.changes.send_modify(|revision| *revision += 1);
            self.changed.notify_one();
        }
        Ok(())
    }

    pub async fn length(&self, inventory: &DocId, digest: BlobId) -> Res<Option<u64>> {
        Ok(
            sqlite::queries::length(&self.sql, inventory.to_string(), digest.to_string())
                .await?
                .map(u64::try_from)
                .transpose()?,
        )
    }

    pub async fn inventories(&self) -> Res<Vec<DocId>> {
        sqlite::queries::inventories(&self.sql)
            .await?
            .into_iter()
            .map(|id| Ok(id.parse()?))
            .collect()
    }

    pub async fn dirty(&self) -> Res<Vec<(DocId, BlobId, u64)>> {
        sqlite::queries::dirty(&self.sql)
            .await?
            .into_iter()
            .map(|row| {
                Ok((
                    row.inventory_id.parse()?,
                    row.digest.parse()?,
                    u64::try_from(row.generation)?,
                ))
            })
            .collect()
    }

    #[cfg(test)]
    pub async fn settle(&self, inventory: &DocId, digest: BlobId, generation: u64) -> Res<()> {
        let changed = sqlite::queries::settle(
            &self.sql,
            inventory.to_string(),
            digest.to_string(),
            i64::try_from(generation)?,
        )
        .await?
        .rows_affected;
        if changed > 0 {
            self.changes.send_modify(|revision| *revision += 1);
        }
        Ok(())
    }

    /// Publish the pair as the caller decided it at `generation`, and settle that generation.
    ///
    /// A pair that has moved past `generation` since is left as it is. Every move bumps the
    /// generation and marks the pair dirty, so the next round decides it again; settling the new
    /// generation here would mark a change clean that no decision (whether to fetch it, above all)
    /// ever saw, and nothing would wake for it again.
    pub(super) async fn publish(
        &self,
        inventory: &DocId,
        digest: BlobId,
        generation: u64,
        effect: impl FnOnce(PairState, Option<u64>) -> futures::future::BoxFuture<'static, Res<bool>>,
    ) -> Res<()> {
        let Some(pair) = self.state(inventory, digest).await? else {
            return Ok(());
        };
        if pair.generation != generation {
            return Ok(());
        }
        let generation = i64::try_from(generation)?;
        let length = self.length(inventory, digest).await?;
        let invalidated = sqlite::queries::invalidate_publish(
            &self.sql,
            inventory.to_string(),
            digest.to_string(),
            generation,
        )
        .await?
        .rows_affected;
        if invalidated > 0 {
            self.changes.send_modify(|revision| *revision += 1);
        }
        let available = effect(pair, length).await?;
        let settled = sqlite::queries::settle_publish(
            &self.sql,
            available,
            inventory.to_string(),
            digest.to_string(),
            generation,
        )
        .await?
        .rows_affected;
        assert_eq!(settled, 1, "publication is serialized with pair mutations");
        self.changes.send_modify(|revision| *revision += 1);
        Ok(())
    }

    pub async fn rearm(&self) -> Res<()> {
        let changed = sqlite::queries::rearm_pairs(&self.sql).await?.rows_affected;

        if changed > 0 {
            self.changes.send_modify(|revision| *revision += 1);
            self.changed.notify_one();
        }
        Ok(())
    }

    pub async fn fail(&self, attempt: &Attempt) -> Res<()> {
        let changed = sqlite::queries::fail_attempt(
            &self.sql,
            attempt.inventory.to_string(),
            attempt.digest.to_string(),
            i64::try_from(attempt.generation)?,
            i64::try_from(attempt.generation)?,
        )
        .await?
        .rows_affected;
        if changed > 0 {
            self.changes.send_modify(|revision| *revision += 1);
        }
        Ok(())
    }

    #[cfg(test)]
    pub async fn begin(&self, inventory: &DocId, digest: BlobId) -> Res<Option<Attempt>> {
        self.begin_from(inventory, digest, None).await
    }

    pub async fn begin_from(
        &self,
        inventory: &DocId,
        digest: BlobId,
        peers: Option<&std::collections::BTreeSet<PeerKey>>,
    ) -> Res<Option<Attempt>> {
        let tx = self.sql.begin_write().await?;
        let Some(state) = Self::state_in(&tx, inventory, digest).await? else {
            tx.rollback().await?;
            return Ok(None);
        };
        if !state.fetch_eligible() || state.bytes || state.fetching {
            tx.rollback().await?;
            return Ok(None);
        }
        let sources =
            sqlite::queries::source_peers(&tx, inventory.to_string(), digest.to_string()).await?;
        let Some(peer) = sources
            .into_iter()
            .map(PeerKey::new)
            .find(|peer| peers.is_none_or(|peers| peers.contains(peer)))
        else {
            tx.rollback().await?;
            return Ok(None);
        };
        sqlite::queries::begin_attempt(&tx, inventory.to_string(), digest.to_string()).await?;
        tx.commit().await?;
        Ok(Some(Attempt {
            inventory: inventory.clone(),
            digest,
            generation: state.generation,
            authorization: state.authorization,
            peer,
        }))
    }

    pub async fn complete(&self, attempt: &Attempt, authorized: bool) -> Res<bool> {
        let tx = self.sql.begin_write().await?;
        let state = Self::state_in(&tx, &attempt.inventory, attempt.digest).await?;
        let current = state.is_some_and(|state| {
            authorized
                && state.fetch_eligible()
                && state.fetching
                && state.generation == attempt.generation
                && state.authorization == attempt.authorization
        });
        let changed = if current {
            sqlite::queries::complete_success(
                &tx,
                attempt.inventory.to_string(),
                attempt.digest.to_string(),
                i64::try_from(attempt.generation)?,
            )
            .await?
            .rows_affected
        } else {
            sqlite::queries::complete_failure(
                &tx,
                attempt.inventory.to_string(),
                attempt.digest.to_string(),
                i64::try_from(attempt.generation)?,
            )
            .await?
            .rows_affected
        };
        tx.commit().await?;
        if changed > 0 {
            self.changes.send_modify(|revision| *revision += 1);
            self.changed.notify_one();
        }
        Ok(current)
    }
}

#[cfg(test)]
mod tests;
