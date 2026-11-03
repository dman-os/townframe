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

pub use discovery::Coverage;
pub use discovery::CoverageGap;
pub(crate) use discovery::DrawerSnapshot;
pub use discovery::InventorySelection;
pub use runtime::InventoryMirrors;
#[cfg(test)]
mod discovery_tests;
#[cfg(test)]
mod producers_tests;
#[cfg(test)]
mod project_tests;
#[cfg(test)]
mod restart_tests;
#[cfg(test)]
mod runtime_tests;

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
        sql.apply_migrations("big_blobs_admission_migrations", &[sqlite_rs::Migration {
            version: 1,
            name: "inventory_pair_admission",
            sql: "CREATE TABLE big_blobs_inventory_registry (
                     inventory_id TEXT PRIMARY KEY NOT NULL
                   , part_id BLOB NOT NULL UNIQUE
                   , readable INTEGER NOT NULL DEFAULT 0 CHECK (readable IN (0, 1))
                   , authorization INTEGER NOT NULL DEFAULT 0
                   , pin_revision INTEGER NOT NULL DEFAULT 0
                   , pins_complete INTEGER NOT NULL DEFAULT 0 CHECK (pins_complete IN (0, 1))
                 ) STRICT;
                 CREATE TABLE big_blobs_mirror_claims (
                     inventory_id TEXT NOT NULL
                   , owner TEXT NOT NULL
                   , PRIMARY KEY (inventory_id, owner)
                 ) STRICT;
                 CREATE TABLE big_blobs_admission_pairs (
                     inventory_id TEXT NOT NULL
                   , digest TEXT NOT NULL
                   , length_octets INTEGER
                   , bytes INTEGER NOT NULL DEFAULT 0 CHECK (bytes IN (0, 1))
                   , generation INTEGER NOT NULL DEFAULT 1
                   , attempt INTEGER
                   , dirty INTEGER NOT NULL DEFAULT 1 CHECK (dirty IN (0, 1))
                   , PRIMARY KEY (inventory_id, digest)
                 ) STRICT;
                 CREATE INDEX big_blobs_admission_dirty ON big_blobs_admission_pairs (dirty, inventory_id, digest);
                 CREATE TABLE big_blobs_producer_claims (
                     inventory_id TEXT NOT NULL
                   , digest TEXT NOT NULL
                   , owner TEXT NOT NULL
                   , PRIMARY KEY (inventory_id, digest, owner)
                 ) STRICT;
                 CREATE TABLE big_blobs_source_observations (
                     inventory_id TEXT NOT NULL
                   , digest TEXT NOT NULL
                   , peer_id BLOB NOT NULL
                   , revision INTEGER NOT NULL
                   , present INTEGER NOT NULL CHECK (present IN (0, 1))
                   , payload TEXT
                   , PRIMARY KEY (inventory_id, digest, peer_id)
                 ) STRICT;
                 CREATE INDEX big_blobs_source_present ON big_blobs_source_observations (inventory_id, digest, present);",
        }]).await?;
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
        self.sql.execute(
            "INSERT OR IGNORE INTO big_blobs_inventory_registry (inventory_id, part_id) VALUES (?, ?)",
            (inventory.to_string(), part.as_bytes().to_vec()),
        ).await?;
        Ok(())
    }

    pub async fn inventory(&self, part: &PartKey) -> Res<Option<DocId>> {
        self.sql
            .query(
                "SELECT inventory_id FROM big_blobs_inventory_registry WHERE part_id = ?",
                (part.as_bytes().to_vec(),),
            )
            .await?
            .all::<String>()?
            .into_iter()
            .next()
            .map(|id| Ok(id.parse()?))
            .transpose()
    }

    pub async fn recover(&self) -> Res<()> {
        let tx = self.sql.begin_write().await?;
        let inventories = tx.execute("UPDATE big_blobs_inventory_registry SET readable = 0, authorization = authorization + 1", ()).await?.rows_affected;
        let pairs = tx.execute("UPDATE big_blobs_admission_pairs SET generation = generation + 1, attempt = NULL, dirty = 1", ()).await?.rows_affected;
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
        let exists = !tx
            .query(
                "SELECT 1 FROM big_blobs_mirror_claims WHERE inventory_id = ? AND owner = ?",
                (inventory.to_string(), owner),
            )
            .await?
            .all::<i64>()?
            .is_empty();
        if exists == selected {
            tx.rollback().await?;
            return Ok(());
        }
        if selected {
            tx.execute(
                "INSERT INTO big_blobs_mirror_claims (inventory_id, owner) VALUES (?, ?)",
                (inventory.to_string(), owner),
            )
            .await?;
        } else {
            tx.execute(
                "DELETE FROM big_blobs_mirror_claims WHERE inventory_id = ? AND owner = ?",
                (inventory.to_string(), owner),
            )
            .await?;
        }
        tx.execute("UPDATE big_blobs_admission_pairs SET generation = generation + 1, attempt = NULL, dirty = 1 WHERE inventory_id = ?", (inventory.to_string(),)).await?;
        tx.commit().await?;
        self.changes.send_modify(|revision| *revision += 1);
        self.changed.notify_one();
        Ok(())
    }

    pub async fn authorize(&self, inventory: &DocId, readable: bool) -> Res<()> {
        self.register(inventory).await?;
        let tx = self.sql.begin_write().await?;
        let previous = tx
            .query(
                "SELECT readable FROM big_blobs_inventory_registry WHERE inventory_id = ?",
                (inventory.to_string(),),
            )
            .await?
            .all::<bool>()?[0];
        if previous == readable {
            tx.rollback().await?;
            return Ok(());
        }
        tx.execute("UPDATE big_blobs_inventory_registry SET readable = ?, authorization = authorization + 1 WHERE inventory_id = ?", (readable, inventory.to_string())).await?;
        tx.execute("UPDATE big_blobs_admission_pairs SET generation = generation + 1, attempt = NULL, dirty = 1 WHERE inventory_id = ?", (inventory.to_string(),)).await?;
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
        let (previous, previously_complete) = tx
            .query(
                "SELECT pin_revision\n                      , pins_complete\n                   FROM big_blobs_inventory_registry\n                  WHERE inventory_id = ?",
                (inventory.to_string(),),
            )
            .await?
            .all::<(u64, bool)>()?[0];
        if revision < previous || (revision == previous && (previously_complete || !complete)) {
            tx.rollback().await?;
            return Ok(());
        }
        if complete {
            let previous = tx.query("SELECT digest FROM big_blobs_admission_pairs WHERE inventory_id = ? AND length_octets IS NOT NULL", (inventory.to_string(),)).await?.all::<String>()?;
            for digest in previous {
                if !pins.contains_key(&digest.parse()?) {
                    tx.execute("UPDATE big_blobs_admission_pairs SET length_octets = NULL, generation = generation + 1, attempt = NULL, dirty = 1 WHERE inventory_id = ? AND digest = ?", (inventory.to_string(), digest)).await?;
                }
            }
        }
        for (digest, length) in pins {
            tx.execute("INSERT INTO big_blobs_admission_pairs (inventory_id, digest, length_octets) VALUES (?, ?, ?)
                        ON CONFLICT (inventory_id, digest) DO UPDATE SET length_octets = excluded.length_octets, generation = generation + 1, attempt = NULL, dirty = 1
                        WHERE length_octets IS NOT excluded.length_octets", (inventory.to_string(), digest.to_string(), i64::try_from(*length)?)).await?;
        }
        tx.execute("UPDATE big_blobs_inventory_registry SET pin_revision = ?, pins_complete = ? WHERE inventory_id = ?", (i64::try_from(revision)?, complete, inventory.to_string())).await?;
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
        tx.execute("INSERT INTO big_blobs_source_observations (inventory_id, digest, peer_id, revision, present, payload) VALUES (?, ?, ?, 0, ?, ?)
                    ON CONFLICT (inventory_id, digest, peer_id) DO UPDATE SET present = excluded.present, payload = COALESCE(excluded.payload, payload)",
                    (inventory.to_string(), digest.to_string(), peer.as_bytes().to_vec(), present, payload.map(serde_json::to_string).transpose()?)).await?;
        Self::touch(&tx, inventory, digest).await?;
        tx.commit().await?;
        self.changes.send_modify(|revision| *revision += 1);
        self.changed.notify_one();
        Ok(())
    }

    async fn touch(tx: &Tx, inventory: &DocId, digest: BlobId) -> Res<()> {
        tx.execute("INSERT INTO big_blobs_admission_pairs (inventory_id, digest) VALUES (?, ?)
                    ON CONFLICT (inventory_id, digest) DO UPDATE SET generation = generation + 1, attempt = NULL, dirty = 1", (inventory.to_string(), digest.to_string())).await?;
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
        let exists = !tx.query(
            "SELECT 1 FROM big_blobs_producer_claims WHERE inventory_id = ? AND digest = ? AND owner = ?",
            (inventory.to_string(), digest.to_string(), owner),
        ).await?.all::<i64>()?.is_empty();
        if exists == active {
            tx.rollback().await?;
            return Ok(());
        }
        if active {
            tx.execute("INSERT OR IGNORE INTO big_blobs_producer_claims (inventory_id, digest, owner) VALUES (?, ?, ?)", (inventory.to_string(), digest.to_string(), owner)).await?;
        } else {
            tx.execute("DELETE FROM big_blobs_producer_claims WHERE inventory_id = ? AND digest = ? AND owner = ?", (inventory.to_string(), digest.to_string(), owner)).await?;
        }
        Self::touch(&tx, inventory, digest).await?;
        if active {
            tx.execute("UPDATE big_blobs_admission_pairs SET bytes = 1 WHERE inventory_id = ? AND digest = ?", (inventory.to_string(), digest.to_string())).await?;
        }
        tx.commit().await?;
        self.changes.send_modify(|revision| *revision += 1);
        self.changed.notify_one();
        Ok(())
    }

    async fn state_in(tx: &Tx, inventory: &DocId, digest: BlobId) -> Res<Option<PairState>> {
        let rows = tx.query(
            "SELECT EXISTS (SELECT 1 FROM big_blobs_mirror_claims s WHERE s.inventory_id = p.inventory_id) AS selected
                  , EXISTS (SELECT 1 FROM big_blobs_producer_claims w WHERE w.inventory_id = p.inventory_id AND w.digest = p.digest) AS writer
                  , p.length_octets IS NOT NULL AS pinned
                  , EXISTS (SELECT 1 FROM big_blobs_source_observations o WHERE o.inventory_id = p.inventory_id AND o.digest = p.digest AND o.present = 1) AS observed
                  , r.readable
                  , p.bytes
                  , p.generation
                  , r.authorization
                  , p.attempt IS NOT NULL AS fetching
               FROM big_blobs_admission_pairs p
               JOIN big_blobs_inventory_registry r USING (inventory_id)
              WHERE p.inventory_id = ? AND p.digest = ?",
            (inventory.to_string(), digest.to_string()),
        ).await?;
        rows.rows
            .into_iter()
            .next()
            .map(|row| {
                Ok(PairState {
                    selected: row.get("selected")?,
                    writer: row.get("writer")?,
                    pinned: row.get("pinned")?,
                    observed: row.get("observed")?,
                    readable: row.get("readable")?,
                    bytes: row.get("bytes")?,
                    generation: row.get("generation")?,
                    authorization: row.get("authorization")?,
                    fetching: row.get("fetching")?,
                })
            })
            .transpose()
    }

    pub async fn state(&self, inventory: &DocId, digest: BlobId) -> Res<Option<PairState>> {
        let tx = self.sql.begin_write().await?;
        let state = Self::state_in(&tx, inventory, digest).await?;
        tx.commit().await?;
        Ok(state)
    }

    pub async fn available(&self, inventory: &DocId, digest: BlobId, available: bool) -> Res<()> {
        let changed = self.sql.execute("UPDATE big_blobs_admission_pairs SET bytes = ?, dirty = 1 WHERE inventory_id = ? AND digest = ? AND bytes != ?", (available, inventory.to_string(), digest.to_string(), available)).await?.rows_affected;
        if changed > 0 {
            self.changes.send_modify(|revision| *revision += 1);
            self.changed.notify_one();
        }
        Ok(())
    }

    pub async fn length(&self, inventory: &DocId, digest: BlobId) -> Res<Option<u64>> {
        Ok(self.sql.query("SELECT length_octets FROM big_blobs_admission_pairs WHERE inventory_id = ? AND digest = ? AND length_octets IS NOT NULL", (inventory.to_string(), digest.to_string())).await?.all::<u64>()?.into_iter().next())
    }

    pub async fn inventories(&self) -> Res<Vec<DocId>> {
        self.sql
            .query(
                "SELECT inventory_id FROM big_blobs_inventory_registry ORDER BY inventory_id",
                (),
            )
            .await?
            .all::<String>()?
            .into_iter()
            .map(|id| Ok(id.parse()?))
            .collect()
    }

    pub async fn dirty(&self) -> Res<Vec<(DocId, BlobId, u64)>> {
        self.sql.query("SELECT inventory_id, digest, generation FROM big_blobs_admission_pairs WHERE dirty = 1 ORDER BY inventory_id, digest LIMIT 128", ()).await?.all::<(String, String, u64)>()?.into_iter().map(|(inventory, digest, generation)| Ok((inventory.parse()?, digest.parse()?, generation))).collect()
    }

    #[cfg(test)]
    pub async fn settle(&self, inventory: &DocId, digest: BlobId, generation: u64) -> Res<()> {
        let changed = self.sql.execute("UPDATE big_blobs_admission_pairs SET dirty = 0 WHERE inventory_id = ? AND digest = ? AND generation = ? AND dirty = 1", (inventory.to_string(), digest.to_string(), i64::try_from(generation)?)).await?.rows_affected;
        if changed > 0 {
            self.changes.send_modify(|revision| *revision += 1);
        }
        Ok(())
    }

    pub(super) async fn publish(
        &self,
        inventory: &DocId,
        digest: BlobId,
        effect: impl FnOnce(PairState, Option<u64>) -> futures::future::BoxFuture<'static, Res<bool>>,
    ) -> Res<()> {
        let Some(pair) = self.state(inventory, digest).await? else {
            return Ok(());
        };
        let generation = i64::try_from(pair.generation)?;
        let length = self.length(inventory, digest).await?;
        let invalidated = self
            .sql
            .execute(
                "UPDATE big_blobs_admission_pairs SET dirty = 1
              WHERE inventory_id = ? AND digest = ? AND generation = ? AND dirty = 0",
                (inventory.to_string(), digest.to_string(), generation),
            )
            .await?
            .rows_affected;
        if invalidated > 0 {
            self.changes.send_modify(|revision| *revision += 1);
        }
        let available = effect(pair, length).await?;
        let settled = self
            .sql
            .execute(
                "UPDATE big_blobs_admission_pairs
                SET bytes = ?
                  , dirty = 0
              WHERE inventory_id = ? AND digest = ? AND generation = ? AND dirty = 1",
                (
                    available,
                    inventory.to_string(),
                    digest.to_string(),
                    generation,
                ),
            )
            .await?
            .rows_affected;
        assert_eq!(settled, 1, "publication is serialized with pair mutations");
        self.changes.send_modify(|revision| *revision += 1);
        Ok(())
    }

    pub async fn rearm(&self) -> Res<()> {
        let changed = self
            .sql
            .execute(
                "UPDATE big_blobs_admission_pairs SET dirty = 1 WHERE dirty = 0",
                (),
            )
            .await?
            .rows_affected;
        if changed > 0 {
            self.changes.send_modify(|revision| *revision += 1);
            self.changed.notify_one();
        }
        Ok(())
    }

    pub async fn fail(&self, attempt: &Attempt) -> Res<()> {
        let changed = self.sql.execute("UPDATE big_blobs_admission_pairs SET generation = generation + 1, attempt = NULL WHERE inventory_id = ? AND digest = ? AND generation = ? AND attempt = ?", (attempt.inventory.to_string(), attempt.digest.to_string(), i64::try_from(attempt.generation)?, i64::try_from(attempt.generation)?)).await?.rows_affected;
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
        let sources = tx.query("SELECT peer_id FROM big_blobs_source_observations WHERE inventory_id = ? AND digest = ? AND present = 1 ORDER BY peer_id", (inventory.to_string(), digest.to_string())).await?.all::<Vec<u8>>()?;
        let Some(peer) = sources
            .into_iter()
            .map(PeerKey::new)
            .find(|peer| peers.is_none_or(|peers| peers.contains(peer)))
        else {
            tx.rollback().await?;
            return Ok(None);
        };
        tx.execute("UPDATE big_blobs_admission_pairs SET attempt = generation WHERE inventory_id = ? AND digest = ?", (inventory.to_string(), digest.to_string())).await?;
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
            tx.execute("UPDATE big_blobs_admission_pairs SET bytes = 1, attempt = NULL, dirty = 1 WHERE inventory_id = ? AND digest = ? AND attempt = ?", (attempt.inventory.to_string(), attempt.digest.to_string(), i64::try_from(attempt.generation)?)).await?.rows_affected
        } else {
            tx.execute("UPDATE big_blobs_admission_pairs SET generation = generation + 1, attempt = NULL, dirty = 1 WHERE inventory_id = ? AND digest = ? AND attempt = ?", (attempt.inventory.to_string(), attempt.digest.to_string(), i64::try_from(attempt.generation)?)).await?.rows_affected
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
mod tests {
    use super::*;

    fn database() -> Database {
        sqlite_rs::SqliteRuntime::new(Default::default())
            .ephemeral(&[])
            .unwrap()
    }

    fn inventory() -> DocId {
        DocId::new([83; 32])
    }
    fn digest() -> BlobId {
        BlobId::new([84; 32])
    }
    fn peer() -> PeerKey {
        PeerKey::new([85; 32])
    }

    async fn selected(sql: Database) -> Res<(Admission, service_utils_rs::StopToken)> {
        let (admission, stop) = Admission::boot(sql).await?;
        admission.select(&inventory(), "root", true).await?;
        admission.authorize(&inventory(), true).await?;
        Ok((admission, stop))
    }

    async fn pin(admission: &Admission, revision: u64) -> Res<()> {
        admission
            .pins(
                &inventory(),
                revision,
                &BTreeMap::from([(digest(), 42)]),
                true,
            )
            .await
    }

    async fn object(admission: &Admission, revision: u64, present: bool) -> Res<()> {
        admission
            .observe(&inventory(), digest(), &peer(), revision, present, None)
            .await
    }

    #[tokio::test]
    async fn both_orderings_park_durably_and_require_reauthorization_after_restart() -> Res<()> {
        for pin_first in [false, true] {
            let sql = database();
            let (admission, stop) = selected(sql.clone()).await?;
            if pin_first {
                pin(&admission, 1).await?;
            } else {
                object(&admission, 10, true).await?;
            }
            assert!(admission.begin(&inventory(), digest()).await?.is_none());
            stop.stop().await;
            drop(admission);
            let (admission, stop) = Admission::boot(sql).await?;
            admission.recover().await?;
            if pin_first {
                object(&admission, 10, true).await?;
            } else {
                pin(&admission, 1).await?;
            }
            assert!(admission.begin(&inventory(), digest()).await?.is_none());
            admission.authorize(&inventory(), true).await?;
            let attempt = admission.begin(&inventory(), digest()).await?.unwrap();
            assert_eq!(attempt.peer, peer());
            assert!(admission.complete(&attempt, true).await?);
            let state = admission.state(&inventory(), digest()).await?.unwrap();
            assert!(state.advertisable());
            assert!(state.retains_bytes());
            stop.stop().await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn identical_digest_cannot_cross_unlock_another_inventory() -> Res<()> {
        let (admission, stop) = selected(database()).await?;
        let other = DocId::new([86; 32]);
        admission.select(&other, "root", true).await?;
        admission.authorize(&other, true).await?;
        pin(&admission, 1).await?;
        admission
            .observe(&other, digest(), &peer(), 10, true, None)
            .await?;
        assert!(admission.begin(&inventory(), digest()).await?.is_none());
        assert!(admission.begin(&other, digest()).await?.is_none());
        object(&admission, 10, true).await?;
        let attempt = admission.begin(&inventory(), digest()).await?.unwrap();
        assert!(admission.complete(&attempt, true).await?);
        assert!(
            admission
                .state(&other, digest())
                .await?
                .is_some_and(|state| !state.bytes && !state.advertisable())
        );
        assert_eq!(
            admission
                .inventory(&crate::workers::inventory_part(&other))
                .await?,
            Some(other)
        );
        stop.stop().await;
        Ok(())
    }

    #[tokio::test]
    async fn durable_tombstone_rejects_old_add_and_old_remove_cannot_undo_readd() -> Res<()> {
        let sql = database();
        let (admission, stop) = selected(sql.clone()).await?;
        pin(&admission, 1).await?;
        object(&admission, 10, true).await?;
        object(&admission, 20, false).await?;
        stop.stop().await;
        drop(admission);
        let (admission, stop) = Admission::boot(sql).await?;
        admission.recover().await?;
        admission.authorize(&inventory(), true).await?;
        object(&admission, 10, true).await?;
        assert!(
            !admission
                .state(&inventory(), digest())
                .await?
                .unwrap()
                .observed
        );
        object(&admission, 30, true).await?;
        object(&admission, 20, false).await?;
        assert!(admission.begin(&inventory(), digest()).await?.is_some());
        stop.stop().await;
        Ok(())
    }

    #[tokio::test]
    async fn late_completions_are_fenced_by_selection_removal_authority_and_restart() -> Res<()> {
        for transition in 0..5 {
            let sql = database();
            let (admission, stop) = selected(sql.clone()).await?;
            pin(&admission, 1).await?;
            object(&admission, 10, true).await?;
            let attempt = admission.begin(&inventory(), digest()).await?.unwrap();
            match transition {
                0 => admission.select(&inventory(), "root", false).await?,
                1 => object(&admission, 20, false).await?,
                2 => admission.authorize(&inventory(), false).await?,
                3 => {
                    admission
                        .pins(&inventory(), 2, &BTreeMap::new(), true)
                        .await?
                }
                4 => admission.recover().await?,
                _ => unreachable!(),
            }
            assert!(!admission.complete(&attempt, true).await?);
            let state = admission.state(&inventory(), digest()).await?.unwrap();
            assert!(!state.advertisable());
            assert!(!state.bytes);
            stop.stop().await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn incomplete_pins_preserve_facts_and_claims_are_independent() -> Res<()> {
        let (admission, stop) = selected(database()).await?;
        pin(&admission, 1).await?;
        object(&admission, 10, true).await?;
        let attempt = admission.begin(&inventory(), digest()).await?.unwrap();
        admission
            .pins(&inventory(), 2, &BTreeMap::new(), false)
            .await?;
        assert!(admission.complete(&attempt, true).await?);
        admission.select(&inventory(), "second-root", true).await?;
        admission.select(&inventory(), "root", false).await?;
        assert!(
            admission
                .state(&inventory(), digest())
                .await?
                .unwrap()
                .advertisable()
        );
        admission.select(&inventory(), "second-root", false).await?;
        let state = admission.state(&inventory(), digest()).await?.unwrap();
        assert!(!state.advertisable());
        assert!(state.retains_bytes());
        object(&admission, 20, false).await?;
        assert!(
            !admission
                .state(&inventory(), digest())
                .await?
                .unwrap()
                .retains_bytes()
        );
        stop.stop().await;
        Ok(())
    }

    #[tokio::test]
    async fn writer_only_production_never_schedules_mirror_downloads() -> Res<()> {
        let (admission, stop) = Admission::boot(database()).await?;
        admission
            .produce(&inventory(), digest(), "document", true)
            .await?;
        admission.authorize(&inventory(), true).await?;
        pin(&admission, 1).await?;
        assert!(
            admission
                .state(&inventory(), digest())
                .await?
                .unwrap()
                .advertisable()
        );
        assert!(admission.begin(&inventory(), digest()).await?.is_none());
        admission
            .produce(&inventory(), digest(), "document", false)
            .await?;
        assert!(
            !admission
                .state(&inventory(), digest())
                .await?
                .unwrap()
                .advertisable()
        );
        assert!(admission.begin(&inventory(), digest()).await?.is_none());
        stop.stop().await;
        Ok(())
    }

    #[tokio::test]
    async fn failed_attempt_cannot_clear_a_later_attempt_or_acknowledge_new_work() -> Res<()> {
        let (admission, stop) = selected(database()).await?;
        pin(&admission, 1).await?;
        object(&admission, 10, true).await?;
        let first = admission.begin(&inventory(), digest()).await?.unwrap();
        admission.fail(&first).await?;
        admission.rearm().await?;
        let second = admission.begin(&inventory(), digest()).await?.unwrap();
        assert!(second.generation > first.generation);
        admission.fail(&first).await?;
        assert!(!admission.complete(&first, true).await?);
        assert!(
            admission
                .state(&inventory(), digest())
                .await?
                .unwrap()
                .fetching
        );
        admission
            .settle(&inventory(), digest(), first.generation)
            .await?;
        assert_eq!(admission.dirty().await?.len(), 1);
        assert!(admission.complete(&second, true).await?);
        admission.available(&inventory(), digest(), false).await?;
        assert!(
            !admission
                .state(&inventory(), digest())
                .await?
                .unwrap()
                .advertisable()
        );
        assert_eq!(admission.length(&inventory(), digest()).await?, Some(42));
        assert_eq!(admission.inventories().await?, vec![inventory()]);
        stop.stop().await;
        Ok(())
    }

    #[tokio::test]
    async fn sources_and_payloadless_observations_are_independent() -> Res<()> {
        let (admission, stop) = selected(database()).await?;
        pin(&admission, 1).await?;
        object(&admission, 10, true).await?;
        let other = PeerKey::new([87; 32]);
        admission
            .observe(&inventory(), digest(), &other, 1, true, None)
            .await?;
        object(&admission, 20, false).await?;
        let attempt = admission.begin(&inventory(), digest()).await?.unwrap();
        assert_eq!(attempt.peer, other);
        assert!(admission.complete(&attempt, true).await?);
        assert!(
            admission
                .state(&inventory(), digest())
                .await?
                .unwrap()
                .advertisable()
        );
        stop.stop().await;
        Ok(())
    }
}
