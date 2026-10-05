//! Durable roots for encrypted-representation pairs.
//!
//! A registered pair is two named tags, `ct:<C> -> C` and `pt:<C> -> P`, and
//! named tags are exactly what the store's GC seeds its root set from, so those
//! two tags are the only thing keeping a representation and the plaintext that
//! serves it alive. They are written by
//! [`CipherBlobProvider::register_pair`](crate::blobs::encrypt::CipherBlobProvider::register_pair)
//! and released by the pin worker when the ciphertext pin leaves the encryption
//! inventory.
//!
//! That release is inventory-diff driven, so it can only release pairs the
//! inventory ever recorded. A crash between the tags and the pin leaves `C` and
//! `P` rooted with nothing that will ever claim them again (ADR 003 §19): the
//! diff cannot see a pair it never wrote, so those tags are permanent.
//!
//! This table is that missing record: one row per rooted pair, written *before*
//! the tags (see [`PairRoots::record_before_root`]) and cleared once a durable
//! facet names the pair or the pin worker has released its tags. A row is not a
//! GC root and not a substitute for the encryption inventory - it only carries a
//! pair's existence across the window in which the tags exist and no facet names
//! them yet. The boot drain over [`PairRoots::unresolved`] lives in
//! [`crate::blobs::pin_worker`], which owns the inventories and the release path.
//!
//! A row also carries the document whose `cipherBlob` facet was expected to name
//! the pair (see [`PairRoots::attach_provenance`]). The drain needs it to tell a
//! pair no facet ever named, which it must release, from one whose pin the worker
//! has simply not derived yet, which it must leave.
//!
//! Releasing never deletes bytes: dropping the two tags hands the pair back to
//! the store's own GC, which reclaims exactly what nothing else roots.

use crate::interlude::*;
use daybook_types::doc::DocId;
use iroh_blobs::Hash;
use sqlx::Row;

/// The durable root record for `ct:<C>`/`pt:<C>` pairs.
///
/// Cheap to clone: it is the local-state SQL context, not a pool of its own.
#[derive(Clone)]
pub struct PairRoots {
    sql: SqlCtx,
}

/// One ledger row a boot has to resolve.
pub(crate) struct UnresolvedPair {
    /// The ciphertext this pair roots, in the hex spelling rows are written in.
    pub cipher_hash: Hash,
    /// The document and branch whose `cipherBlob` facet was expected to name this
    /// pair, when the rooting path knew them. `None` means no facet check is
    /// possible, so the drain must leave the pair alone: it cannot show that
    /// nothing claims it.
    pub claimed_by: Option<(DocId, String)>,
}

impl PairRoots {
    /// Open the record over a booted local-state database.
    ///
    /// Idempotent, so every component that roots or resolves pairs may call it
    /// over the same `SqlCtx`.
    pub(crate) async fn boot(sql: SqlCtx) -> Res<Self> {
        Self::init_schema(&sql).await?;
        Ok(Self { sql })
    }

    async fn init_schema(sql: &SqlCtx) -> Res<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS blob_pair_root (
                cipher_hash TEXT NOT NULL PRIMARY KEY
              , doc_id TEXT
              , branch_path TEXT
            ) STRICT
            "#,
        )
        .execute(&sql.write_pool)
        .await?;
        Ok(())
    }

    /// Record that `c_hash`'s pair is about to be rooted.
    ///
    /// This runs immediately before the tags are written. That ordering is the
    /// whole contract: a rooted pair therefore always has a row, which is what
    /// lets the boot drain tell a pair the inventory never recorded (release it)
    /// from one the inventory has not caught up with yet (leave it).
    ///
    /// Re-recording is idempotent: a crash between the tags and the facet write
    /// replays this on the next attempt, and the row it already has is the one
    /// that says the pair is still unresolved.
    pub(crate) async fn record_before_root(&self, c_hash: Hash) -> Res<()> {
        sqlx::query("INSERT OR IGNORE INTO blob_pair_root (cipher_hash) VALUES (?)")
            .bind(c_hash.to_hex())
            .execute(&self.sql.write_pool)
            .await?;
        Ok(())
    }

    /// Record which document's facet is expected to name this pair.
    ///
    /// Called by the rooting path straight after the tags are written, while it
    /// still knows the document it is about to author the `cipherBlob` facet
    /// into. An `UPDATE`, not an insert: the row already exists (see
    /// [`PairRoots::record_before_root`]), and a missing row means a rooting path
    /// skipped the record, which is the one bug this ledger exists to prevent.
    ///
    /// A crash between the tags and this call leaves a row with no provenance, so
    /// the drain leaves that pair alone rather than guessing at the facets.
    pub(crate) async fn attach_provenance(
        &self,
        c_hash: Hash,
        doc_id: &DocId,
        branch_path: &str,
    ) -> Res<()> {
        let updated = sqlx::query(
            "UPDATE blob_pair_root
                SET doc_id = ?
                  , branch_path = ?
              WHERE cipher_hash = ?",
        )
        .bind(doc_id)
        .bind(branch_path)
        .bind(c_hash.to_hex())
        .execute(&self.sql.write_pool)
        .await?
        .rows_affected();
        eyre::ensure!(
            updated == 1,
            "pair {c_hash} was given provenance with no root record: \
             record_before_root must run before the tags are written"
        );
        Ok(())
    }

    /// Drop the row for `c_hash`: the pair is recorded somewhere else now (a
    /// durable facet names it, or the pin worker released its tags).
    ///
    /// Deleting an absent row is a no-op, so both the boot drain and the pin
    /// worker may call this without coordinating.
    pub(crate) async fn clear(&self, c_hash: Hash) -> Res<()> {
        sqlx::query("DELETE FROM blob_pair_root WHERE cipher_hash = ?")
            .bind(c_hash.to_hex())
            .execute(&self.sql.write_pool)
            .await?;
        Ok(())
    }

    /// Every row a previous run left unresolved, with whatever the rooting path
    /// recorded about the facets that were expected to claim it.
    ///
    /// The drain resolves rows independently and in any order, so no ordering is
    /// asked of the database here.
    pub(crate) async fn unresolved(&self) -> Res<Vec<UnresolvedPair>> {
        let rows = sqlx::query("SELECT cipher_hash, doc_id, branch_path FROM blob_pair_root")
            .fetch_all(&self.sql.read_pool)
            .await?;
        rows.into_iter()
            .map(|row| {
                let hex: String = row.try_get("cipher_hash")?;
                let cipher_hash = hex.parse::<Hash>().map_err(|err| {
                    // Only this module writes rows, and it writes canonical hex:
                    // a row we cannot parse means the table was tampered with,
                    // which is worth a loud failure rather than a quiet skip that
                    // would strand the pair's tags forever.
                    eyre::eyre!("blob_pair_root holds {hex}, not a content hash: {err}")
                })?;
                let doc_id: Option<DocId> = row.try_get("doc_id")?;
                let branch_path: Option<String> = row.try_get("branch_path")?;
                // Both columns come from one statement or neither does, so a
                // half-filled provenance is not a state this table can be in.
                // Treating it as absent costs only a conservative leave.
                Ok(UnresolvedPair {
                    cipher_hash,
                    claimed_by: doc_id.zip(branch_path),
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A row survives a reboot with its provenance, and `clear` retires it.
    ///
    /// The reboot is the point: the drain only ever runs over rows an earlier
    /// process wrote, so the columns and the hex round trip have to survive
    /// `boot` over the same database.
    #[tokio::test]
    async fn provenance_round_trips_across_a_boot() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let roots = PairRoots::boot(sql.clone()).await?;
        let blob_id = crate::blobs::BlobId::new(*blake3::hash(b"pair root round trip").as_bytes());
        let c_hash = crate::blobs::blob_id_to_iroh_hash(blob_id);

        roots.record_before_root(c_hash).await?;
        let rebooted = PairRoots::boot(sql.clone()).await?;
        let unresolved = rebooted.unresolved().await?;
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0].cipher_hash, c_hash);
        assert!(
            unresolved[0].claimed_by.is_none(),
            "nothing attached provenance yet"
        );

        let doc_id: DocId = "doc-pair-root-test".to_string();
        rebooted.attach_provenance(c_hash, &doc_id, "main").await?;
        let unresolved = rebooted.unresolved().await?;
        assert_eq!(unresolved[0].claimed_by, Some((doc_id, "main".to_string())));

        rebooted.clear(c_hash).await?;
        assert!(rebooted.unresolved().await?.is_empty());
        Ok(())
    }

    /// A rooting path cannot attach provenance for a pair it never recorded.
    #[tokio::test]
    async fn provenance_without_a_record_is_a_loud_failure() -> Res<()> {
        let roots = PairRoots::boot(SqlCtx::memory().await?).await?;
        let blob_id = crate::blobs::BlobId::new(*blake3::hash(b"unrecorded pair").as_bytes());
        let c_hash = crate::blobs::blob_id_to_iroh_hash(blob_id);
        let err = roots
            .attach_provenance(c_hash, &"doc-unrecorded".to_string(), "main")
            .await
            .expect_err("attach without record_before_root must fail");
        assert!(
            err.to_string().contains("no root record"),
            "unexpected error: {err}"
        );
        Ok(())
    }
}
