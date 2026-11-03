//! Durable roots for encrypted-representation pairs.
//!
//! A registered pair is two named tags, `ct:<C> -> C` and `pt:<C> -> P`, and
//! named tags are exactly what the store's GC seeds its root set from, so those
//! two tags are the only thing keeping a representation and the plaintext that
//! serves it alive. They are written by
//! [`CipherBlobProvider::register_pair`](crate::encrypt::CipherBlobProvider::register_pair)
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
//! `daybook_core::blobs::pin_worker`, which owns the inventories and the release path.
//!
//! A row also carries the document whose `cipherBlob` facet was expected to name
//! the pair (see [`PairRoots::record_before_root_with_provenance`]). The drain needs it to tell a
//! pair no facet ever named, which it must release, from one whose pin the worker
//! has simply not derived yet, which it must leave.
//!
//! Releasing never deletes bytes: dropping the two tags hands the pair back to
//! the store's own GC, which reclaims exactly what nothing else roots.

use crate::encrypt::EncodingParams;
use crate::encrypt::MasterKey;
use crate::encrypt::Padding;
use crate::interlude::*;
use big_blobs_core::BlobId;

use iroh_blobs::Hash;

pub(crate) struct VirtualPair {
    pub cipher: Hash,
    pub plaintext: Hash,
    pub key: MasterKey,
    pub encoding: EncodingParams,
    pub source: String,
    pub claimed_by: Option<(String, String)>,
}

/// The durable root record for `ct:<C>`/`pt:<C>` pairs.
///
/// Cheap to clone: it is the local-state SQL context, not a pool of its own.
#[derive(Clone)]
pub struct PairRoots {
    sql: Database,
    ownership_bound: Arc<std::sync::atomic::AtomicBool>,
}

/// One ledger row a boot has to resolve.
pub struct UnresolvedPair {
    /// The ciphertext this pair roots, in the hex spelling rows are written in.
    pub cipher_hash: Hash,
    /// The document and branch whose `cipherBlob` facet was expected to name this
    /// pair, when the rooting path knew them. `None` means no facet check is
    /// possible, so the drain must leave the pair alone: it cannot show that
    /// nothing claims it.
    pub claimed_by: Option<(String, String)>,
}

impl PairRoots {
    /// Open the record over a booted local-state database.
    ///
    /// Idempotent, so every component that roots or resolves pairs may call it
    /// over the same `Database`.
    pub async fn boot(sql: Database) -> Res<Self> {
        Self::init_schema(&sql).await?;
        Ok(Self {
            sql,
            ownership_bound: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    async fn init_schema(sql: &Database) -> Res<()> {
        sql.apply_migrations(
            "big_blobs_pair_roots_migrations",
            &[
                sqlite_rs::Migration {
                    version: 1,
                    name: "blob_pair_root",
                    sql: "CREATE TABLE IF NOT EXISTS blob_pair_root (
                          cipher_hash TEXT NOT NULL PRIMARY KEY
                        , doc_id TEXT
                        , branch_path TEXT
                      ) STRICT;",
                },
                sqlite_rs::Migration {
                    version: 2,
                    name: "local_virtual_pairs",
                    sql: "CREATE TABLE big_blobs_virtual_pairs (
                              cipher_hash TEXT PRIMARY KEY NOT NULL
                            , digest TEXT UNIQUE NOT NULL
                            , plaintext_hash TEXT NOT NULL
                            , master_key BLOB NOT NULL CHECK (length(master_key) = 32)
                            , record_size INTEGER NOT NULL
                            , padding TEXT NOT NULL CHECK (padding IN ('minimal', 'record'))
                            , source TEXT NOT NULL
                          ) STRICT;",
                },
            ],
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn bind_ownership(&self) -> Res<bool> {
        use std::sync::atomic::Ordering;

        if self.ownership_bound.load(Ordering::Acquire) {
            return Ok(true);
        }
        if self.sql.query(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'big_blobs_byte_dependencies'",
            (),
        ).await?.optional::<i64>()?.is_none() {
            return Ok(false);
        }
        self.sql
            .execute_batch(
                "CREATE TRIGGER IF NOT EXISTS big_blobs_virtual_pair_dependency_release
             AFTER DELETE ON big_blobs_byte_dependencies
             BEGIN
                 DELETE FROM big_blobs_virtual_pairs WHERE digest = OLD.digest;
             END;
             CREATE TRIGGER IF NOT EXISTS big_blobs_virtual_pair_preclaim_release
             AFTER DELETE ON blob_pair_root
             WHEN NOT EXISTS (
                 SELECT 1
                   FROM big_blobs_virtual_pairs AS pair
                   JOIN big_blobs_byte_dependencies AS dependency ON dependency.digest = pair.digest
                  WHERE pair.cipher_hash = OLD.cipher_hash
             )
             BEGIN
                 DELETE FROM big_blobs_virtual_pairs WHERE cipher_hash = OLD.cipher_hash;
             END;",
            )
            .await?;
        self.ownership_bound.store(true, Ordering::Release);
        Ok(true)
    }

    pub(crate) async fn record_virtual(&self, pair: &VirtualPair) -> Res<()> {
        self.bind_ownership().await?;
        let tx = self.sql.begin_write().await?;
        let cipher = pair.cipher.to_hex().to_string();
        let changed = tx
            .execute(
                "INSERT INTO big_blobs_virtual_pairs
                 (cipher_hash, digest, plaintext_hash, master_key, record_size, padding, source)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (cipher_hash) DO UPDATE SET source = excluded.source
             WHERE plaintext_hash = excluded.plaintext_hash
               AND master_key = excluded.master_key
               AND record_size = excluded.record_size
               AND padding = excluded.padding",
                (
                    cipher.as_str(),
                    BlobId::new(*pair.cipher.as_bytes()).to_string(),
                    pair.plaintext.to_hex().to_string(),
                    pair.key.0.as_slice(),
                    i64::try_from(pair.encoding.record_size)?,
                    match pair.encoding.padding {
                        Padding::Minimal => "minimal",
                        Padding::Record => "record",
                    },
                    pair.source.as_str(),
                ),
            )
            .await?;
        eyre::ensure!(
            changed.rows_affected == 1,
            "virtual ciphertext binding changed"
        );
        tx.execute(
            "INSERT OR IGNORE INTO blob_pair_root (cipher_hash) VALUES (?)",
            (cipher.as_str(),),
        )
        .await?;
        if let Some((document, branch)) = &pair.claimed_by {
            tx.execute(
                "UPDATE blob_pair_root
                    SET doc_id = ?
                      , branch_path = ?
                  WHERE cipher_hash = ?",
                (document.as_str(), branch.as_str(), cipher.as_str()),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn virtual_pairs(&self) -> Res<Vec<(VirtualPair, bool, bool, bool)>> {
        let bound = self.bind_ownership().await?;
        let owned = if bound {
            "EXISTS (SELECT 1 FROM big_blobs_byte_owners WHERE digest = pair.digest)"
        } else {
            "0"
        };
        let managed_pin = if bound {
            "EXISTS (SELECT 1 FROM big_blobs_byte_pin_roots WHERE digest = pair.digest)"
        } else {
            "0"
        };
        let rows = self
            .sql
            .query(
                &format!(
                    "SELECT pair.cipher_hash
                      , pair.plaintext_hash
                      , pair.master_key
                      , pair.record_size
                      , pair.padding
                      , pair.source
                      , root.doc_id
                      , root.branch_path
                      , root.cipher_hash IS NOT NULL
                      , {owned}
                      , {managed_pin}
                   FROM big_blobs_virtual_pairs AS pair
                   LEFT JOIN blob_pair_root AS root ON root.cipher_hash = pair.cipher_hash"
                ),
                (),
            )
            .await?
            .all::<(
                String,
                String,
                Vec<u8>,
                u64,
                String,
                String,
                Option<String>,
                Option<String>,
                bool,
                bool,
                bool,
            )>()?;
        rows.into_iter()
            .map(
                |(
                    cipher,
                    plaintext,
                    key,
                    rs,
                    padding,
                    source,
                    doc,
                    branch,
                    preclaim,
                    owned,
                    managed_pin,
                )| {
                    let key = key
                        .try_into()
                        .map_err(|_| eyre::eyre!("invalid local virtual pair key length"))?;
                    let padding = match padding.as_str() {
                        "minimal" => Padding::Minimal,
                        "record" => Padding::Record,
                        _ => eyre::bail!("invalid local virtual pair padding"),
                    };
                    Ok((
                        VirtualPair {
                            cipher: cipher.parse()?,
                            plaintext: plaintext.parse()?,
                            key: MasterKey(key),
                            encoding: EncodingParams::new(rs, padding)?,
                            source,
                            claimed_by: doc.zip(branch),
                        },
                        preclaim,
                        owned,
                        managed_pin,
                    ))
                },
            )
            .collect()
    }

    pub(crate) async fn forget_virtual(&self, cipher: Hash) -> Res<()> {
        self.sql
            .execute(
                "DELETE FROM big_blobs_virtual_pairs WHERE cipher_hash = ?",
                (cipher.to_hex().to_string(),),
            )
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
    pub async fn record_before_root(&self, c_hash: Hash) -> Res<()> {
        self.sql
            .execute(
                "INSERT OR IGNORE INTO blob_pair_root (cipher_hash) VALUES (?)",
                (c_hash.to_hex().to_string(),),
            )
            .await?;
        Ok(())
    }

    /// Record a pair and its expected cipherBlob owner atomically before rooting it.
    ///
    /// The provenance must be in the same transaction as the root row: a crash
    /// after the tags are written but before a separate provenance update must
    /// still leave boot reconciliation able to prove that an unclaimed pair is
    /// safe to release.
    pub async fn record_before_root_with_provenance(
        &self,
        c_hash: Hash,
        doc_id: &str,
        branch_path: &str,
    ) -> Res<()> {
        let tx = self.sql.begin_write().await?;
        let cipher_hash = c_hash.to_hex().to_string();
        tx.execute(
            "INSERT OR IGNORE INTO blob_pair_root (cipher_hash) VALUES (?)",
            (cipher_hash.as_str(),),
        )
        .await?;
        tx.execute(
            "UPDATE blob_pair_root
                SET doc_id = ?
                  , branch_path = ?
              WHERE cipher_hash = ?",
            (doc_id, branch_path, cipher_hash),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Drop the row for `c_hash`: the pair is recorded somewhere else now (a
    /// durable facet names it, or the pin worker released its tags).
    ///
    /// Deleting an absent row is a no-op, so both the boot drain and the pin
    /// worker may call this without coordinating.
    pub async fn clear(&self, c_hash: Hash) -> Res<()> {
        let bound = self.bind_ownership().await?;
        let tx = self.sql.begin_write().await?;
        tx.execute(
            "DELETE FROM blob_pair_root WHERE cipher_hash = ?",
            (c_hash.to_hex().to_string(),),
        )
        .await?;
        if !bound {
            tx.execute(
                "DELETE FROM big_blobs_virtual_pairs WHERE cipher_hash = ?",
                (c_hash.to_hex().to_string(),),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Every row a previous run left unresolved, with whatever the rooting path
    /// recorded about the facets that were expected to claim it.
    ///
    /// The drain resolves rows independently and in any order, so no ordering is
    /// asked of the database here.
    pub async fn unresolved(&self) -> Res<Vec<UnresolvedPair>> {
        self.sql
            .query(
                "SELECT cipher_hash
                      , doc_id
                      , branch_path
                   FROM blob_pair_root",
                (),
            )
            .await?
            .all::<(String, Option<String>, Option<String>)>()?
            .into_iter()
            .map(|(hex, doc_id, branch_path)| {
                let cipher_hash = hex.parse::<Hash>().map_err(|err| {
                    // Only this module writes rows, and it writes canonical hex:
                    // a row we cannot parse means the table was tampered with,
                    // which is worth a loud failure rather than a quiet skip that
                    // would strand the pair's tags forever.
                    eyre::eyre!("blob_pair_root holds {hex}, not a content hash: {err}")
                })?;
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

    #[tokio::test]
    async fn virtual_binding_is_immutable_but_accepted_source_can_advance() -> Res<()> {
        let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[])?;
        let roots = PairRoots::boot(sql.clone()).await?;
        let mut pair = VirtualPair {
            cipher: Hash::new(b"accepted cipher"),
            plaintext: Hash::new(b"accepted plaintext"),
            key: MasterKey::random(),
            encoding: EncodingParams::new(4096, Padding::Minimal)?,
            source: "local".into(),
            claimed_by: Some(("document".into(), "main".into())),
        };
        roots.record_virtual(&pair).await?;
        pair.source = "accepted-document-version".into();
        roots.record_virtual(&pair).await?;
        pair.key = MasterKey::random();
        let error = roots.record_virtual(&pair).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("virtual ciphertext binding changed")
        );
        let mut records = roots.virtual_pairs().await?;
        assert_eq!(records.len(), 1);
        let (stored, preclaim, owned, managed_pin) = records.pop().unwrap();
        assert_eq!(stored.source, "accepted-document-version");
        assert_eq!(stored.encoding, pair.encoding);
        assert!(preclaim);
        assert!(!owned);
        assert!(!managed_pin);
        assert!(stored.key != pair.key);
        roots.clear(pair.cipher).await?;
        assert_eq!(
            sql.query("SELECT count(*) FROM big_blobs_virtual_pairs", ())
                .await?
                .one::<i64>()?,
            0
        );
        Ok(())
    }

    /// A row survives a reboot with its provenance, and `clear` retires it.
    #[tokio::test]
    async fn provenance_round_trips_across_a_boot() -> Res<()> {
        let runtime = sqlite_rs::SqliteRuntime::new(default());
        let sql = runtime.ephemeral(&[])?;
        let roots = PairRoots::boot(sql.clone()).await?;
        let c_hash = Hash::new(b"pair root round trip");
        let doc_id = "doc-pair-root-test".to_string();

        roots
            .record_before_root_with_provenance(c_hash, &doc_id, "main")
            .await?;
        let rebooted = PairRoots::boot(sql.clone()).await?;
        let unresolved = rebooted.unresolved().await?;
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0].cipher_hash, c_hash);
        assert_eq!(unresolved[0].claimed_by, Some((doc_id, "main".to_string())));

        rebooted.clear(c_hash).await?;
        assert!(rebooted.unresolved().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn boot_preserves_legacy_rows_and_table_identity() -> Res<()> {
        let runtime = sqlite_rs::SqliteRuntime::new(default());
        let sql = runtime.ephemeral(&[])?;
        sql.execute_batch(
            "CREATE TABLE IF NOT EXISTS blob_pair_root (
                 cipher_hash TEXT NOT NULL PRIMARY KEY
               , doc_id TEXT
               , branch_path TEXT
             ) STRICT;",
        )
        .await?;
        let c_hash = Hash::new(b"legacy pair root");
        sql.execute(
            "INSERT INTO blob_pair_root (cipher_hash, doc_id, branch_path) VALUES (?, ?, ?)",
            (c_hash.to_hex().to_string(), "legacy-doc", "main"),
        )
        .await?;

        let roots = PairRoots::boot(sql.clone()).await?;
        roots.record_before_root(c_hash).await?;
        roots.record_before_root(c_hash).await?;
        let mut unresolved = roots.unresolved().await?;
        assert_eq!(unresolved.len(), 1);
        let pair = unresolved.pop().unwrap();
        assert_eq!(
            (pair.cipher_hash, pair.claimed_by),
            (c_hash, Some(("legacy-doc".to_string(), "main".to_string())))
        );

        roots
            .record_before_root_with_provenance(c_hash, "updated-doc", "draft")
            .await?;
        assert_eq!(
            sql.query(
                "SELECT cipher_hash
                      , doc_id
                      , branch_path
                   FROM blob_pair_root",
                (),
            )
            .await?
            .all::<(String, Option<String>, Option<String>)>()?,
            vec![(
                c_hash.to_hex().to_string(),
                Some("updated-doc".to_string()),
                Some("draft".to_string())
            )]
        );
        roots.clear(c_hash).await?;
        roots.clear(c_hash).await?;
        assert!(
            sql.query("SELECT cipher_hash FROM blob_pair_root", ())
                .await?
                .all::<String>()?
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn unprovenanced_record_is_idempotent() -> Res<()> {
        let runtime = sqlite_rs::SqliteRuntime::new(default());
        let roots = PairRoots::boot(runtime.ephemeral(&[])?).await?;
        let c_hash = Hash::new(b"unprovenanced pair root");
        roots.record_before_root(c_hash).await?;
        roots.record_before_root(c_hash).await?;
        let mut unresolved = roots.unresolved().await?;
        assert_eq!(unresolved.len(), 1);
        let pair = unresolved.pop().unwrap();
        assert_eq!((pair.cipher_hash, pair.claimed_by), (c_hash, None));
        Ok(())
    }

    #[tokio::test]
    async fn provenance_failure_rolls_back_the_root_record() -> Res<()> {
        let runtime = sqlite_rs::SqliteRuntime::new(default());
        let sql = runtime.ephemeral(&[])?;
        let roots = PairRoots::boot(sql.clone()).await?;
        sql.execute_batch(
            "CREATE TRIGGER reject_provenance BEFORE UPDATE ON blob_pair_root
             BEGIN
                 SELECT RAISE(ABORT, 'rejected provenance');
             END;",
        )
        .await?;
        let error = roots
            .record_before_root_with_provenance(Hash::new(b"rejected pair root"), "doc", "main")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("rejected provenance"));
        assert!(roots.unresolved().await?.is_empty());
        Ok(())
    }
}
