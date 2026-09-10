//! Durable encrypted secret-material storage.
//!
//! Two tables (migration 003):
//! - `big_repo_secret_blobs`: AEAD ciphertext for each secret blob, tagged
//!   with the `dek_id` + `dek_version` that encrypted it.
//! - `big_repo_deks`: wrapped DEKs, one row per `(dek_id, dek_version)`, so
//!   old versions remain unwrappable while blobs referencing them exist.
//!
//! This module is deliberately dumb: it stores raw bytes. Encryption/decryption
//! and DEK lifecycle live one layer up (the keyhive storage facade), which
//! calls into `secrets_rs` for the DEK/AEAD primitives.

use super::*;

/// Durable secret-material families. Stored in the `kind` column of
/// `big_repo_secret_blobs`; each family maps onto a default `dek_id` so
/// materials can be re-encrypted independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SecretBlobKind {
    /// CGKA / local peer secrets (today: the `local-secrets/` dir).
    LocalSecret = 0,
    /// Prekey-secret sidecar (today: `prekey-secrets.bin`).
    PrekeySidecar = 1,
    /// Doc reservations (ephemeral signing keys; today: `reservations/`).
    Reservation = 2,
}

impl SecretBlobKind {
    pub(crate) fn as_i64(self) -> i64 {
        self as i64
    }

    // Read-side decoding is unused today: the desktop DEK source (keyring /
    // file) never reads `kind` back from the DB. It is the future cloud/KMS
    // path (unwrap wrapped_dek, decrypt via the blob's recorded kind).
    #[allow(dead_code)]
    pub(crate) fn from_i64(value: i64) -> Result<Self, SqliteBigRepoStoreError> {
        match value {
            0 => Ok(Self::LocalSecret),
            1 => Ok(Self::PrekeySidecar),
            2 => Ok(Self::Reservation),
            _ => Err(SqliteBigRepoStoreError::InvalidRecord),
        }
    }
}

/// One stored (wrapped) DEK for a `(dek_id, dek_version)`.
///
/// Write-only today: envelope rows are recorded so rotation bookkeeping has
/// a durable source of truth, but desktop DEKs are sourced from the keyring /
/// file fallback. `load_dek` + `DekRow` are the future cloud/KMS unwrap path.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct DekRow {
    pub(crate) wrapped_dek: Vec<u8>,
    pub(crate) kek_version: u64,
    pub(crate) algorithm: String,
}

/// One stored encrypted secret blob, self-describing its DEK.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SecretBlobRow {
    pub(crate) dek_id: String,
    pub(crate) dek_version: u64,
    pub(crate) ciphertext: Vec<u8>,
    pub(crate) nonce: Vec<u8>,
}

impl SqliteBigRepoStore {
    /// Upsert one wrapped DEK row for `(dek_id, version)`. Re-wrapping the
    /// same version (e.g. after a KEK rotation) overwrites the prior wrap.
    pub(crate) async fn save_dek(
        &self,
        dek_id: &str,
        version: u64,
        wrapped_dek: Vec<u8>,
        kek_version: u64,
        algorithm: &str,
    ) -> Res<()> {
        sqlx::query!(
            "INSERT INTO big_repo_deks(
                scope_id, dek_id, dek_version, wrapped_dek, kek_version, algorithm
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(scope_id, dek_id, dek_version) DO UPDATE SET
                 wrapped_dek = excluded.wrapped_dek,
                 kek_version = excluded.kek_version,
                 algorithm = excluded.algorithm",
            self.scope().id(),
            dek_id,
            i64::try_from(version).expect(ERROR_IMPOSSIBLE),
            &wrapped_dek,
            i64::try_from(kek_version).expect(ERROR_IMPOSSIBLE),
            algorithm,
        )
        .execute(&self.sql.write_pool)
        .await?;
        Ok(())
    }

    /// Load one wrapped DEK row, if present.
    ///
    /// Write-only today (see `DekRow`): the desktop DEK source is the keyring /
    /// file fallback, so nothing reads the envelope back yet. Future cloud/KMS
    /// unwrap + rotation GC will consume this.
    #[allow(dead_code)]
    pub(crate) async fn load_dek(&self, dek_id: &str, version: u64) -> Res<Option<DekRow>> {
        let row = sqlx::query!(
            "SELECT wrapped_dek, kek_version, algorithm
               FROM big_repo_deks
              WHERE scope_id = ?1 AND dek_id = ?2 AND dek_version = ?3",
            self.scope().id(),
            dek_id,
            i64::try_from(version).expect(ERROR_IMPOSSIBLE),
        )
        .fetch_optional(&self.sql.read_pool)
        .await?;
        Ok(row.map(|row| DekRow {
            wrapped_dek: row.wrapped_dek,
            kek_version: Self::u64_from_db(row.kek_version),
            algorithm: row.algorithm,
        }))
    }

    /// Upsert one encrypted secret blob under its DEK.
    pub(crate) async fn save_secret_blob(
        &self,
        kind: SecretBlobKind,
        blob_id: &[u8],
        dek_id: &str,
        dek_version: u64,
        ciphertext: Vec<u8>,
        nonce: Vec<u8>,
    ) -> Res<bool> {
        let mut tx = self.sql.write_pool.begin().await?;
        let dek_version = i64::try_from(dek_version).expect(ERROR_IMPOSSIBLE);
        let inserted = sqlx::query(
            "INSERT INTO big_repo_secret_blobs
                 (scope_id
                , kind
                , blob_id
                , dek_id
                , dek_version
                , ciphertext
                , nonce
                 )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(scope_id, kind, blob_id) DO NOTHING",
        )
        .bind(self.scope().id())
        .bind(kind.as_i64())
        .bind(blob_id)
        .bind(dek_id)
        .bind(dek_version)
        .bind(&ciphertext)
        .bind(&nonce)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;

        if !inserted {
            sqlx::query(
                "UPDATE big_repo_secret_blobs
                    SET dek_id = ?1
                      , dek_version = ?2
                      , ciphertext = ?3
                      , nonce = ?4
                  WHERE scope_id = ?5
                    AND kind = ?6
                    AND blob_id = ?7",
            )
            .bind(dek_id)
            .bind(dek_version)
            .bind(&ciphertext)
            .bind(&nonce)
            .bind(self.scope().id())
            .bind(kind.as_i64())
            .bind(blob_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(inserted)
    }

    /// Load one encrypted secret blob, if present.
    pub(crate) async fn load_secret_blob(
        &self,
        kind: SecretBlobKind,
        blob_id: &[u8],
    ) -> Res<Option<SecretBlobRow>> {
        let row = sqlx::query!(
            "SELECT dek_id, dek_version, ciphertext, nonce
               FROM big_repo_secret_blobs
              WHERE scope_id = ?1 AND kind = ?2 AND blob_id = ?3",
            self.scope().id(),
            kind.as_i64(),
            blob_id,
        )
        .fetch_optional(&self.sql.read_pool)
        .await?;
        Ok(row.map(|row| SecretBlobRow {
            dek_id: row.dek_id,
            dek_version: Self::u64_from_db(row.dek_version),
            ciphertext: row.ciphertext,
            nonce: row.nonce,
        }))
    }

    /// All blob ids present for one kind (listing, dedup, GC scans).
    pub(crate) async fn list_secret_blob_ids(&self, kind: SecretBlobKind) -> Res<Vec<Vec<u8>>> {
        let rows: Vec<Vec<u8>> = sqlx::query_scalar!(
            "SELECT blob_id AS \"blob_id: Vec<u8>\"
               FROM big_repo_secret_blobs
              WHERE scope_id = ?1 AND kind = ?2
              ORDER BY blob_id",
            self.scope().id(),
            kind.as_i64(),
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        Ok(rows)
    }

    /// Delete one encrypted secret blob.
    pub(crate) async fn delete_secret_blob(&self, kind: SecretBlobKind, blob_id: &[u8]) -> Res<()> {
        sqlx::query!(
            "DELETE FROM big_repo_secret_blobs
              WHERE scope_id = ?1 AND kind = ?2 AND blob_id = ?3",
            self.scope().id(),
            kind.as_i64(),
            blob_id,
        )
        .execute(&self.sql.write_pool)
        .await?;
        Ok(())
    }
}
