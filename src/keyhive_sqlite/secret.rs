//! The key hierarchy of the secret columns: a key-encryption key the embedder holds, the
//! data-encryption keys it wraps, and the seal of each secret under its DEK.

use std::fmt;

use chacha20poly1305::ChaCha20Poly1305;
use chacha20poly1305::KeyInit;
use chacha20poly1305::aead::Aead;
use chacha20poly1305::aead::AeadCore;
use chacha20poly1305::aead::OsRng;
use chacha20poly1305::aead::Payload;
use sqlite_rs::Executor;
use sqlite_rs::QueryExecutor;

use crate::error::Error;
use crate::error::SecretFault;
use crate::session::Session;
use crate::sqlite::queries;

/// The only algorithm: ChaCha20-Poly1305 with a random 12-byte nonce, as `secrets_rs`'s
/// `encrypt_blob`. Kept in `kh_dek.algorithm` so a second one is a migration, not a guess.
const ALGORITHM: &str = "chacha20poly1305";

/// The key that wraps the DEKs: 32 bytes the embedder keeps outside the database (an OS keyring, a
/// KMS, a file elsewhere), and the version its rotation numbers it by.
#[derive(Clone)]
pub struct Kek {
    pub version: u64,
    key: [u8; 32],
}

impl Kek {
    pub fn new(version: u64, key: [u8; 32]) -> Self {
        Self { version, key }
    }
}

impl fmt::Debug for Kek {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Kek")
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

/// Where the embedder's key-encryption keys come from. The store asks for the current one to wrap a
/// new DEK, and for the one a DEK row names to unwrap an existing DEK, so a rotation keeps the
/// versions it has not yet re-wrapped (see [`Session::rewrap_deks`]).
pub trait KekSource: Send + Sync + 'static {
    /// The key new wraps are made under.
    fn current(&self) -> Kek;

    /// The key of this version, or `None` if the embedder no longer holds it.
    fn get(&self, version: u64) -> Option<Kek>;
}

/// A single key with no history.
impl KekSource for Kek {
    fn current(&self) -> Kek {
        self.clone()
    }

    fn get(&self, version: u64) -> Option<Kek> {
        (version == self.version).then(|| self.clone())
    }
}

/// Which secret column a DEK serves. One DEK per kind, so a kind re-keys independently.
#[derive(Clone, Copy)]
pub(crate) enum Kind {
    KeyPair,
    ContentKey,
}

impl Kind {
    pub(crate) fn dek_id(self) -> &'static str {
        match self {
            Kind::KeyPair => "key_pair",
            Kind::ContentKey => "content_key",
        }
    }
}

/// A DEK in the clear, for the length of one call.
pub(crate) struct Dek {
    pub(crate) id: String,
    pub(crate) version: i64,
    key: [u8; 32],
}

/// `secret` sealed under `dek`, bound to `aad`: the ciphertext and its nonce.
pub(crate) fn seal(dek: &Dek, aad: &[u8], plaintext: &[u8]) -> (Vec<u8>, Vec<u8>) {
    seal_under(&dek.key, aad, plaintext)
}

fn seal_under(key: &[u8; 32], aad: &[u8], plaintext: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let cipher = ChaCha20Poly1305::new(key.into());
    let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);
    let sealed = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .expect("encrypting into a vector cannot fail");
    (sealed, nonce.to_vec())
}

fn open_under(
    key: &[u8; 32],
    aad: &[u8],
    nonce: &[u8],
    sealed: &[u8],
) -> Result<Vec<u8>, SecretFault> {
    let nonce: &[u8; 12] = nonce.try_into().expect("the nonce column is 12 bytes");
    ChaCha20Poly1305::new(key.into())
        .decrypt(nonce.into(), Payload { msg: sealed, aad })
        .map_err(|_| SecretFault::Open)
}

pub(crate) fn open(
    dek: &Dek,
    aad: &[u8],
    nonce: &[u8],
    sealed: &[u8],
) -> Result<Vec<u8>, SecretFault> {
    open_under(&dek.key, aad, nonce, sealed)
}

/// What binds a wrapped DEK to its row.
fn dek_aad(id: &str, version: i64) -> Vec<u8> {
    let mut aad = b"kh_dek\0".to_vec();
    aad.extend_from_slice(id.as_bytes());
    aad.push(0);
    aad.extend_from_slice(&version.to_be_bytes());
    aad
}

fn unwrap_dek(
    keks: &dyn KekSource,
    id: &str,
    version: i64,
    row: (&str, &[u8], &[u8], i64),
) -> Result<Dek, SecretFault> {
    let (algorithm, nonce, wrapped, kek_version) = row;
    if algorithm != ALGORITHM {
        return Err(SecretFault::Algorithm(algorithm.to_owned()));
    }
    let kek_version = u64::try_from(kek_version).expect("a stored kek version is not negative");
    let kek = keks
        .get(kek_version)
        .ok_or(SecretFault::NoKek(kek_version))?;
    let key = open_under(&kek.key, &dek_aad(id, version), nonce, wrapped)?;
    let key: [u8; 32] = key.try_into().map_err(|_| SecretFault::Shape)?;
    Ok(Dek {
        id: id.to_owned(),
        version,
        key,
    })
}

impl<T, E: QueryExecutor> Session<'_, T, E> {
    /// The DEK a row names, unwrapped.
    pub(crate) async fn dek(
        &self,
        table: &'static str,
        id: &str,
        version: i64,
    ) -> Result<Dek, Error> {
        let row = queries::dek_get(self.tx, id.to_owned(), version).await?;
        let fault = |source| Error::Secret { table, source };
        let Some(row) = row else {
            return Err(fault(SecretFault::NoDek {
                id: id.to_owned(),
                version,
            }));
        };
        unwrap_dek(
            self.keks,
            id,
            version,
            (
                &row.algorithm,
                &row.nonce,
                &row.wrapped_dek,
                row.kek_version,
            ),
        )
        .map_err(fault)
    }
}

impl<T, E: Executor> Session<'_, T, E> {
    /// The newest DEK of `kind`, made (and wrapped under the current KEK) in this transaction if
    /// there is none. The cleartext key is never cached across calls: a DEK created in a
    /// transaction that rolls back must not outlive it.
    pub(crate) async fn dek_for_write(
        &self,
        table: &'static str,
        kind: Kind,
    ) -> Result<Dek, Error> {
        let id = kind.dek_id();
        if let Some(row) = queries::dek_latest(self.tx, id.to_owned()).await? {
            return unwrap_dek(
                self.keks,
                id,
                row.dek_version,
                (
                    &row.algorithm,
                    &row.nonce,
                    &row.wrapped_dek,
                    row.kek_version,
                ),
            )
            .map_err(|source| Error::Secret { table, source });
        }
        let key: [u8; 32] = ChaCha20Poly1305::generate_key(&mut OsRng).into();
        let kek = self.keks.current();
        let (wrapped, nonce) = seal_under(&kek.key, &dek_aad(id, 1), &key);
        queries::dek_insert(
            self.tx,
            id.to_owned(),
            1,
            ALGORITHM.to_owned(),
            nonce,
            wrapped,
            i64::try_from(kek.version).expect("a kek version fits SQLite's integer"),
        )
        .await?;
        Ok(Dek {
            id: id.to_owned(),
            version: 1,
            key,
        })
    }

    /// Re-wrap every DEK that a key-encryption key older than the current one wraps, under the
    /// current one: the incremental half of a KEK rotation, after which the old KEK can be retired.
    /// Returns how many it re-wrapped. No secret row is touched; the DEKs do not change.
    pub async fn rewrap_deks(&self) -> Result<usize, Error> {
        let current = self.keks.current();
        let current_version =
            i64::try_from(current.version).expect("a kek version fits SQLite's integer");
        let stale = queries::deks_stale(self.tx, current_version).await?;
        for row in &stale {
            let dek = unwrap_dek(
                self.keks,
                &row.dek_id,
                row.dek_version,
                (
                    &row.algorithm,
                    &row.nonce,
                    &row.wrapped_dek,
                    row.kek_version,
                ),
            )
            .map_err(|source| Error::Secret {
                table: "kh_dek",
                source,
            })?;
            let aad = dek_aad(&row.dek_id, row.dek_version);
            let (wrapped, nonce) = seal_under(&current.key, &aad, &dek.key);
            queries::dek_rewrap(
                self.tx,
                nonce,
                wrapped,
                current_version,
                row.dek_id.clone(),
                row.dek_version,
            )
            .await?;
        }
        Ok(stale.len())
    }
}
