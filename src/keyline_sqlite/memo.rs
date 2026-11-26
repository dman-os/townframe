//! The verification memo: which stored rows this handle has already verified.
//!
//! A row read from the tables is not a witness (`rows::verified_from_sql` decodes it, checks the
//! signature and compares the identity), and a point query reads a whole closure, so paying one
//! signature check per row per query would make the evaluation's cost the verification's. The memo
//! keeps the verified certificate beside the exact bytes it was verified from, and trusts a row
//! only if its bytes are those bytes. Certificates are immutable and content-addressed, so the memo
//! is a cache of a pure function and needs no version: a row edited on disk no longer matches and is
//! verified again, which is how an outside modification still surfaces.
//!
//! It is bounded, and emptied when full: it holds no state a query could not rebuild.

use std::collections::HashMap;
use std::sync::Mutex;

use keyhive_codec::traits::Decode;
use keyhive_codec::traits::Encode;
use keyline::certificate::CertificateId;
use keyline::certificate::VerifiedCertificate;

use crate::error::Error;
use crate::rows;

/// Entries held before the memo empties itself. Several hundred bytes each.
const CAPACITY: usize = 65_536;

type Key = (i64, [u8; 32]);

struct Entry<W> {
    signed: Vec<u8>,
    certificate: VerifiedCertificate<W>,
}

pub struct Memo<W> {
    entries: Mutex<HashMap<Key, Entry<W>>>,
}

impl<W> Default for Memo<W> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }
}

const ERROR_MUTEX: &str = "the memo mutex is not poisoned: nothing panics under it";

fn key(id: CertificateId) -> Key {
    let (kind, digest) = rows::kind_and_digest(id);
    (
        kind,
        digest.try_into().expect("a digest is 32 bytes"),
    )
}

impl<W: Encode + Decode> Memo<W> {
    /// The certificate a row holds, verified: from the memo if it has verified these bytes, else
    /// by verifying them now.
    pub fn verified(
        &self,
        kind: i64,
        digest: &[u8],
        signed: &[u8],
    ) -> Result<VerifiedCertificate<W>, Error> {
        let id = rows::id_from_sql(kind, digest);
        if let Some(entry) = self.entries.lock().expect(ERROR_MUTEX).get(&key(id))
            && entry.signed == signed
        {
            return Ok(entry.certificate.clone());
        }
        let certificate = rows::verified_from_sql::<W>(kind, digest, signed)?;
        self.remember(key(id), signed.to_vec(), certificate.clone());
        Ok(certificate)
    }

    /// Certificates a transaction just inserted, with the bytes it wrote: already verified, so the
    /// first read need not.
    pub fn warm(&self, batch: Vec<(Vec<u8>, VerifiedCertificate<W>)>) {
        for (signed, certificate) in batch {
            self.remember(key(certificate.id()), signed, certificate);
        }
    }

    fn remember(&self, key: Key, signed: Vec<u8>, certificate: VerifiedCertificate<W>) {
        let mut entries = self.entries.lock().expect(ERROR_MUTEX);
        if entries.len() >= CAPACITY {
            entries.clear();
        }
        entries.insert(key, Entry { signed, certificate });
    }
}
