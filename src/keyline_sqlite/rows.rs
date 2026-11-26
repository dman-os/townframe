//! Certificates to rows and back.

use keyhive_codec::traits::Decode;
use keyhive_codec::traits::Encode;
use keyhive_crypto::digest::Digest;
use keyhive_crypto::identifier::Identifier;
use keyline::certificate::Certificate;
use keyline::certificate::CertificateId;
use keyline::certificate::VerifiedCertificate;
use keyline::power::Power;
use keyline::revocation::Revocation;
use sqlite_rs::BindTuple;
use sqlite_rs::Sql;

use crate::error::Error;
use crate::error::StoredFault;

pub const KIND_DELEGATION: i64 = 0;
pub const KIND_REVOCATION: i64 = 1;

/// The columns of one `keyline_certificates` row, in table order.
pub struct CertificateRow {
    kind: i64,
    digest: Vec<u8>,
    issuer: Vec<u8>,
    audience: Option<Vec<u8>>,
    subject: Option<Vec<u8>>,
    power: Option<i64>,
    citation: Option<Vec<u8>>,
    revoke: Option<Vec<u8>>,
    retain: Option<Vec<u8>>,
    signed: Vec<u8>,
}

impl CertificateRow {
    /// Variables a row binds.
    pub const WIDTH: usize = 10;

    pub fn of<W: Encode>(cert: &VerifiedCertificate<W>) -> Self {
        let (kind, digest) = kind_and_digest(cert.id());
        let issuer = cert.issuer().to_bytes().to_vec();
        let signed = cert.clone().into_certificate().encode().as_bytes().to_vec();
        match cert {
            VerifiedCertificate::Delegation(delegation) => {
                let delegation = delegation.payload();
                Self {
                    kind,
                    digest,
                    issuer,
                    audience: Some(delegation.audience.to_bytes().to_vec()),
                    subject: Some(delegation.subject.to_bytes().to_vec()),
                    power: Some(power_to_sql(delegation.power)),
                    citation: delegation
                        .citation
                        .map(|citation| citation.as_slice().to_vec()),
                    revoke: None,
                    retain: None,
                    signed,
                }
            }
            VerifiedCertificate::Revocation(revocation) => {
                let revocation = revocation.payload();
                Self {
                    kind,
                    digest,
                    issuer,
                    audience: None,
                    subject: None,
                    power: None,
                    citation: None,
                    revoke: Some(revocation.revoke.as_slice().to_vec()),
                    retain: retain_to_sql(revocation),
                    signed,
                }
            }
        }
    }
}

impl BindTuple for CertificateRow {
    fn bind_tuple(self, sql: &mut Sql) {
        (
            self.kind,
            self.digest,
            self.issuer,
            self.audience,
            self.subject,
            self.power,
            self.citation,
            self.revoke,
            self.retain,
            self.signed,
        )
            .bind_tuple(sql);
    }
}

/// The watermarks of a revocation as the `retain` column holds them, or `None` for no
/// watermarks.
fn retain_to_sql<W: Encode>(revocation: &Revocation<W>) -> Option<Vec<u8>> {
    if revocation.retain.is_empty() {
        return None;
    }
    let mut out = Vec::new();
    for (subject, watermark) in &revocation.retain {
        let watermark = watermark.encode();
        out.extend_from_slice(subject.as_bytes());
        out.extend_from_slice(
            &u32::try_from(watermark.as_bytes().len())
                .expect("a watermark fits a u32 length")
                .to_be_bytes(),
        );
        out.extend_from_slice(watermark.as_bytes());
    }
    Some(out)
}

pub fn kind_and_digest(id: CertificateId) -> (i64, Vec<u8>) {
    match id {
        CertificateId::Delegation(digest) => (KIND_DELEGATION, digest.as_slice().to_vec()),
        CertificateId::Revocation(digest) => (KIND_REVOCATION, digest.as_slice().to_vec()),
    }
}

/// The identity a row names. Rows are written by this crate, so a bad kind or digest length is
/// a broken invariant.
pub fn id_from_sql(kind: i64, digest: &[u8]) -> CertificateId {
    let digest: [u8; 32] = digest.try_into().expect("a stored digest is 32 bytes");
    match kind {
        KIND_DELEGATION => CertificateId::Delegation(Digest::from(digest)),
        KIND_REVOCATION => CertificateId::Revocation(Digest::from(digest)),
        other => unreachable!("stored certificate kind {other}"),
    }
}

pub fn digest_from_sql<T>(digest: &[u8]) -> Digest<T> {
    Digest::from(<[u8; 32]>::try_from(digest).expect("a stored digest is 32 bytes"))
}

pub fn identifier_from_sql(identifier: &[u8]) -> Identifier {
    Identifier::from_bytes(
        identifier
            .try_into()
            .expect("a stored identifier is 32 bytes"),
    )
    .expect("a stored identifier was a valid key when it was inserted")
}

pub fn power_to_sql(power: Power) -> i64 {
    i64::try_from(power.rank()).expect("a rank is a small integer")
}

pub fn power_from_sql(rank: i64) -> Power {
    usize::try_from(rank)
        .ok()
        .and_then(|rank| Power::ALL.get(rank))
        .copied()
        .unwrap_or_else(|| unreachable!("stored power rank {rank}"))
}

/// The certificate as received, from a `signed` column. Checks the framing only, as a
/// certificate off the wire is checked before its signature is.
pub fn certificate_from_sql<W: Decode>(signed: &[u8]) -> Certificate<W> {
    Certificate::decode(signed).expect("a stored certificate is a certificate frame")
}

/// A stored certificate, verified again: decoded canonically and checked against its issuer's
/// signature and the identity of its row.
pub fn verified_from_sql<W: Encode + Decode>(
    kind: i64,
    digest: &[u8],
    signed: &[u8],
) -> Result<VerifiedCertificate<W>, Error> {
    let row = id_from_sql(kind, digest);
    let fault = |source: StoredFault| Error::Stored { id: row, source };
    let certificate = Certificate::<W>::decode(signed).map_err(|e| fault(e.into()))?;
    let verified = certificate.verify().map_err(|e| fault(e.into()))?;
    if verified.id() != row {
        return Err(fault(StoredFault::Identity {
            found: verified.id(),
        }));
    }
    Ok(verified)
}
