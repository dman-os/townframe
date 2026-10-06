//! Bounded current-state and single-key provenance attestations, never history arrays.
use crate::interlude::*;
use big_repo::keyhive_core::access::Access;
use big_repo::{CoordinationAuthority, CoordinationError};
use big_sync_core::{PeerKey, encrypted_register::SignatureBytes};
use ed25519_dalek::Signer;
use keyhive_crypto::verifiable::Verifiable;
use serde::de::{DeserializeOwned, Error as _, SeqAccess, Visitor};
use std::io::Write;
use utils_rs::prelude::eyre::ensure;

pub(crate) const MAX_HEADS: usize = 64;
const MAX_BYTES: usize = 16 * 1024;
pub(crate) const POOL_DOMAIN: &[u8] = b"daybook/task-pool-current-control/v2\0";
pub(crate) const PROCESSOR_DOMAIN: &[u8] = b"daybook/processor-slot-current-control/v2\0";
const SOURCE_DOMAIN: &[u8] = b"daybook/register-source-control/v1\0";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RegisterSource {
    pub scope: Vec<u8>,
    pub incarnation: [u8; 32],
    pub key_ref: Url,
}

#[derive(Serialize, Deserialize)]
struct ControlBody<S, T> {
    source: S,
    current: T,
}

/// One authenticated key provenance statement, never transport history.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RegisterSourceControl(SignedCurrentControl);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SignedCurrentControl {
    #[serde(deserialize_with = "read_statement")]
    statement: Vec<u8>,
    publisher: [u8; 32],
    signature: SignatureBytes,
}

fn read_statement<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    struct Bytes;
    impl<'de> Visitor<'de> for Bytes {
        type Value = Vec<u8>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded current-control transcript")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Vec<u8>, A::Error> {
            let length = sequence.size_hint().unwrap_or(0);
            if length > MAX_BYTES {
                return Err(A::Error::custom("current control exceeds byte budget"));
            }
            let mut bytes = Vec::with_capacity(length);
            while let Some(byte) = sequence.next_element::<u8>()? {
                if bytes.len() == MAX_BYTES {
                    return Err(A::Error::custom("current control exceeds byte budget"));
                }
                bytes.push(byte);
            }
            Ok(bytes)
        }
    }
    deserializer.deserialize_seq(Bytes)
}

struct StatementWriter<'a>(&'a mut Vec<u8>);
impl Write for StatementWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_BYTES - self.0.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "current control exceeds byte budget",
            ));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl SignedCurrentControl {
    pub(crate) fn statement(
        domain: &[u8],
        document: &DocumentId,
        group: &[u8; 32],
        source: (&[u8], &[u8; 32], &Url),
        body: &impl Serialize,
    ) -> Result<Vec<u8>, CoordinationError> {
        let document = document
            .to_bytes32()
            .map_err(|_| CoordinationError::Invalid("invalid control document"))?;
        let mut statement = Vec::new();
        statement.extend_from_slice(domain);
        statement.extend_from_slice(&document);
        statement.extend_from_slice(group);
        #[derive(Serialize)]
        struct Source<'a> {
            scope: &'a [u8],
            incarnation: &'a [u8; 32],
            key_ref: &'a Url,
        }
        let body = ControlBody {
            source: Source {
                scope: source.0,
                incarnation: source.1,
                key_ref: source.2,
            },
            current: body,
        };
        serde_json::to_writer(StatementWriter(&mut statement), &body).map_err(|error| {
            if error.is_io() {
                CoordinationError::Invalid("current control exceeds byte budget")
            } else {
                CoordinationError::Other(error.into())
            }
        })?;
        Ok(statement)
    }

    fn locus(domain: &[u8], statement: &[u8]) -> Result<(DocumentId, [u8; 32]), CoordinationError> {
        let header = domain.len() + 64;
        if statement.len() <= header
            || statement.len() > MAX_BYTES
            || !statement.starts_with(domain)
        {
            return Err(CoordinationError::Invalid(
                "invalid current-control framing",
            ));
        }
        Ok((
            DocumentId::new(&statement[domain.len()..domain.len() + 32]),
            statement[domain.len() + 32..header]
                .try_into()
                .expect("checked group width"),
        ))
    }

    pub(crate) async fn sign(
        domain: &[u8],
        repo: &big_repo::BigRepo,
        statement: Vec<u8>,
        peer: &PeerKey,
        authority: &CoordinationAuthority,
    ) -> Result<Self, CoordinationError> {
        let (document, group) = Self::locus(domain, &statement)?;
        if authority.document() != &document || authority.group() != &group {
            return Err(CoordinationError::Invalid("control authority mismatch"));
        }
        if !authority
            .effective_agents()
            .iter()
            .any(|(agent, access)| agent.as_slice() == peer.as_bytes() && *access >= Access::Relay)
        {
            return Err(CoordinationError::Unauthorized);
        }
        // The signer rechecks this same graph generation, including requester admission.
        repo.with_coordination_signer(authority, |signer| {
            let signature = signer
                .try_sign(&statement)
                .map_err(|_| CoordinationError::Invalid("control signing failed"))?
                .to_bytes();
            Ok(Self {
                statement,
                publisher: signer.verifying_key().to_bytes(),
                signature: SignatureBytes {
                    r: signature[..32].try_into().expect("signature width"),
                    s: signature[32..].try_into().expect("signature width"),
                },
            })
        })
        .await
    }

    pub(crate) fn decode<T: DeserializeOwned>(
        &self,
        domain: &[u8],
        group: [u8; 32],
    ) -> Res<(DocumentId, RegisterSource, T)> {
        let (document, signed_group) = Self::locus(domain, &self.statement)?;
        ensure!(
            signed_group == group,
            "control names another sponsored authority"
        );
        ed25519_dalek::VerifyingKey::from_bytes(&self.publisher)?.verify_strict(
            &self.statement,
            &ed25519_dalek::Signature::from_components(self.signature.r, self.signature.s),
        )?;
        let body: ControlBody<RegisterSource, T> =
            serde_json::from_slice(&self.statement[domain.len() + 64..])?;
        Ok((document, body.source, body.current))
    }

    pub(crate) async fn admit(
        &self,
        repo: &big_repo::BigRepo,
        document: DocumentId,
        group: [u8; 32],
        peer: Option<&PeerKey>,
    ) -> Res<CoordinationAuthority> {
        let authority = repo.coordination_authority(document, group).await?;
        if !authority.edit_agents().contains(&self.publisher)
            || peer.is_some_and(|peer| {
                !authority.effective_agents().iter().any(|(agent, access)| {
                    agent.as_slice() == peer.as_bytes() && *access >= Access::Relay
                })
            })
        {
            return Err(CoordinationError::Unauthorized.into());
        }
        Ok(repo
            .admit_coordination_access(&authority, Access::Relay)
            .await?)
    }
}

impl RegisterSourceControl {
    fn domain(&self) -> Res<&'static [u8]> {
        [POOL_DOMAIN, PROCESSOR_DOMAIN, SOURCE_DOMAIN]
            .into_iter()
            .find(|domain| self.0.statement.starts_with(domain))
            .ok_or_else(|| eyre::eyre!("unknown source control domain"))
    }

    pub(crate) fn decode(&self, group: [u8; 32]) -> Res<(DocumentId, RegisterSource)> {
        let domain = self.domain()?;
        let (document, source, _): (_, _, serde::de::IgnoredAny) = self.0.decode(domain, group)?;
        ensure!(
            !source.scope.is_empty() && source.scope.len() <= 4096,
            "source scope exceeds budget"
        );
        let reference = daybook_types::url::parse_facet_ref(&source.key_ref)?;
        ensure!(
            reference.branch.is_none()
                && reference.at.is_none()
                && reference.facet_key.tag
                    == daybook_types::doc::FacetTag::WellKnown(
                        daybook_types::doc::WellKnownFacetTag::Jwk
                    )
                && daybook_types::url::build_facet_ref(
                    reference.doc_id.as_str(),
                    &reference.facet_key
                )? == source.key_ref,
            "source must name a canonical native JWK facet"
        );
        let key_document: DocumentId = reference.doc_id.as_str().parse()?;
        key_document.to_bytes32()?;
        ensure!(
            domain == SOURCE_DOMAIN || key_document == document,
            "current control source is not its publication key"
        );
        Ok((document, source))
    }

    pub(crate) async fn admit(
        &self,
        repo: &big_repo::BigRepo,
        group: [u8; 32],
        peer: Option<&PeerKey>,
        expected: (&[u8], &[u8; 32], &Url),
    ) -> Res<RegisterSource> {
        let (document, source) = self.decode(group)?;
        if source.scope != expected.0
            || &source.incarnation != expected.1
            || &source.key_ref != expected.2
        {
            return Err(CoordinationError::Invalid("source proof binding mismatch").into());
        }
        // Provenance survives later selections. Current Edit on the original
        // native document/group remains mandatory; this proof never selects transport.
        let publisher = self.0.admit(repo, document, group, peer).await?;
        let reference = daybook_types::url::parse_facet_ref(&source.key_ref)?;
        let authority = repo
            .coordination_authority(reference.doc_id.as_str().parse()?, group)
            .await?;
        repo.admit_coordination_access(&authority, Access::Relay)
            .await?;
        // The source-document await must not outlive its publisher's sampled grant.
        repo.admit_coordination_access(&publisher, Access::Relay)
            .await?;
        Ok(source)
    }

    pub(crate) async fn sign(
        repo: &big_repo::BigRepo,
        authority: &CoordinationAuthority,
        peer: &PeerKey,
        source: &RegisterSource,
    ) -> Res<Self> {
        let statement = SignedCurrentControl::statement(
            SOURCE_DOMAIN,
            authority.document(),
            authority.group(),
            (&source.scope, &source.incarnation, &source.key_ref),
            &(),
        )?;
        Ok(Self(
            SignedCurrentControl::sign(SOURCE_DOMAIN, repo, statement, peer, authority).await?,
        ))
    }
}
