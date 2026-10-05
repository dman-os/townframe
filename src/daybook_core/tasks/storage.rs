//! Durable authenticated current registers. Physical scope retirement is not implemented here.
//!
//! The store owns the checked BigRepo boundary: callers cannot inject writer identities,
//! SQL scopes or reducer admissions. Crypto awaits precede SQLite transactions. Current
//! state and its BigSync publication commit together, so restart needs no publication scan.

use std::sync::Arc;

use big_repo::keyhive_core::access::Access;
use big_repo::{BigRepo, CoordinationAuthority};
use big_sync::{HostPartStore, LocalPartRevisionReader, SqliteObjWrite, SqlitePartStore};
use big_sync_core::{
    ObjKey, PartKey,
    part_store::CursorIndex,
    rpc::{SubPartsRequest, SubscriptionTarget},
};
use keyhive_crypto::verifiable::Verifiable;
use sqlx::{Sqlite, Transaction};
use utils_rs::prelude::eyre::ensure;
use utils_rs::prelude::*;

use crate::blobs::encrypt::{
    CONTENT_ENCODING_AES128GCM, EncodingParams, JwkOct, MasterKey, decrypt_bytes, encrypt_with_rs,
};
use big_sync_core::encrypted_register::{
    CausalVersion, EncryptedRegister, LaneState, Limits, MergeOutcome, OriginalStatement,
    RegisterKey, RegisterSnapshot, Representation, RepresentationBinding,
};
use daybook_types::doc::{ChangeHashSet, FacetTag, WellKnownFacet, WellKnownFacetTag};
use std::collections::BTreeSet;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PinnedJwk {
    pub key_ref: Url,
    pub heads: ChangeHashSet,
}

/// Domain-provisioned scope and allowed key slots; opening never creates a key.
pub struct RegisterBinding {
    pub scope: Vec<u8>,
    /// Explicit current transport part selected by the owning domain descriptor.
    /// Scope and cryptographic incarnation do not choose subscription identity.
    pub part: PartKey,
    pub authority: CoordinationAuthority,
    pub allowed_key_refs: BTreeSet<Url>,
    pub publication_key: PinnedJwk,
    pub incarnation: [u8; 32],
}

impl RegisterBinding {
    pub(crate) fn validate_keys(
        document: &big_repo::DocumentId,
        scope: &[u8],
        allowed: &BTreeSet<Url>,
        publication: &PinnedJwk,
    ) -> Res<()> {
        ensure!(
            !scope.is_empty() && scope.len() <= MAX_SLOT_BYTES,
            "register scope exceeds budget"
        );
        let document_id = document.to_string();
        for key_ref in allowed {
            let reference = daybook_types::url::parse_facet_ref(key_ref)?;
            let canonical = daybook_types::url::build_facet_ref(
                reference.doc_id.as_str(),
                &reference.facet_key,
            )?;
            ensure!(
                reference.doc_id.as_str() == document_id
                    && reference.branch.is_none()
                    && reference.at.is_none()
                    && reference.facet_key.tag == FacetTag::WellKnown(WellKnownFacetTag::Jwk)
                    && canonical == *key_ref,
                "pool key reference must name a canonical JWK facet in its native authority document"
            );
        }
        ensure!(
            allowed.contains(&publication.key_ref),
            "publication key outside pool key slots"
        );
        ensure!(
            !publication.heads.0.is_empty(),
            "publication key must pin document heads"
        );
        Ok(())
    }
}

const MAX_CIPHERTEXT_BYTES: usize = 1024 * 1024;
const MAX_FRONTIER_ENTRIES: usize = 64;
const MAX_SLOT_BYTES: usize = 4096;

/// One checked authority document/group, in one existing SQLite part-store scope.
/// Incarnation is configured transport identity, not a scope-retirement certificate.
pub struct RegisterStore {
    parts: Arc<SqlitePartStore>,
    repo: Arc<BigRepo>,
    authority: CoordinationAuthority,
    scope: Vec<u8>,
    allowed_key_refs: BTreeSet<Url>,
    publication_key: tokio::sync::RwLock<PinnedJwk>,
    incarnation: [u8; 32],
    part: PartKey,
}

// Boxing this short-lived stack value would add an allocation to every write.
#[expect(clippy::large_enum_variant)]
enum Incoming {
    Local(Representation),
    Remote(RegisterSnapshot),
}

impl RegisterStore {
    pub async fn open(
        parts: Arc<SqlitePartStore>,
        repo: Arc<BigRepo>,
        binding: RegisterBinding,
    ) -> Res<Self> {
        let RegisterBinding {
            scope,
            part,
            authority,
            allowed_key_refs,
            publication_key,
            incarnation,
        } = binding;
        // Native facets avoid Drawer registration's additional admin grants.
        RegisterBinding::validate_keys(
            authority.document(),
            &scope,
            &allowed_key_refs,
            &publication_key,
        )?;
        let sampled = repo
            .coordination_authority(authority.document().clone(), *authority.group())
            .await?;
        let authority = repo
            .admit_coordination_access(&sampled, Access::Relay)
            .await?;
        let store = Self {
            parts,
            repo,
            scope,
            authority,
            allowed_key_refs,
            publication_key: tokio::sync::RwLock::new(publication_key),
            incarnation,
            part,
        };
        let mut write = store.parts.begin_obj_write(store.control_key()).await?;
        for query in [
            "CREATE TABLE IF NOT EXISTS encrypted_register_current (
                scope_id INTEGER NOT NULL
              , logical_key BLOB NOT NULL
              , snapshot_json TEXT NOT NULL
              , PRIMARY KEY(scope_id, logical_key)
            )",
            "CREATE TABLE IF NOT EXISTS encrypted_register_sequence (
                register_scope BLOB NOT NULL
              , writer BLOB NOT NULL
              , next_sequence INTEGER NOT NULL
              , PRIMARY KEY(register_scope, writer)
            )",
            "CREATE TABLE IF NOT EXISTS encrypted_register_consumer (
                scope_id INTEGER NOT NULL
              , consumer BLOB NOT NULL
              , selector BLOB NOT NULL
              , through_revision INTEGER NOT NULL
              , PRIMARY KEY(scope_id, consumer)
            )",
        ] {
            sqlx::query(query)
                .execute(&mut **write.context_mut())
                .await?;
        }
        write.commit().await?;
        Ok(store)
    }

    fn control_key(&self) -> ObjKey {
        ObjKey::new(self.part.as_bytes())
    }

    /// Refresh provisioned key metadata without splitting the retained owner.
    /// Opaque relays retain key references/heads, not key bytes. Publication and
    /// plaintext release independently resolve keys under their Read/Edit gates.
    pub(crate) async fn refresh_publication_key(&self, binding: &RegisterBinding) -> Res<()> {
        ensure!(
            binding.scope == self.scope
                && binding.part == self.part
                && binding.incarnation == self.incarnation
                && binding.authority.document() == self.authority.document()
                && binding.authority.group() == self.authority.group()
                && binding.allowed_key_refs == self.allowed_key_refs,
            "register authority, transport, or key slots changed; explicit migration is required"
        );
        self.fresh_authority(Access::Relay).await?;
        let mut publication = self.publication_key.write().await;
        if *publication != binding.publication_key {
            publication.clone_from(&binding.publication_key);
        }
        Ok(())
    }
    pub fn part(&self) -> &PartKey {
        &self.part
    }

    pub fn key(&self, slot: &[u8]) -> Res<RegisterKey> {
        ensure!(
            slot.len() <= MAX_SLOT_BYTES,
            "coordination slot exceeds budget"
        );
        Ok(RegisterKey {
            scope: self.scope.clone(),
            slot: slot.to_vec(),
        })
    }

    fn limits(&self, key: RegisterKey) -> Limits {
        Limits {
            key,
            max_key_bytes: MAX_SLOT_BYTES,
            max_ciphertext_bytes: MAX_CIPHERTEXT_BYTES,
            max_frontier_entries: MAX_FRONTIER_ENTRIES,
            max_metadata_bytes: 16 * 1024,
        }
    }

    async fn resolve_key(&self, pinned: &PinnedJwk) -> Res<MasterKey> {
        ensure!(
            self.allowed_key_refs.contains(&pinned.key_ref),
            "key reference outside configured domain slots"
        );
        ensure!(
            !pinned.heads.0.is_empty(),
            "key reference must pin document heads"
        );
        let reference = daybook_types::url::parse_facet_ref(&pinned.key_ref)?;
        ensure!(
            reference.facet_key.tag == FacetTag::WellKnown(WellKnownFacetTag::Jwk),
            "key reference must name a JWK facet"
        );
        ensure!(
            reference.at.is_none(),
            "key heads belong in the explicit pinned-heads field"
        );
        let handle = match self.repo.get_doc(self.authority.document()).await? {
            big_repo::DocLookup::Ready(handle) if !handle.is_partially_decrypted() => handle,
            big_repo::DocLookup::Ready(_)
            | big_repo::DocLookup::Missing
            | big_repo::DocLookup::PendingMaterialization => {
                eyre::bail!("pinned pool key document unavailable");
            }
        };
        let raw = handle
            .hydrate_path_at_heads::<am_utils_rs::codecs::ThroughJson<serde_json::Value>>(
                &pinned.heads.0,
                automerge::ROOT,
                vec![
                    "facets".into(),
                    autosurgeon::Prop::Key(reference.facet_key.to_string().into()),
                ],
            )
            .await?
            .ok_or_else(|| ferr!("pinned pool key facet unavailable"))?;
        let WellKnownFacet::Jwk(jwk) = WellKnownFacet::from_json(raw.0, WellKnownFacetTag::Jwk)?
        else {
            unreachable!("JWK decoder returns JWK")
        };
        serde_json::from_value::<JwkOct>(serde_json::to_value(jwk)?)?.to_master_key()
    }

    fn check_binding(&self, representation: &Representation) -> Res<()> {
        ensure!(
            representation.original.key.scope == self.scope
                && representation.binding.incarnation == self.incarnation,
            "register transport binding mismatch"
        );
        let key_ref = Url::parse(std::str::from_utf8(&representation.binding.key_ref)?)?;
        ensure!(
            self.allowed_key_refs.contains(&key_ref),
            "key reference outside configured domain slots"
        );
        ensure!(
            !representation.binding.key_heads.is_empty(),
            "key reference pins no heads"
        );
        EncodingParams::from_encoding_parameters(
            std::str::from_utf8(&representation.binding.encoding)?,
            &serde_json::from_slice(&representation.binding.parameters)?,
        )?;
        Ok(())
    }

    async fn fresh_authority(&self, required: Access) -> Res<CoordinationAuthority> {
        let sampled = self
            .repo
            .coordination_authority(self.authority.document().clone(), *self.authority.group())
            .await
            .wrap_err_with(|| {
                format!(
                    "sampling {required:?} coordination authority for {}",
                    self.authority.document()
                )
            })?;
        self.repo
            .admit_coordination_access(&sampled, required)
            .await
            .wrap_err_with(|| {
                format!(
                    "admitting {required:?} coordination authority for {}",
                    self.authority.document()
                )
            })
    }

    /// Checked local publisher identity; no signing key escapes this boundary.
    pub async fn local_writer(&self) -> Res<[u8; 32]> {
        let authority = self.fresh_authority(Access::Edit).await?;
        Ok(self
            .repo
            .with_coordination_signer(&authority, |signer| Ok(signer.verifying_key().to_bytes()))
            .await?)
    }

    async fn reserve_sequence(&self, writer: [u8; 32]) -> Res<u64> {
        let mut write = self.parts.begin_obj_write(self.control_key()).await?;
        let next: i64 = sqlx::query_scalar(
            "INSERT INTO encrypted_register_sequence(
                register_scope
              , writer
              , next_sequence
            ) VALUES (?, ?, 2)
            ON CONFLICT(register_scope, writer)
            DO UPDATE SET next_sequence = next_sequence + 1
            RETURNING next_sequence - 1",
        )
        .bind(&self.scope)
        .bind(writer.as_slice())
        .fetch_one(&mut **write.context_mut())
        .await?;
        ensure!(next > 0, "coordination writer sequence exhausted");
        write.commit().await?;
        Ok(u64::try_from(next).expect("positive writer sequence"))
    }

    /// Reserves sequence before signing; interrupted/failed publications leave gaps, never reuse.
    /// Frontier is explicit domain-owned causal knowledge, not an inferred historical DAG.
    pub async fn publish_local(
        &self,
        slot: &[u8],
        frontier: Vec<CausalVersion>,
        body: Vec<u8>,
    ) -> Res<MergeOutcome> {
        ensure!(
            body.len() <= MAX_CIPHERTEXT_BYTES / 2,
            "coordination body exceeds budget"
        );
        ensure!(
            frontier.len() <= MAX_FRONTIER_ENTRIES,
            "coordination frontier exceeds budget"
        );
        let key = self.key(slot)?;
        let authority = self.fresh_authority(Access::Edit).await?;
        let writer = self
            .repo
            .with_coordination_signer(&authority, |signer| Ok(signer.verifying_key().to_bytes()))
            .await?;
        let sequence = self.reserve_sequence(writer).await?;
        let authority = self.fresh_authority(Access::Edit).await?;
        let (statement, header) = self
            .repo
            .with_coordination_signer(&authority, |signer| {
                assert_eq!(
                    signer.verifying_key().to_bytes(),
                    writer,
                    "reserved local signer identity is stable"
                );
                OriginalStatement::sign(
                    key.clone(),
                    sequence,
                    frontier,
                    body,
                    signer.verifying_key(),
                    signer,
                )
                .map_err(|error| big_repo::CoordinationError::Other(ferr!(error)))
            })
            .await?;
        let original = serde_json::to_vec(&statement)?;
        // A refresh waits for this publication to commit; one ciphertext never
        // mixes key bytes with another pinned-key header.
        let publication_key = self.publication_key.read().await;
        let master = self.resolve_key(&publication_key).await?;
        let framing = EncodingParams::DEFAULT;
        let ciphertext = encrypt_with_rs(&master, &original, framing.record_size, framing.padding);
        let mut key_heads: Vec<_> = publication_key.heads.0.iter().map(|head| head.0).collect();
        key_heads.sort_unstable();
        key_heads.dedup();
        let binding = RepresentationBinding {
            incarnation: self.incarnation,
            key_ref: publication_key.key_ref.as_str().as_bytes().to_vec(),
            key_heads,
            encoding: CONTENT_ENCODING_AES128GCM.as_bytes().to_vec(),
            parameters: serde_json::to_vec(&framing)?,
        };
        let authority = self.fresh_authority(Access::Edit).await?;
        let representation = self
            .repo
            .with_coordination_signer(&authority, |signer| {
                Representation::sign(header, binding, ciphertext, signer.verifying_key(), signer)
                    .map_err(|error| big_repo::CoordinationError::Other(ferr!(error)))
            })
            .await?;
        self.merge_and_commit(Incoming::Local(representation)).await
    }

    /// Backend receive boundary. Hints are not grants: only this store's bound part is adopted.
    /// Rejected evidence never reaches publication's object/membership allocation.
    pub async fn receive(
        &self,
        obj_id: &ObjKey,
        part_hints: &[PartKey],
        payload: serde_json::Value,
    ) -> Res<MergeOutcome> {
        ensure!(
            part_hints.iter().all(|part| part == &self.part),
            "coordination part binding mismatch"
        );
        let incoming: RegisterSnapshot = serde_json::from_value(payload)?;
        ensure!(
            incoming.key.encode().as_slice() == obj_id.as_bytes(),
            "coordination object binding mismatch"
        );
        ensure!(
            incoming.key.scope == self.scope && incoming.key.slot.len() <= MAX_SLOT_BYTES,
            "coordination logical binding mismatch"
        );
        self.fresh_authority(Access::Relay).await?;
        self.merge_and_commit(Incoming::Remote(incoming)).await
    }

    async fn merge_and_commit(&self, incoming: Incoming) -> Res<MergeOutcome> {
        let key = match &incoming {
            Incoming::Local(representation) => &representation.original.key,
            Incoming::Remote(snapshot) => &snapshot.key,
        }
        .clone();
        match &incoming {
            Incoming::Local(value) => self.check_binding(value)?,
            Incoming::Remote(snapshot) => {
                for lane in snapshot.lanes.values() {
                    if let LaneState::Current { representation } = lane {
                        self.check_binding(representation)?;
                    }
                }
            }
        }
        let encoded = key.encode();
        let mut write = self
            .parts
            .begin_obj_write(ObjKey::new(encoded.clone()))
            .await?;
        let scope = write.scope_id();
        let current: Option<String> = sqlx::query_scalar(
            "SELECT snapshot_json FROM encrypted_register_current WHERE scope_id = ? AND logical_key = ?"
        ).bind(scope).bind(&encoded).fetch_optional(&mut **write.context_mut()).await?;
        let limits = self.limits(key);
        let mut state = match current {
            Some(current) => EncryptedRegister::restore(limits, serde_json::from_str(&current)?)?,
            None => EncryptedRegister::new(limits),
        };
        let outcome = match incoming {
            Incoming::Local(representation) => state.merge_local(representation)?,
            Incoming::Remote(snapshot) => state.merge_snapshot(snapshot)?,
        };
        if outcome != MergeOutcome::Unchanged {
            let payload = serde_json::to_value(state.snapshot())?;
            let json = serde_json::to_string(&payload)?;
            sqlx::query(
                "INSERT INTO encrypted_register_current(
                    scope_id
                  , logical_key
                  , snapshot_json
                ) VALUES (?, ?, ?)
                ON CONFLICT(scope_id, logical_key)
                DO UPDATE SET snapshot_json = excluded.snapshot_json",
            )
            .bind(scope)
            .bind(&encoded)
            .bind(json)
            .execute(&mut **write.context_mut())
            .await?;
            write
                .publish(payload, std::slice::from_ref(&self.part))
                .await?;
        }
        write.commit().await?;
        Ok(outcome)
    }

    /// Indexed opaque current lookup, permitted for configured encrypted relays.
    pub async fn current(&self, slot: &[u8]) -> Res<Option<RegisterSnapshot>> {
        self.fresh_authority(Access::Relay).await?;
        let key = self.key(slot)?;
        let mut write = self
            .parts
            .begin_obj_write(ObjKey::new(key.encode()))
            .await?;
        let scope = write.scope_id();
        let json: Option<String> = sqlx::query_scalar(
            "SELECT snapshot_json FROM encrypted_register_current WHERE scope_id = ? AND logical_key = ?"
        ).bind(scope).bind(key.encode()).fetch_optional(&mut **write.context_mut()).await?;
        write.commit().await?;
        json.map(|json| serde_json::from_str(&json).map_err(Into::into))
            .transpose()
    }

    /// Decrypt then authenticate the exact producer statement; plaintext is never persisted here.
    pub async fn open_original(&self, representation: &Representation) -> Res<OriginalStatement> {
        self.fresh_authority(Access::Read).await?;
        self.check_binding(representation)?;
        EncryptedRegister::new(self.limits(representation.original.key.clone()))
            .verify_representation(representation)?;
        let pinned = PinnedJwk {
            key_ref: Url::parse(std::str::from_utf8(&representation.binding.key_ref)?)?,
            heads: ChangeHashSet(
                representation
                    .binding
                    .key_heads
                    .iter()
                    .copied()
                    .map(automerge::ChangeHash)
                    .collect::<Vec<_>>()
                    .into(),
            ),
        };
        let master = self.resolve_key(&pinned).await?;
        let framing = EncodingParams::from_encoding_parameters(
            std::str::from_utf8(&representation.binding.encoding)?,
            &serde_json::from_slice(&representation.binding.parameters)?,
        )?;
        ensure!(
            representation.ciphertext.len() >= 21,
            "truncated RFC 8188 header"
        );
        let wire_rs = u32::from_be_bytes(
            representation.ciphertext[16..20]
                .try_into()
                .expect("four-byte record size"),
        );
        ensure!(
            u64::from(wire_rs) == framing.record_size,
            "authenticated framing differs from ciphertext header"
        );
        let bytes = decrypt_bytes(&master, &representation.ciphertext)?;
        let statement: OriginalStatement = serde_json::from_slice(&bytes)?;
        statement.verify_header(&representation.original)?;
        // Key materialization awaits may race group revocation. Admit Read again
        // at plaintext release, not only before resolving the pinned native key.
        self.fresh_authority(Access::Read).await?;
        Ok(statement)
    }

    pub async fn consumer_checkpoint(&self, consumer: &[u8]) -> Res<CursorIndex> {
        self.fresh_authority(Access::Read).await?;
        let mut write = self.parts.begin_obj_write(self.control_key()).await?;
        let scope = write.scope_id();
        let row: Option<(Vec<u8>, i64)> = sqlx::query_as(
            "SELECT selector
                  , through_revision
               FROM encrypted_register_consumer
              WHERE scope_id = ? AND consumer = ?",
        )
        .bind(scope)
        .bind(consumer)
        .fetch_optional(&mut **write.context_mut())
        .await?;
        write.commit().await?;
        match row {
            Some((selector, through)) => {
                ensure!(
                    selector == self.part.as_bytes(),
                    "coordination consumer selector changed; explicit successor checkpoint required"
                );
                Ok(u64::try_from(through)?)
            }
            None => Ok(0),
        }
    }

    /// Current part checkpoint + tail, not an append-only effect journal or boot corpus walk.
    pub async fn open_consumer_reader(
        &self,
        consumer: &[u8],
    ) -> Res<Box<dyn LocalPartRevisionReader>> {
        let after = self.consumer_checkpoint(consumer).await?;
        self.parts
            .open_revision_reader(SubPartsRequest {
                lower_bound: after,
                targets: [SubscriptionTarget::Part {
                    part_id: self.part.clone(),
                    cursor: after,
                }]
                .into(),
            })
            .await?
            .map_err(|error| ferr!("coordination revision reader: {error:?}"))
    }

    /// Apply derived SQL changes through context_mut; checkpoint and those changes commit together.
    pub async fn begin_consumer_settlement<'a>(
        &'a self,
        consumer: &[u8],
        through: CursorIndex,
    ) -> Res<ConsumerSettlement<'a>> {
        self.fresh_authority(Access::Read).await?;
        let mut write = self.parts.begin_obj_write(self.control_key()).await?;
        let scope = write.scope_id();
        let row: Option<(Vec<u8>, i64)> = sqlx::query_as(
            "SELECT selector
                  , through_revision
               FROM encrypted_register_consumer
              WHERE scope_id = ? AND consumer = ?",
        )
        .bind(scope)
        .bind(consumer)
        .fetch_optional(&mut **write.context_mut())
        .await?;
        let through = i64::try_from(through)?;
        if let Some((selector, previous)) = row {
            ensure!(
                selector == self.part.as_bytes(),
                "coordination consumer selector changed"
            );
            ensure!(
                through >= previous,
                "coordination consumer cursor regressed"
            );
        }
        // Read the fence through the owned writer transaction. Acquiring a
        // separate reader here can invert pool ownership under concurrent sync.
        let latest: i64 =
            sqlx::query_scalar("SELECT value FROM big_sync_meta WHERE key = 'global_cursor'")
                .fetch_one(&mut **write.context_mut())
                .await?;
        ensure!(
            through <= latest,
            "coordination consumer cursor exceeds committed revision"
        );
        sqlx::query(
            "INSERT INTO encrypted_register_consumer(
                scope_id
              , consumer
              , selector
              , through_revision
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(scope_id, consumer)
            DO UPDATE SET through_revision = excluded.through_revision",
        )
        .bind(scope)
        .bind(consumer)
        .bind(self.part.as_bytes())
        .bind(through)
        .execute(&mut **write.context_mut())
        .await?;
        Ok(ConsumerSettlement { write })
    }
}

/// Concrete SQLite settlement: drop rolls back the cursor and derived changes together.
pub struct ConsumerSettlement<'a> {
    write: SqliteObjWrite<'a>,
}

impl<'a> ConsumerSettlement<'a> {
    pub fn context_mut(&mut self) -> &mut Transaction<'a, Sqlite> {
        self.write.context_mut()
    }
    pub async fn commit(self) -> Res<()> {
        self.write.commit().await
    }
}

#[cfg(test)]
mod tests;
