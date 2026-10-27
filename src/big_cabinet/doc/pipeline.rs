//! The two-phase write pipeline and boot recovery shared by create, write, and branch.

use super::commands::Detail;
use super::commands::Kind;
use super::commands::Row;
use super::commands::State;
use super::*;

/// What a write intends, fixed before its first transaction so that the transaction it signs and
/// the transaction it commits are built from identical inputs.
pub(super) struct Plan<'a> {
    pub(super) document: &'a DocId,
    /// The `docUuid` bound to the document if it has none yet; see [`Dmeta::doc_uuid`].
    pub(super) doc_uuid: Uuid7,
    pub(super) actor: &'a Actor,
    pub(super) set: BTreeMap<DatumKey, (SchemaHandle, Json)>,
    pub(super) remove: BTreeSet<DatumKey>,
    pub(super) now: jiff::Timestamp,
    pub(super) predecessors: BTreeMap<DatumKey, Vec<Cid>>,
    /// Aliases minted for slots that had none when the plan was first staged.
    pub(super) aliases: BTreeMap<DatumKey, Uuid7>,
}

impl Plan<'_> {
    pub(super) fn keys(&self) -> impl Iterator<Item = &DatumKey> {
        self.set.keys().chain(self.remove.iter())
    }
}

/// The result of a write that failed. `Ambiguous` means the change may already be committed in
/// memory while its persistence failed: its command row must survive for the boot pass to settle.
pub(super) enum Failure {
    Refused(DocsError),
    Ambiguous(DocsError),
}

impl From<DocsError> for Failure {
    fn from(error: DocsError) -> Self {
        Self::Refused(error)
    }
}

impl Failure {
    pub(super) fn into_error(self) -> DocsError {
        match self {
            Self::Refused(error) | Self::Ambiguous(error) => error,
        }
    }
}

pub(crate) fn digest(value: &impl serde::Serialize) -> [u8; 32] {
    *blake3::hash(&serde_json::to_vec(value).expect("a request serializes")).as_bytes()
}

impl DocsService {
    /// The receipt of a completed command, `None` for an unknown one, and a refusal for a command
    /// still in flight or reused for a different request.
    pub(super) async fn replay(
        &self,
        command: &CommandId,
        digest: [u8; 32],
        signer: Uuid7,
    ) -> Result<Option<WriteReceipt>, DocsError> {
        let Some(row) = self.commands.get(command).await? else {
            return Ok(None);
        };
        if row.digest != digest || row.actor != signer {
            return Err(DocsError::CommandReused {
                command: command.clone(),
            });
        }
        match row.state {
            State::Done => Ok(Some(WriteReceipt {
                doc_id: row.document.expect("a done command names its document"),
                heads: row.heads,
            })),
            State::Pending | State::Committed => Err(DocsError::CommandInFlight {
                command: command.clone(),
            }),
        }
    }

    pub(super) fn schemas(
        &self,
        set: BTreeMap<DatumKey, Json>,
        remove: &BTreeSet<DatumKey>,
    ) -> Result<BTreeMap<DatumKey, (SchemaHandle, Json)>, DocsError> {
        for key in set.keys().chain(remove.iter()) {
            if is_system(key) {
                return Err(DocsError::SystemDatum {
                    datum_key: key.clone(),
                });
            }
            if set.contains_key(key) && remove.contains(key) {
                return Err(DocsError::SetAndRemoved {
                    datum_key: key.clone(),
                });
            }
        }
        set.into_iter()
            .map(|(key, value)| {
                let schema = self
                    .schema_repo
                    .require_for_write(key.tag().as_str())
                    .map_err(|_| DocsError::UnknownSchema {
                        datum_tag: key.tag().to_string(),
                    })?;
                Ok((key, (schema, value)))
            })
            .collect()
    }

    /// Apply a plan to a transaction and derive the claim each touched datum's signature covers.
    ///
    /// The value a claim covers is read back from the transaction after the content mutation, not
    /// taken from the caller's JSON, and the alias it names is the smallest one visible in the
    /// transaction. `signatures: None` stages with placeholder signatures so that every refusal —
    /// including dmeta's — surfaces before anything is signed; the caller then rolls back.
    pub(super) fn stage<'t>(
        &self,
        mut tx: Transaction<'t>,
        plan: &mut Plan<'_>,
        signatures: Option<&BTreeMap<DatumKey, SignatureBytes>>,
    ) -> Result<(Transaction<'t>, BTreeMap<DatumKey, DatumClaim>), DocsError> {
        let datums = match tx.get(automerge::ROOT, "datums").map_err(repository)? {
            Some((Value::Object(ObjType::Map), datums)) => datums,
            Some(_) => {
                return Err(DocsError::Repository("datums is not a map".into()));
            }
            None => tx
                .put_object(automerge::ROOT, "datums", ObjType::Map)
                .map_err(repository)?,
        };
        let values: Vec<(&DatumKey, &CompiledSchema, Json)> = plan
            .set
            .iter()
            .map(|(key, (schema, value))| (key, schema.compiled(), value.clone()))
            .collect();
        for (key, schema, value) in values {
            tx = reconcile_json_atomic(tx, schema, &datums, key.to_string().as_str().into(), value)
                .map_err(|error| DocsError::InvalidValue {
                    datum_key: key.clone(),
                    reason: error.to_string(),
                })?;
        }
        for key in &plan.remove {
            if tx
                .get(&datums, key.to_string())
                .map_err(repository)?
                .is_some()
            {
                tx.delete(&datums, key.to_string()).map_err(repository)?;
            }
        }
        let mut dmeta = match tx.get(&datums, DMETA_KEY).map_err(repository)? {
            None => Dmeta::new(plan.now),
            Some(_) => {
                let json = hydrate_value(&tx, &datums, DMETA_KEY.into(), self.dmeta.compiled())
                    .map_err(repository)?;
                serde_json::from_value::<Dmeta>(json).map_err(repository)?
            }
        };
        let doc_uuid = dmeta
            .doc_uuid(plan.document, plan.doc_uuid)
            .expect("a freshly minted document UUID is unbound");
        let mut claims = BTreeMap::new();
        let keys: Vec<DatumKey> = plan.keys().cloned().collect();
        for key in keys {
            let visible = dmeta
                .datum_uuids
                .iter()
                .filter(|(_, alias)| **alias == key)
                .map(|(uuid, _)| *uuid)
                .min();
            let chosen = match visible {
                Some(chosen) => chosen,
                None => {
                    let alias = *plan.aliases.entry(key.clone()).or_default();
                    dmeta
                        .record_datum_alias(alias, key.clone())
                        .expect("a freshly minted alias is unbound");
                    alias
                }
            };
            let native = NativeValue::read_at(&tx, &datums, key.to_string()).map_err(|error| {
                DocsError::InvalidValue {
                    datum_key: key.clone(),
                    reason: format!("{error:?}"),
                }
            })?;
            let removed = native.is_none();
            claims.insert(
                key.clone(),
                DatumClaim {
                    origin_document: plan.document.clone(),
                    datum_key: key.clone(),
                    chosen_uuid: chosen,
                    actor_uuid: plan.actor.actor_id,
                    operation: if removed {
                        ClaimOperation::Remove
                    } else {
                        ClaimOperation::Set
                    },
                    value_cid: native.map(|native| cid_of(&encode_native(&native))),
                    predecessors: plan.predecessors.get(&key).cloned().unwrap_or_default(),
                },
            );
            let signature = match signatures {
                Some(signatures) => signatures[&key].clone(),
                None => SignatureBytes::try_from([0_u8; 64].as_slice())
                    .expect("64 bytes are a signature"),
            };
            dmeta.datums.insert(
                key,
                DatumMetadata {
                    updated_at: vec![plan.now],
                    deleted_at: if removed { vec![plan.now] } else { Vec::new() },
                    signatures: vec![DatumSignature {
                        actor_uuid: plan.actor.actor_id,
                        signature,
                        doc_uuid,
                        message: None,
                        inherited: None,
                    }],
                },
            );
        }
        dmeta
            .actors
            .entry(plan.actor.actor_id)
            .or_insert_with(|| ActorMetadata {
                pubkey: plan.actor.public_key.clone(),
                actor_url: plan.actor.url.clone(),
                delegation: Some(plan.actor.certificate.clone()),
            });
        dmeta.updated_at = vec![plan.now];
        let tx = dmeta
            .reconcile_atomic(
                tx,
                self.dmeta.compiled(),
                &datums,
                DMETA_KEY.into(),
                &plan.keys().cloned().collect(),
            )
            .map_err(repository)?;
        Ok((tx, claims))
    }

    pub(super) async fn sign(
        &self,
        context: &RequestContext,
        capability: CapId,
        claims: &BTreeMap<DatumKey, DatumClaim>,
    ) -> Result<BTreeMap<DatumKey, SignatureBytes>, DocsError> {
        let mut signatures = BTreeMap::new();
        for (key, claim) in claims {
            let signed = self
                .authority
                .call(
                    context.child(),
                    SignDatumClaim {
                        actor: capability,
                        claim: claim.clone(),
                    },
                )
                .await
                .map_err(authority_error)?;
            assert_eq!(
                &signed.claim, claim,
                "the signer signs the claim it was given"
            );
            signatures.insert(key.clone(), signed.signature);
        }
        Ok(signatures)
    }

    /// The datum heads of every named datum at `heads`, verified: a datum that does not verify
    /// cannot match a condition taken from an earlier read.
    pub(super) fn datum_heads(
        doc: &Automerge,
        heads: &[ChangeHash],
        keys: impl Iterator<Item = DatumKey>,
    ) -> BTreeMap<DatumKey, Vec<ChangeHash>> {
        let gate = Gate::new(doc, heads);
        keys.map(|key| {
            let heads = match gate.verify(&key) {
                Verdict::Accepted { datum_heads, .. } | Verdict::Pending { datum_heads, .. } => {
                    datum_heads
                }
                Verdict::Rejected { .. } => Vec::new(),
            };
            (key, heads)
        })
        .collect()
    }

    /// Check a write's condition against the document as it is now and return the base to write
    /// from. Run once when the write is staged and again, under the same lock as the commit, when
    /// it is committed.
    pub(super) fn condition(
        doc: &Automerge,
        mode: &WriteMode,
    ) -> Result<Vec<ChangeHash>, DocsError> {
        let current = sorted(doc.get_heads());
        match mode {
            WriteMode::IfDocUnchanged { heads } => {
                check_heads(doc, heads)?;
                if sorted(heads.clone()) != current {
                    return Err(DocsError::Conflict {
                        heads: current,
                        datums: BTreeMap::new(),
                    });
                }
                Ok(current)
            }
            WriteMode::IfDatumsUnchanged { datums } => {
                let actual = Self::datum_heads(doc, &current, datums.keys().cloned());
                let expected: BTreeMap<_, _> = datums
                    .iter()
                    .map(|(key, version)| (key.clone(), sorted(version.heads.clone())))
                    .collect();
                if actual != expected {
                    return Err(DocsError::Conflict {
                        heads: current,
                        datums: actual
                            .into_iter()
                            .map(|(key, heads)| (key, datum_version(doc, heads)))
                            .collect(),
                    });
                }
                Ok(current)
            }
            WriteMode::AtHeads { heads } => {
                check_heads(doc, heads)?;
                Ok(sorted(heads.clone()))
            }
            WriteMode::AtCurrent => Ok(current),
        }
    }
}

impl DocsService {
    /// Settle what a crash left in the command ledger. See [`super::commands`].
    pub(super) async fn recover(&self) -> Result<(), DocsError> {
        let context = RequestContext::root();
        let reserved: BTreeSet<DocId> = self
            .big_repo
            .reserved_doc_ids()
            .await
            .map_err(repository)?
            .into_iter()
            .collect();
        for row in self.commands.unresolved().await? {
            match (row.kind, row.state, row.document.clone()) {
                // The id was never allocated, or its reservation holds no content: nothing exists
                // to finish. An allocated-but-unrecorded reservation is left alone, since BigRepo
                // reservations are shared with other allocators.
                (Kind::Create, State::Pending, None) => {
                    self.commands.discard(&row.command).await?;
                }
                (Kind::Create, State::Pending, Some(document)) => {
                    let Some(handle) = self
                        .reserved_content(&row.command, &document, &reserved)
                        .await?
                    else {
                        continue;
                    };
                    let heads = sorted(handle.with_document_read(|doc| doc.get_heads()).await);
                    let Some(Detail::Create { owner_id }) = row.detail else {
                        unreachable!("a create row records its placement authority");
                    };
                    let placement = row.placement.expect("a create row names its placement");
                    self.settle_create(
                        &context,
                        &row.command,
                        owner_id,
                        &document,
                        &placement,
                        &heads,
                    )
                    .await?;
                }
                (Kind::Create, State::Committed, Some(document)) => {
                    let Some(Detail::Create { owner_id }) = row.detail else {
                        unreachable!("a create row records its placement authority");
                    };
                    let placement = row.placement.expect("a create row names its placement");
                    self.access
                        .place(&context, owner_id, &document, &placement)
                        .await
                        .map_err(access_error)?;
                    self.commands
                        .advance(&row.command, State::Done, &row.heads)
                        .await?;
                }
                (Kind::Write, State::Pending, Some(document)) => {
                    let Some(handle) = self.target_handle(&row.command, &document).await? else {
                        continue;
                    };
                    let committed = handle
                        .with_document_read(|doc| committed_change(doc, &row.command))
                        .await;
                    match committed {
                        Some(hash) => {
                            self.commands
                                .advance(&row.command, State::Done, &[hash])
                                .await?;
                        }
                        None => self.commands.discard(&row.command).await?,
                    }
                }
                (Kind::Branch, State::Pending, None) => {
                    self.commands.discard(&row.command).await?;
                }
                (Kind::Branch, State::Pending, Some(branch)) => {
                    let Some(handle) = self
                        .reserved_content(&row.command, &branch, &reserved)
                        .await?
                    else {
                        continue;
                    };
                    let heads = sorted(handle.with_document_read(|doc| doc.get_heads()).await);
                    self.commands
                        .advance(&row.command, State::Committed, &heads)
                        .await?;
                    self.recover_branch(
                        &context,
                        &Row {
                            state: State::Committed,
                            heads,
                            ..row
                        },
                    )
                    .await?;
                }
                (Kind::Branch, State::Committed, Some(_)) => {
                    self.recover_branch(&context, &row).await?;
                }
                (Kind::Merge, State::Pending, Some(target)) => {
                    let Some(Detail::Merge { heads, .. }) = &row.detail else {
                        unreachable!("a merge row records its source heads")
                    };
                    let Some(handle) = self.target_handle(&row.command, &target).await? else {
                        continue;
                    };
                    let merged = handle
                        .with_document_read(|doc| {
                            heads
                                .iter()
                                .all(|head| doc.get_change_by_hash(head).is_some())
                                .then(|| sorted(doc.get_heads()))
                        })
                        .await;
                    match merged {
                        Some(heads) => {
                            self.commands
                                .advance(&row.command, State::Done, &heads)
                                .await?;
                        }
                        None => self.commands.discard(&row.command).await?,
                    }
                }
                (kind, state, document) => {
                    unreachable!("no command reaches {kind:?}/{state:?} with {document:?}")
                }
            }
        }
        self.commands.purge_done().await
    }

    /// The document a pending create or branch reserved, once it holds content: the reservation
    /// is committed if it is still open. `None` once the command is discarded because the
    /// document never received content, abandoning a reservation still left open.
    async fn reserved_content(
        &self,
        command: &CommandId,
        document: &DocId,
        reserved: &BTreeSet<DocId>,
    ) -> Result<Option<BigDocHandle>, DocsError> {
        let exists = self
            .big_repo
            .has_keyhive_document(document)
            .await
            .map_err(repository)?;
        if !exists {
            if reserved.contains(document) {
                self.big_repo
                    .abandon_allocation(document.clone())
                    .await
                    .map_err(repository)?;
            }
            self.commands.discard(command).await?;
            return Ok(None);
        }
        if reserved.contains(document) {
            return self
                .big_repo
                .commit_reserved(document.clone(), &[])
                .await
                .map(Some)
                .map_err(repository);
        }
        self.handle(document).await.map(Some)
    }

    /// The existing document a pending write or merge targets. `None` once the command is
    /// discarded because the document is gone, or while the document is still materializing:
    /// whether the change landed is not yet knowable then, so the row stays in flight for the
    /// next boot pass.
    async fn target_handle(
        &self,
        command: &CommandId,
        document: &DocId,
    ) -> Result<Option<BigDocHandle>, DocsError> {
        match self.handle(document).await {
            Ok(handle) => Ok(Some(handle)),
            Err(DocsError::DocNotFound) => {
                self.commands.discard(command).await?;
                Ok(None)
            }
            Err(DocsError::Pending { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Settle a committed branch from its row. A backlink the origin now refuses (its branch list
    /// stopped verifying after the branch was taken) leaves the row in flight rather than failing
    /// boot: the branch exists and cannot be rolled back, and the refusal is the origin's to repair.
    pub(super) async fn recover_branch(
        &self,
        context: &RequestContext,
        row: &Row,
    ) -> Result<(), DocsError> {
        match self.settle_branch(context, row).await {
            Err(
                DocsError::BaseNotAccepted { .. }
                | DocsError::DocNotFound
                | DocsError::Pending { .. },
            ) => Ok(()),
            result => result,
        }
    }
}
