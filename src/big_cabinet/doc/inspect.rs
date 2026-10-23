//! The unsafe inspection pair: raw claim evidence for diagnostics and tests.

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EvidenceScope {
    UnsafeUnattributedSignatureEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerifiedSignatureEvidence {
    pub signature: FacetSignature,
    pub actor_public_key: MultibaseKey,
    pub claim: FacetClaim,
    pub introduction_marker: String,
    pub introduction_hash: ChangeHash,
    pub parents: Vec<ChangeHash>,
    pub resulting_heads: Vec<ChangeHash>,
    pub automerge_actor: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum SignatureEvidence {
    VerifiedSignature {
        evidence: Box<VerifiedSignatureEvidence>,
    },
    Pending {
        reason: String,
    },
    ResourceRefused {
        reason: String,
    },
    Unsupported {
        reason: String,
    },
    ProvenForgery {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FacetClaimEvidence {
    pub marker: String,
    pub result: SignatureEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FacetClaimsInspection {
    pub doc_id: DocId,
    pub facet_key: FacetKey,
    pub heads: Vec<ChangeHash>,
    pub scope: EvidenceScope,
    pub evidence: Vec<FacetClaimEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsafeFacetInspection {
    pub native_value: Option<Bytes>,
    pub schema_known: bool,
    pub claims: FacetClaimsInspection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsafeInspectFacet {
    pub capability: CapId,
    pub doc_id: DocId,
    pub facet_key: FacetKey,
    pub heads: Vec<ChangeHash>,
}

impl Operation for UnsafeInspectFacet {
    type Output = UnsafeFacetInspection;
    type Error = DocsError;
    const XRPC: XrpcOperation =
        XrpcOperation::procedure("org.example.daybook.docs.unsafeInspectFacet")
            .with_schemas("UnsafeFacetInspection", "DocsError");
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectFacetClaims {
    pub capability: CapId,
    pub doc_id: DocId,
    pub facet_key: FacetKey,
    pub heads: Vec<ChangeHash>,
}

impl Operation for InspectFacetClaims {
    type Output = FacetClaimsInspection;
    type Error = DocsError;
    const XRPC: XrpcOperation =
        XrpcOperation::procedure("org.example.daybook.docs.inspectFacetClaims")
            .with_schemas("FacetClaimsInspection", "DocsError");
}

fn history_reason(error: ClaimHistoryError) -> String {
    match error {
        ClaimHistoryError::ResourceRefused(reason) => format!("resource limit: {reason}"),
        ClaimHistoryError::Unsupported(reason) => format!("unsupported: {reason}"),
        ClaimHistoryError::MalformedWitness(reason) => format!("malformed witness: {reason}"),
        ClaimHistoryError::InvalidSignature => "invalid signature".into(),
        ClaimHistoryError::Automerge(reason) => format!("Automerge: {reason}"),
    }
}

impl From<ClaimHistoryError> for DocsError {
    fn from(error: ClaimHistoryError) -> Self {
        match error {
            ClaimHistoryError::ResourceRefused(reason) => Self::ResourceRefused {
                reason: reason.into(),
            },
            ClaimHistoryError::Unsupported(reason) => Self::Unsupported {
                reason: reason.into(),
            },
            ClaimHistoryError::Automerge(reason) => Self::Repository(reason),
            error @ (ClaimHistoryError::MalformedWitness(_)
            | ClaimHistoryError::InvalidSignature) => Self::MalformedEvidence {
                reason: history_reason(error),
            },
        }
    }
}

impl From<ClaimHistoryEntry> for FacetClaimEvidence {
    fn from(entry: ClaimHistoryEntry) -> Self {
        let result = match entry.result {
            ClaimHistoryResult::Accepted(claim) => SignatureEvidence::VerifiedSignature {
                evidence: Box::new(VerifiedSignatureEvidence {
                    signature: claim.signature,
                    actor_public_key: claim.actor_public_key,
                    claim: claim.claim,
                    introduction_marker: claim.introduction.marker.to_string(),
                    introduction_hash: claim.introduction.introduction_hash,
                    parents: claim.introduction.parents,
                    resulting_heads: claim.introduction.resulting_heads,
                    automerge_actor: claim
                        .introduction
                        .automerge_actor
                        .to_bytes()
                        .to_vec()
                        .into(),
                }),
            },
            ClaimHistoryResult::Pending(ClaimHistoryError::ResourceRefused(reason)) => {
                SignatureEvidence::ResourceRefused {
                    reason: reason.into(),
                }
            }
            ClaimHistoryResult::Pending(ClaimHistoryError::Unsupported(reason)) => {
                SignatureEvidence::Unsupported {
                    reason: reason.into(),
                }
            }
            ClaimHistoryResult::Pending(error) => SignatureEvidence::Pending {
                reason: history_reason(error),
            },
            ClaimHistoryResult::ProvenForgery(error) => SignatureEvidence::ProvenForgery {
                reason: history_reason(error),
            },
        };
        Self {
            marker: entry.marker.to_string(),
            result,
        }
    }
}

impl DocsService {
    async fn inspected(
        &self,
        context: &RequestContext,
        capability: CapId,
        doc_id: &DocId,
        heads: &[ChangeHash],
    ) -> Result<BigDocHandle, DocsError> {
        let held = self
            .authority
            .call(context.child(), InspectCapability { capability })
            .await
            .map_err(authority_error)?;
        if held.issuer != self.system_actor.uuid
            || held.object != inspection_object(doc_id)
            || !held.rights.contains(Rights::USE)
        {
            return Err(DocsError::Denied);
        }
        if heads.len() > HistoryBudget::default().max_entries_per_frontier {
            return Err(DocsError::ResourceRefused {
                reason: "heads frontier".into(),
            });
        }
        context.validate()?;
        self.handle(doc_id).await
    }

    fn claims(
        doc: &Automerge,
        doc_id: DocId,
        facet_key: FacetKey,
        heads: Vec<ChangeHash>,
    ) -> Result<FacetClaimsInspection, DocsError> {
        let evidence =
            reconstruct_current_claims(doc, &facet_key, &heads, HistoryBudget::default())?
                .into_iter()
                .map(FacetClaimEvidence::from)
                .collect();
        Ok(FacetClaimsInspection {
            doc_id,
            facet_key,
            heads,
            scope: EvidenceScope::UnsafeUnattributedSignatureEvidence,
            evidence,
        })
    }
}

impl Handler<UnsafeInspectFacet> for DocsService {
    async fn call(
        &self,
        request: Envelope<UnsafeInspectFacet>,
    ) -> Result<ResponseEnvelope<UnsafeFacetInspection>, DocsError> {
        let params = request.params;
        let document = self
            .inspected(
                &request.context,
                params.capability,
                &params.doc_id,
                &params.heads,
            )
            .await?;
        let schema_known = self
            .schema_repo
            .resolve_for_read(params.facet_key.tag().as_str())
            .is_some();
        let output = document
            .with_document_read(|doc| {
                request.context.validate()?;
                check_heads(doc, &params.heads)?;
                let native_value = match doc
                    .get_at(automerge::ROOT, "facets", &params.heads)
                    .map_err(ClaimHistoryError::from)?
                {
                    None => None,
                    Some((Value::Object(ObjType::Map), facets)) => NativeValue::read_at_heads(
                        doc,
                        &facets,
                        params.facet_key.to_string(),
                        &params.heads,
                        &mut NativeReadBudget::default(),
                    )
                    .map_err(ClaimHistoryError::from)?
                    .map(|value| encode_native(&value).into()),
                    Some(_) => {
                        return Err(DocsError::MalformedEvidence {
                            reason: "facets is not a map".into(),
                        });
                    }
                };
                let claims = Self::claims(doc, params.doc_id, params.facet_key, params.heads)?;
                Ok::<_, DocsError>(UnsafeFacetInspection {
                    native_value,
                    schema_known,
                    claims,
                })
            })
            .await?;
        Ok(output.into())
    }
}

impl Handler<InspectFacetClaims> for DocsService {
    async fn call(
        &self,
        request: Envelope<InspectFacetClaims>,
    ) -> Result<ResponseEnvelope<FacetClaimsInspection>, DocsError> {
        let params = request.params;
        let document = self
            .inspected(
                &request.context,
                params.capability,
                &params.doc_id,
                &params.heads,
            )
            .await?;
        let output = document
            .with_document_read(|doc| {
                request.context.validate()?;
                check_heads(doc, &params.heads)?;
                Self::claims(doc, params.doc_id, params.facet_key, params.heads)
            })
            .await?;
        Ok(output.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::{
        Actor, AttenuateCapability, Authority, CapId, Capability, MintCapability, OpenActor,
        RetractCapability, Rights,
    };
    use crate::schema::SchemaRepo;
    use automerge::transaction::Transactable;
    use automerge::{Automerge, ChangeHash, ObjId, ObjType, ReadDoc};
    use big_cabinet_core::FacetKey;
    use big_cabinet_core::dmeta::{
        Bytes, ClaimOperation, DocId, FacetClaim, FacetSignature, MultibaseKey, NativeValue,
        SignatureBytes, SignedFacetClaim, Uuid7, cid_of, encode_native,
    };
    use big_repo::{BigDocHandle, BigRepo, BigRepoStopToken, Config, SharedBigRepo, StorageConfig};
    use ed25519_dalek::{Signer, SigningKey};
    use secrets_rs::SecretStore;
    use service_utils_rs::{
        Admission, Client, ClientError, ContextError, IrpcAdapter, RequestContext, Route,
        StopToken, XrpcClient, XrpcRouter, xrpc,
    };
    use sqlx_utils_rs::SqlCtx;
    use std::sync::Arc;

    const FACET: &str = "org.example.note/main";
    const DMETA: &str = "org.example.daybook.dmeta/main";

    struct Fixture {
        authority: Authority,
        authority_stop: StopToken,
        issuer: Actor,
        repo: SharedBigRepo,
        repo_stop: Option<BigRepoStopToken>,
        docs: DocsRepo,
        docs_stop: Option<StopToken>,
        document: BigDocHandle,
        capability: Capability,
        expected: UnsafeFacetInspection,
    }

    impl Fixture {
        async fn new() -> Self {
            let keyring =
                keyring_core::mock::Store::new().unwrap() as Arc<keyring_core::CredentialStore>;
            let secrets = SecretStore::boot_with_store(keyring).await.unwrap();
            let (authority, authority_stop) =
                crate::authority::boot(&secrets, SqlCtx::memory().await.unwrap())
                    .await
                    .unwrap();
            let issuer = authority
                .client
                .call(
                    RequestContext::root(),
                    OpenActor {
                        parent: authority.root,
                        path: "documents".into(),
                        rights: Rights::all(),
                    },
                )
                .await
                .unwrap();
            let (repo, repo_stop) = BigRepo::boot(Config {
                node_identity_seed: [31; 32],
                storage: StorageConfig::Memory,
                scope_key: "document-service-tests".into(),
                hidden_parts: Default::default(),
                automerge_frontier_group_scope: Default::default(),
                causal_checkpoint_group_scope: Default::default(),
                group_part_group_scope: Default::default(),
            })
            .await
            .unwrap();
            let schemas = Arc::new(SchemaRepo::open([]).unwrap());
            let (docs, docs_stop) =
                boot(config(&authority, &repo, schemas, issuer.capability).await)
                    .await
                    .unwrap();
            let key = SigningKey::from_bytes(&[19; 32]);
            let alias = Uuid7::new();
            let actor = Uuid7::new();
            let doc_uuid = Uuid7::new();
            let public_key =
                MultibaseKey::try_from(key.verifying_key().as_bytes().as_slice()).unwrap();
            let mut initial = Automerge::new();
            let (facets, signatures, bindings) = {
                let mut tx = initial.transaction();
                let facets = tx
                    .put_object(automerge::ROOT, "facets", ObjType::Map)
                    .unwrap();
                let dmeta = tx.put_object(&facets, DMETA, ObjType::Map).unwrap();
                let aliases = tx.put_object(&dmeta, "facetUuids", ObjType::Map).unwrap();
                tx.put(&aliases, alias.to_string(), FACET).unwrap();
                let bindings = tx.put_object(&dmeta, "branchUuids", ObjType::Map).unwrap();
                let actors = tx.put_object(&dmeta, "actors", ObjType::Map).unwrap();
                let actor_meta = tx
                    .put_object(&actors, actor.to_string(), ObjType::Map)
                    .unwrap();
                tx.put(&actor_meta, "pubkey", public_key.to_string())
                    .unwrap();
                let metadata = tx.put_object(&dmeta, "facets", ObjType::Map).unwrap();
                let facet_meta = tx.put_object(&metadata, FACET, ObjType::Map).unwrap();
                let signatures = tx
                    .put_object(&facet_meta, "signatures", ObjType::List)
                    .unwrap();
                tx.commit();
                (facets, signatures, bindings)
            };
            let document = repo.create_doc(initial).await.unwrap();
            let doc_id = document.document_id();
            let facet_key: FacetKey = FACET.parse().unwrap();
            let native = NativeValue::Bytes(vec![0, 255, 128]);
            let signed = SignedFacetClaim {
                actor_public_key: public_key.clone(),
                claim: FacetClaim {
                    origin_document: doc_id.clone(),
                    facet_key: facet_key.clone(),
                    chosen_uuid: alias,
                    actor_uuid: actor,
                    operation: ClaimOperation::Set,
                    value_cid: Some(cid_of(&encode_native(&native))),
                    predecessors: Vec::new(),
                },
                signature: SignatureBytes::try_from([0_u8; 64].as_slice()).unwrap(),
            };
            let signature = SignatureBytes::try_from(
                key.sign(&signed.claim.signing_bytes().unwrap())
                    .to_bytes()
                    .as_slice(),
            )
            .unwrap();
            let signed = SignedFacetClaim {
                signature,
                ..signed
            };
            signed.verify().unwrap();
            let (marker, introduction_hash, parents, automerge_actor) = document
                .with_document(|doc| {
                    let parents = doc.get_heads();
                    let automerge_actor = Bytes::from(doc.get_actor().to_bytes().to_vec());
                    let mut tx = doc.transaction();
                    tx.put(&facets, FACET, vec![0_u8, 255, 128]).unwrap();
                    let marker = tx.insert_object(&signatures, 0, ObjType::Map).unwrap();
                    tx.put(&marker, "actorUuid", actor.to_string()).unwrap();
                    tx.put(&marker, "docUuid", doc_uuid.to_string()).unwrap();
                    tx.put(&marker, "signature", signed.signature.as_bytes().to_vec())
                        .unwrap();
                    tx.put(&bindings, doc_uuid.to_string(), doc_id.to_string())
                        .unwrap();
                    let hash = tx.commit().0.unwrap();
                    (marker, hash, parents, automerge_actor)
                })
                .await
                .unwrap();
            let capability = docs.mint_inspection_handle(doc_id.clone()).await.unwrap();
            let expected = UnsafeFacetInspection {
                native_value: Some(encode_native(&native).into()),
                schema_known: false,
                claims: FacetClaimsInspection {
                    doc_id,
                    facet_key,
                    heads: vec![introduction_hash],
                    scope: EvidenceScope::UnsafeUnattributedSignatureEvidence,
                    evidence: vec![FacetClaimEvidence {
                        marker: marker.to_string(),
                        result: SignatureEvidence::VerifiedSignature {
                            evidence: Box::new(VerifiedSignatureEvidence {
                                signature: FacetSignature {
                                    actor_uuid: actor,
                                    doc_uuid,
                                    signature: signed.signature,
                                    message: None,
                                    inherited: None,
                                },
                                actor_public_key: public_key,
                                claim: signed.claim,
                                introduction_marker: marker.to_string(),
                                introduction_hash,
                                parents,
                                resulting_heads: vec![introduction_hash],
                                automerge_actor,
                            }),
                        },
                    }],
                },
            };
            Self {
                authority,
                authority_stop,
                issuer,
                repo,
                repo_stop: Some(repo_stop),
                docs,
                docs_stop: Some(docs_stop),
                document,
                capability,
                expected,
            }
        }

        fn request(&self) -> UnsafeInspectFacet {
            UnsafeInspectFacet {
                capability: self.capability.id,
                doc_id: self.document.document_id(),
                facet_key: FACET.parse().unwrap(),
                heads: self.expected.claims.heads.clone(),
            }
        }

        async fn mint(&self, issuer: CapId, object: String, rights: Rights) -> Capability {
            self.authority
                .client
                .call(
                    RequestContext::root(),
                    MintCapability {
                        issuer,
                        object,
                        rights,
                    },
                )
                .await
                .unwrap()
        }

        async fn retract(&self, capability: CapId) {
            self.authority
                .client
                .call(
                    RequestContext::root(),
                    RetractCapability {
                        via: self.authority.root,
                        capability,
                    },
                )
                .await
                .unwrap();
        }

        async fn unsigned_value(&self, value: automerge::ScalarValue) -> Vec<ChangeHash> {
            self.document
                .with_document(|doc| {
                    let (_, facets) = doc.get(automerge::ROOT, "facets").unwrap().unwrap();
                    let mut tx = doc.transaction();
                    tx.put(&facets, FACET, value).unwrap();
                    tx.commit();
                    doc.get_heads()
                })
                .await
                .unwrap()
        }

        async fn replace_signature(&self, bytes: Option<Vec<u8>>) -> (ObjId, Vec<ChangeHash>) {
            self.document
                .with_document(|doc| {
                    let (_, facets) = doc.get(automerge::ROOT, "facets").unwrap().unwrap();
                    let (_, dmeta) = doc.get(&facets, DMETA).unwrap().unwrap();
                    let (_, metadata) = doc.get(&dmeta, "facets").unwrap().unwrap();
                    let (_, metadata) = doc.get(&metadata, FACET).unwrap().unwrap();
                    let (_, signatures) = doc.get(&metadata, "signatures").unwrap().unwrap();
                    let (actor_uuid, doc_uuid) = match &self.expected.claims.evidence[0].result {
                        SignatureEvidence::VerifiedSignature { evidence } => {
                            (evidence.claim.actor_uuid, evidence.signature.doc_uuid)
                        }
                        _ => unreachable!(),
                    };
                    let mut tx = doc.transaction();
                    tx.delete(&signatures, 0).unwrap();
                    let marker = tx.insert_object(&signatures, 0, ObjType::Map).unwrap();
                    tx.put(&marker, "actorUuid", actor_uuid.to_string())
                        .unwrap();
                    tx.put(&marker, "docUuid", doc_uuid.to_string()).unwrap();
                    if let Some(bytes) = bytes {
                        tx.put(&marker, "signature", bytes).unwrap();
                    }
                    tx.commit();
                    (marker, doc.get_heads())
                })
                .await
                .unwrap()
        }

        async fn stop(mut self) {
            if let Some(stop) = self.docs_stop.take() {
                stop.stop().await;
            }
            drop(self.document);
            if let Some(stop) = self.repo_stop.take() {
                stop.stop().await.unwrap();
            }
            self.authority_stop.stop().await;
        }
    }

    async fn config(
        authority: &Authority,
        repo: &SharedBigRepo,
        schemas: Arc<SchemaRepo>,
        issuer: CapId,
    ) -> DocsConfig {
        DocsConfig {
            big_repo: Arc::clone(repo),
            schema_repo: schemas,
            authority: authority.client.clone(),
            issuer,
            node_issuer: authority.node_issuer.clone(),
            access: Arc::new(LedgerAccess::new(authority.client.clone(), issuer)),
            sql: SqlCtx::memory().await.unwrap(),
            facet_set: None,
            facet_ref: None,
        }
    }

    async fn inspect(
        client: &Client<DocsProtocol>,
        request: UnsafeInspectFacet,
    ) -> Result<UnsafeFacetInspection, DocsError> {
        client
            .call(RequestContext::root(), request)
            .await
            .map_err(|error| match error {
                ClientError::Operation(error) => error,
                ClientError::Transport { message, .. } => panic!("inspection transport: {message}"),
            })
    }

    async fn claims(
        client: &Client<DocsProtocol>,
        request: UnsafeInspectFacet,
    ) -> Result<FacetClaimsInspection, DocsError> {
        client
            .call(
                RequestContext::root(),
                InspectFacetClaims {
                    capability: request.capability,
                    doc_id: request.doc_id,
                    facet_key: request.facet_key,
                    heads: request.heads,
                },
            )
            .await
            .map_err(|error| match error {
                ClientError::Operation(error) => error,
                ClientError::Transport { message, .. } => panic!("claims transport: {message}"),
            })
    }

    #[tokio::test]
    async fn explicit_heads_preserve_native_bytes_and_do_not_authenticate_unsigned_edits() {
        let fixture = Fixture::new().await;
        let newer_heads = fixture
            .unsigned_value(automerge::ScalarValue::Bytes(vec![1, 2, 3]))
            .await;
        let client = &fixture.docs.client;
        assert_eq!(
            inspect(client, fixture.request()).await.unwrap(),
            fixture.expected
        );
        assert_eq!(
            claims(client, fixture.request()).await.unwrap(),
            fixture.expected.claims
        );
        let mut newer = fixture.request();
        newer.heads = newer_heads.clone();
        let mut expected = fixture.expected.clone();
        expected.native_value = Some(encode_native(&NativeValue::Bytes(vec![1, 2, 3])).into());
        expected.claims.heads = newer_heads;
        assert_eq!(inspect(client, newer).await.unwrap(), expected);
        let mut empty = fixture.request();
        empty.heads.clear();
        assert_eq!(
            inspect(client, empty).await.unwrap(),
            UnsafeFacetInspection {
                native_value: None,
                schema_known: false,
                claims: FacetClaimsInspection {
                    doc_id: fixture.document.document_id(),
                    facet_key: FACET.parse().unwrap(),
                    heads: Vec::new(),
                    scope: EvidenceScope::UnsafeUnattributedSignatureEvidence,
                    evidence: Vec::new(),
                },
            }
        );
        let wire = serde_json::to_value(&fixture.expected).unwrap();
        assert_eq!(
            wire["claims"]["scope"],
            "unsafeUnattributedSignatureEvidence"
        );
        assert_eq!(
            wire["claims"]["evidence"][0]["result"]["status"],
            "verifiedSignature"
        );
        assert!(wire["nativeValue"]["$bytes"].is_string());
        fixture.stop().await;
    }

    #[tokio::test]
    async fn unknown_heads_resource_refusal_and_unsupported_values_are_not_rejections() {
        let fixture = Fixture::new().await;
        let client = &fixture.docs.client;
        let unknown = ChangeHash([91; 32]);
        let mut request = fixture.request();
        request.heads.push(unknown);
        let expected = DocsError::UnknownHeads {
            heads: vec![unknown],
        };
        assert_eq!(
            inspect(client, request.clone()).await.unwrap_err(),
            expected
        );
        assert_eq!(claims(client, request).await.unwrap_err(), expected);
        let mut request = fixture.request();
        request.heads =
            vec![request.heads[0]; HistoryBudget::default().max_entries_per_frontier + 1];
        let expected = DocsError::ResourceRefused {
            reason: "heads frontier".into(),
        };
        assert_eq!(
            inspect(client, request.clone()).await.unwrap_err(),
            expected
        );
        assert_eq!(claims(client, request).await.unwrap_err(), expected);
        let heads = fixture
            .unsigned_value(automerge::ScalarValue::Unknown {
                type_code: 15,
                bytes: vec![3],
            })
            .await;
        let mut request = fixture.request();
        request.heads = heads;
        assert_eq!(
            inspect(client, request.clone()).await.unwrap_err(),
            DocsError::Unsupported {
                reason: "unknown scalar kind".into()
            }
        );
        let mut expected = fixture.expected.claims.clone();
        expected.heads = request.heads.clone();
        assert_eq!(claims(client, request).await.unwrap(), expected);
        fixture.stop().await;
    }

    #[tokio::test]
    async fn missing_signature_bytes_is_forgery_and_does_not_poison_descendants() {
        let fixture = Fixture::new().await;
        let (marker, heads) = fixture.replace_signature(None).await;
        let mut request = fixture.request();
        request.heads = heads.clone();
        let expected = FacetClaimsInspection {
            doc_id: request.doc_id.clone(),
            facet_key: request.facet_key.clone(),
            heads,
            scope: EvidenceScope::UnsafeUnattributedSignatureEvidence,
            evidence: vec![FacetClaimEvidence {
                marker: marker.to_string(),
                result: SignatureEvidence::ProvenForgery {
                    reason: "malformed witness: signature bytes".into(),
                },
            }],
        };
        assert_eq!(
            claims(&fixture.docs.client, request.clone()).await.unwrap(),
            expected
        );
        assert_eq!(
            inspect(&fixture.docs.client, request).await.unwrap().claims,
            expected
        );
        let (marker, heads) = fixture.replace_signature(Some(vec![0; 64])).await;
        let mut request = fixture.request();
        request.heads = heads.clone();
        let expected = FacetClaimsInspection {
            doc_id: request.doc_id.clone(),
            facet_key: request.facet_key.clone(),
            heads,
            scope: EvidenceScope::UnsafeUnattributedSignatureEvidence,
            evidence: vec![FacetClaimEvidence {
                marker: marker.to_string(),
                result: SignatureEvidence::ProvenForgery {
                    reason: "invalid signature".into(),
                },
            }],
        };
        assert_eq!(
            claims(&fixture.docs.client, request.clone()).await.unwrap(),
            expected
        );
        assert_eq!(
            inspect(&fixture.docs.client, request).await.unwrap().claims,
            expected
        );
        fixture.stop().await;
    }

    #[tokio::test]
    async fn invalid_signature_with_available_context_is_proven_forgery() {
        let fixture = Fixture::new().await;
        let (marker, heads) = fixture.replace_signature(Some(vec![0; 64])).await;
        let mut request = fixture.request();
        request.heads = heads.clone();
        let expected = FacetClaimsInspection {
            doc_id: request.doc_id.clone(),
            facet_key: request.facet_key.clone(),
            heads,
            scope: EvidenceScope::UnsafeUnattributedSignatureEvidence,
            evidence: vec![FacetClaimEvidence {
                marker: marker.to_string(),
                result: SignatureEvidence::ProvenForgery {
                    reason: "invalid signature".into(),
                },
            }],
        };
        assert_eq!(
            claims(&fixture.docs.client, request.clone()).await.unwrap(),
            expected
        );
        assert_eq!(
            inspect(&fixture.docs.client, request).await.unwrap().claims,
            expected
        );
        fixture.stop().await;
    }

    #[tokio::test]
    async fn capability_checks_precede_any_repository_lookup() {
        let mut fixture = Fixture::new().await;
        let wrong_object = fixture
            .docs
            .mint_inspection_handle(DocId::new([82; 32]))
            .await
            .unwrap();
        let wrong_issuer = fixture
            .mint(
                fixture.authority.root,
                fixture.capability.object.clone(),
                Rights::USE,
            )
            .await;
        let no_use = fixture
            .mint(
                fixture.issuer.capability,
                fixture.capability.object.clone(),
                Rights::empty(),
            )
            .await;
        let retracted = fixture
            .docs
            .mint_inspection_handle(fixture.document.document_id())
            .await
            .unwrap();
        fixture.retract(retracted.id).await;
        let ancestor = fixture
            .authority
            .client
            .call(
                RequestContext::root(),
                AttenuateCapability {
                    capability: fixture.issuer.capability,
                    rights: Rights::USE,
                },
            )
            .await
            .unwrap();
        fixture.repo_stop.take().unwrap().stop().await.unwrap();
        let denied = [
            wrong_object.id,
            wrong_issuer.id,
            no_use.id,
            retracted.id,
            fixture.issuer.capability,
            ancestor.id,
            CapId::default(),
        ];
        for capability in denied {
            let mut request = fixture.request();
            request.capability = capability;
            assert_eq!(
                inspect(&fixture.docs.client, request.clone())
                    .await
                    .unwrap_err(),
                DocsError::Denied
            );
            assert_eq!(
                claims(&fixture.docs.client, request).await.unwrap_err(),
                DocsError::Denied
            );
        }
        assert!(matches!(
            inspect(&fixture.docs.client, fixture.request()).await,
            Err(DocsError::Repository(_))
        ));
        assert!(matches!(
            claims(&fixture.docs.client, fixture.request()).await,
            Err(DocsError::Repository(_))
        ));
        fixture.stop().await;
    }

    #[tokio::test]
    async fn boot_requires_exact_actor_association_and_shutdown_closes_inspection_and_minting() {
        let mut fixture = Fixture::new().await;
        let derived = fixture
            .authority
            .client
            .call(
                RequestContext::root(),
                AttenuateCapability {
                    capability: fixture.issuer.capability,
                    rights: Rights::all(),
                },
            )
            .await
            .unwrap();
        for capability in [fixture.capability.id, derived.id] {
            let refused = boot(
                config(
                    &fixture.authority,
                    &fixture.repo,
                    Arc::new(SchemaRepo::open([]).unwrap()),
                    capability,
                )
                .await,
            )
            .await;
            assert!(matches!(refused, Err(DocsError::Denied)));
        }
        fixture.docs_stop.take().unwrap().stop().await;
        let expected = DocsError::Context(ContextError::Cancelled);
        assert_eq!(
            inspect(&fixture.docs.client, fixture.request())
                .await
                .unwrap_err(),
            expected
        );
        assert_eq!(
            claims(&fixture.docs.client, fixture.request())
                .await
                .unwrap_err(),
            expected
        );
        assert_eq!(
            fixture
                .docs
                .mint_inspection_handle(fixture.document.document_id())
                .await
                .unwrap_err(),
            expected
        );
        fixture.stop().await;
    }

    #[tokio::test]
    async fn direct_irpc_and_xrpc_share_exact_heads_evidence_and_denials() {
        const BODY_LIMIT: usize = 1024 * 1024;
        let fixture = Fixture::new().await;
        fixture
            .unsigned_value(automerge::ScalarValue::Bytes(vec![9]))
            .await;
        let (irpc, irpc_stop) = IrpcAdapter::local::<_, DocsProtocol>(
            Arc::clone(&fixture.docs.host),
            Admission::new(4, 1),
            8,
        );
        let mut router = XrpcRouter::new(BODY_LIMIT);
        DocsProtocol::mount(&mut router, Arc::clone(&fixture.docs.host)).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (http_task, http_shutdown) = xrpc::serve(listener, router.into_router()).await.unwrap();
        let base = url::Url::parse(&format!("http://{address}/")).unwrap();
        let transports = [
            fixture.docs.client.clone(),
            Client::irpc(irpc),
            Client::xrpc(XrpcClient::new(base, BODY_LIMIT).unwrap()),
        ];
        for client in &transports {
            assert_eq!(
                inspect(client, fixture.request()).await.unwrap(),
                fixture.expected
            );
            assert_eq!(
                claims(client, fixture.request()).await.unwrap(),
                fixture.expected.claims
            );
            let mut unknown = fixture.request();
            unknown.heads = vec![ChangeHash([92; 32])];
            let expected = DocsError::UnknownHeads {
                heads: unknown.heads.clone(),
            };
            assert_eq!(
                inspect(client, unknown.clone()).await.unwrap_err(),
                expected
            );
            assert_eq!(claims(client, unknown).await.unwrap_err(), expected);
            let mut denied = fixture.request();
            denied.capability = fixture.issuer.capability;
            assert_eq!(
                inspect(client, denied.clone()).await.unwrap_err(),
                DocsError::Denied
            );
            assert_eq!(claims(client, denied).await.unwrap_err(), DocsError::Denied);
        }
        http_shutdown.send(()).unwrap();
        http_task.await.unwrap().unwrap();
        irpc_stop.stop().await;
        fixture.stop().await;
    }
}
