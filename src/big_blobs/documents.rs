use crate::encrypt::CONTENT_ENCODING_AES128GCM;
use crate::encrypt::CipherBlobProvider;
use crate::encrypt::CipherReader;
use crate::encrypt::EncodingParams;
use crate::encrypt::JwkOct;
use crate::encrypt::MapKeySource;
use crate::encrypt::MasterKey;
use crate::encrypt::Padding;
#[cfg(test)]
use crate::encrypt::drop_pair_tags;
use crate::encrypt::ensure_stored;
use crate::encrypt::get_decrypted;
use crate::pair_roots::PairRoots;
use crate::pair_roots::VirtualPair;
use automerge::ChangeHash;
use big_blobs_core::Blob;
use big_blobs_core::BlobId;
use big_blobs_core::BlobPin;
use big_blobs_core::CipherBlob;
use big_blobs_core::Jwk;
use big_blobs_core::Representation;
use big_blobs_core::Rfc8188EncodingParameters;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DatumReference;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DatumVersion;
use big_cabinet_core::doc::DatumsRead;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use bytes::Bytes;
use iroh_blobs::Hash;
use iroh_blobs::HashAndFormat;
use iroh_blobs::api::Store;
use iroh_blobs::api::TempTag;
use serde_json::Value;
use service_client_native_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use url::Url;
use utils_rs::prelude::Res;
use utils_rs::prelude::eyre;

#[derive(Clone)]
pub struct DocumentBlobs {
    docs: Client<DocsProtocol>,
    capability: CapId,
    store: Store,
    provider: Arc<CipherBlobProvider>,
    roots: PairRoots,
}

#[derive(Debug, Default)]
pub struct DocumentReconcile {
    pub heads: Vec<ChangeHash>,
    pub retained: Vec<RetainedRepresentation>,
    pub claims: DocumentClaims,
    pub deferred: Vec<DocumentDeferral>,
}

#[derive(Debug, Default)]
pub struct DocumentClaims {
    pub heads: Vec<ChangeHash>,
    pub representations: BTreeMap<BlobId, u64>,
    pub pins: BTreeMap<BlobId, u64>,
    pub deferred: Vec<DocumentDeferral>,
}

impl DocumentClaims {
    pub fn is_claimed(&self, id: &BlobId) -> Option<bool> {
        if self.representations.contains_key(id) || self.pins.contains_key(id) {
            Some(true)
        } else if self.deferred.is_empty() {
            Some(false)
        } else {
            None
        }
    }

    pub fn release_candidates(&self, previous: &BTreeSet<BlobId>) -> BTreeSet<BlobId> {
        previous
            .iter()
            .copied()
            .filter(|id| self.is_claimed(id) == Some(false))
            .collect()
    }

    fn from_read(doc_id: &DocId, read: &DatumsRead) -> Res<Self> {
        let mut claims = Self {
            heads: read.heads.clone(),
            ..Self::default()
        };
        for (key, datum) in &read.datums {
            let tag = key.tag().as_str();
            if tag != CipherBlob::ID && tag != BlobPin::ID {
                continue;
            }
            let reason = match datum {
                DatumRead::Accepted { value, .. } => {
                    let (id, length, entries) = if tag == CipherBlob::ID {
                        let representation: Representation =
                            serde_json::from_value(value["representation"].clone())?;
                        (
                            representation.digest.parse::<BlobId>().ok(),
                            representation.length_octets,
                            &mut claims.representations,
                        )
                    } else {
                        let pin: BlobPin = serde_json::from_value(value.clone())?;
                        (
                            std::str::from_utf8(key.slot_bytes())
                                .ok()
                                .and_then(|slot| slot.parse::<BlobId>().ok()),
                            pin.length_octets,
                            &mut claims.pins,
                        )
                    };
                    if let Some(id) = id {
                        if let Some(previous) = entries.insert(id, length) {
                            eyre::ensure!(
                                previous == length,
                                "accepted claims disagree on representation length"
                            );
                        }
                        continue;
                    }
                    DeferralReason::UnsupportedReference
                }
                DatumRead::Absent { .. } => continue,
                _ => DeferralReason::DatumUnavailable {
                    doc_id: doc_id.clone(),
                    datum_key: key.clone(),
                },
            };
            claims.deferred.push(DocumentDeferral {
                datum_key: Some(key.clone()),
                reason,
            });
        }
        for (id, length) in &claims.pins {
            if let Some(representation_length) = claims.representations.get(id) {
                eyre::ensure!(
                    representation_length == length,
                    "accepted pin disagrees with representation length"
                );
            }
        }
        Ok(claims)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedRepresentation {
    pub blob_key: DatumKey,
    pub cipher_key: DatumKey,
    pub plaintext: BlobId,
    pub ciphertext: BlobId,
    pub length_octets: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentDeferral {
    pub datum_key: Option<DatumKey>,
    pub reason: DeferralReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeferralReason {
    Document {
        doc_id: DocId,
        error: DocsError,
    },
    DatumUnavailable {
        doc_id: DocId,
        datum_key: DatumKey,
    },
    BytesUnavailable {
        plaintext: BlobId,
        ciphertext: Option<BlobId>,
    },
    UnsupportedEncoding {
        content_encoding: String,
    },
    UnsupportedReference,
    UnsupportedKey,
}

type Attempt<T> = Res<Result<T, DeferralReason>>;

impl DocumentBlobs {
    pub fn new(
        docs: Client<DocsProtocol>,
        capability: CapId,
        store: Store,
        provider: Arc<CipherBlobProvider>,
        roots: PairRoots,
    ) -> Self {
        Self {
            docs,
            capability,
            store,
            provider,
            roots,
        }
    }

    pub async fn claims(&self, context: &RequestContext, doc_id: &DocId) -> Res<DocumentClaims> {
        match self.read(context, doc_id, At::Current, None).await? {
            Ok(read) => DocumentClaims::from_read(doc_id, &read),
            Err(reason) => Ok(DocumentClaims {
                deferred: vec![DocumentDeferral {
                    datum_key: None,
                    reason,
                }],
                ..DocumentClaims::default()
            }),
        }
    }

    pub async fn reconcile(
        &self,
        context: &RequestContext,
        doc_id: &DocId,
    ) -> Res<DocumentReconcile> {
        let mut report = DocumentReconcile::default();
        let read = match self.read(context, doc_id, At::Current, None).await? {
            Ok(read) => read,
            Err(reason) => {
                let deferral = DocumentDeferral {
                    datum_key: None,
                    reason,
                };
                report.claims.deferred.push(deferral.clone());
                report.deferred.push(deferral);
                return Ok(report);
            }
        };
        report.claims = DocumentClaims::from_read(doc_id, &read)?;
        report.deferred = report.claims.deferred.clone();
        let mut heads = read.heads.clone();
        for (blob_key, datum) in &read.datums {
            if blob_key.tag().as_str() != Blob::ID {
                continue;
            }
            let DatumRead::Accepted { value, .. } = datum else {
                if !matches!(datum, DatumRead::Absent { .. }) {
                    report.deferred.push(DocumentDeferral {
                        datum_key: Some(blob_key.clone()),
                        reason: DeferralReason::DatumUnavailable {
                            doc_id: doc_id.clone(),
                            datum_key: blob_key.clone(),
                        },
                    });
                }
                continue;
            };
            let blob: Blob = serde_json::from_value(value.clone())?;
            let plaintext = plaintext_id(&blob)?;
            let (jwk_key, default_cipher_key) = representation_keys(blob_key)?;
            let via_keys = match resolution_keys(doc_id, &blob, &plaintext) {
                Ok(keys) => keys,
                Err(reason) => {
                    report.deferred.push(DocumentDeferral {
                        datum_key: Some(blob_key.clone()),
                        reason,
                    });
                    continue;
                }
            };
            // Resolution order (ADR 026 §§2-3): the naming-convention slot
            // in this doc first, then any `?via=` hint left on the Blob,
            // then the legacy hashed representation slot. New writes only
            // ever create the naming-convention slot; the other two are
            // read fallbacks for documents written before ADR 026.
            let mut cipher_keys: Vec<DatumKey> = Vec::new();
            let push_key = |key: DatumKey, keys: &mut Vec<DatumKey>| {
                if !keys.contains(&key) {
                    keys.push(key);
                }
            };
            let present = |key: &DatumKey| {
                read.datums
                    .get(key)
                    .is_some_and(|datum| !matches!(datum, DatumRead::Absent { .. }))
            };
            if present(&default_cipher_key) {
                push_key(default_cipher_key.clone(), &mut cipher_keys);
            }
            for key in via_keys {
                push_key(key, &mut cipher_keys);
            }
            let (_, legacy_cipher_key) = legacy_representation_keys(doc_id, blob_key, &plaintext)?;
            if present(&legacy_cipher_key) {
                push_key(legacy_cipher_key, &mut cipher_keys);
            }
            if cipher_keys.is_empty() {
                let result = self
                    .create(
                        context,
                        doc_id,
                        &read,
                        &mut heads,
                        blob_key,
                        &blob,
                        &plaintext,
                        &jwk_key,
                        &default_cipher_key,
                    )
                    .await?;
                if !record_result(&mut report, doc_id, blob_key, result)? {
                    break;
                }
            } else {
                let mut stale = false;
                for cipher_key in cipher_keys {
                    let result = self
                        .reuse(
                            context,
                            doc_id,
                            &read,
                            &mut heads,
                            blob_key,
                            &blob,
                            &plaintext,
                            &cipher_key,
                        )
                        .await?;
                    if !record_result(&mut report, doc_id, &cipher_key, result)? {
                        stale = true;
                        break;
                    }
                }
                if stale {
                    break;
                }
            }
        }
        report.claims.heads = heads.clone();
        report.heads = heads;
        Ok(report)
    }

    async fn read(
        &self,
        context: &RequestContext,
        doc_id: &DocId,
        at: At,
        datums: Option<BTreeSet<DatumKey>>,
    ) -> Attempt<DatumsRead> {
        match self
            .docs
            .call(
                context.child(),
                ReadDatums {
                    capability: self.capability,
                    doc_id: doc_id.clone(),
                    at,
                    datums,
                    optimistic: false,
                },
            )
            .await
        {
            Ok(read) => Ok(Ok(read)),
            Err(error) => document_error(doc_id, error),
        }
    }

    async fn write(
        &self,
        context: &RequestContext,
        doc_id: &DocId,
        heads: &mut Vec<ChangeHash>,
        set: BTreeMap<DatumKey, Value>,
    ) -> Attempt<()> {
        let mut expected = heads.clone();
        expected.sort();
        let mut request = WriteDatums {
            capability: self.capability,
            command: None,
            doc_id: doc_id.clone(),
            mode: WriteMode::IfDocUnchanged { heads: expected },
            set,
            remove: BTreeSet::new(),
        };
        request.command = Some(command_id(&request)?);
        match self.docs.call(context.child(), request).await {
            Ok(receipt) => {
                eyre::ensure!(
                    receipt.doc_id == *doc_id,
                    "write receipt names a different document"
                );
                *heads = receipt.heads;
                Ok(Ok(()))
            }
            Err(error) => document_error(doc_id, error),
        }
    }

    async fn ciphertext_length(&self, hash: Hash) -> Res<u64> {
        match self.store.blobs().status(hash).await? {
            iroh_blobs::api::blobs::BlobStatus::Complete { size } => Ok(size),
            status => eyre::bail!("rooted ciphertext {hash} is not complete: {status:?}"),
        }
    }

    async fn plaintext(&self, blob: &Blob, plaintext: &BlobId) -> Res<Option<TempTag>> {
        let hash = Hash::from(plaintext.to_bytes32());
        let tag = self.store.tags().temp_tag(HashAndFormat::raw(hash)).await?;
        if self.store.blobs().has(hash).await?
            && let Some(reader) = self.store.sync_reader(hash).await?
        {
            eyre::ensure!(
                reader.len() == blob.length_octets,
                "plaintext length differs from Blob metadata"
            );
            return Ok(Some(tag));
        }
        let Some(inline) = &blob.inline else {
            return Ok(None);
        };
        eyre::ensure!(
            inline.len() as u64 == blob.length_octets,
            "inline plaintext length differs from Blob metadata"
        );
        eyre::ensure!(
            Hash::new(inline) == hash,
            "inline plaintext digest differs from Blob metadata"
        );
        let (tag, imported) = ensure_stored(
            &self.store,
            futures::stream::iter([Ok(Bytes::copy_from_slice(inline))]),
        )
        .await?;
        eyre::ensure!(
            imported == hash,
            "inline plaintext import changed its digest"
        );
        Ok(Some(tag))
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "each argument is a distinct pipeline input (context, capability, doc identity, datum keys, cursors); a params struct would only hide the flow"
    )]
    async fn create(
        &self,
        context: &RequestContext,
        doc_id: &DocId,
        read: &DatumsRead,
        heads: &mut Vec<ChangeHash>,
        blob_key: &DatumKey,
        blob: &Blob,
        plaintext: &BlobId,
        jwk_key: &DatumKey,
        cipher_key: &DatumKey,
    ) -> Attempt<RetainedRepresentation> {
        let Some(_plaintext_tag) = self.plaintext(blob, plaintext).await? else {
            return Ok(Err(DeferralReason::BytesUnavailable {
                plaintext: *plaintext,
                ciphertext: None,
            }));
        };
        let (key, new_jwk) = match read.datums.get(jwk_key) {
            Some(DatumRead::Accepted { value, .. }) => match master_key(value) {
                Some(key) => (key, None),
                None => return Ok(Err(DeferralReason::UnsupportedKey)),
            },
            None | Some(DatumRead::Absent { .. }) => {
                let key = MasterKey::random();
                let jwk = serde_json::to_value(JwkOct::from_master_key(&key))?;
                (key, Some(jwk))
            }
            Some(_) => {
                return Ok(Err(DeferralReason::DatumUnavailable {
                    doc_id: doc_id.clone(),
                    datum_key: jwk_key.clone(),
                }));
            }
        };
        let encoding = EncodingParams::DEFAULT;
        // Amended ADR 003 §19 ordering (ADR 026 §§2-3,10): the §11 pass and
        // pair registration happen first, with the cipherBlob value computed
        // in memory; the JWK and cipherBlob are then written into the
        // content doc in one transaction, which is the commit point. The
        // encryptor no longer writes `Blob.urls`.
        let c_hash = self
            .provider
            .install_with_provenance(
                &self.store,
                &self.roots,
                &key,
                Hash::from(plaintext.to_bytes32()),
                encoding,
                &doc_id.to_string(),
                "main",
            )
            .await?;
        let ciphertext = BlobId::new(*c_hash.as_bytes());
        let c_len = self.ciphertext_length(c_hash).await?;
        // A pre-existing JWK is pinned at the current doc heads, exactly as
        // the staged write pinned it before. A JWK minted by this write
        // cannot name the write's own resulting heads inside its own
        // payload, so it carries empty `key_ref_heads`: for an in-doc
        // naming-convention reference that means "the heads at which the
        // cipherBlob itself was written" (see `key`), which is precisely
        // the single transaction that also wrote the JWK.
        let key_heads = if new_jwk.is_some() {
            Vec::new()
        } else {
            heads.iter().map(ToString::to_string).collect()
        };
        let cipher = CipherBlob {
            representation: Representation {
                digest: ciphertext.digest_str(),
                length_octets: c_len,
                extra: Default::default(),
            },
            content_encoding: CONTENT_ENCODING_AES128GCM.into(),
            key_ref: format!("db+facet:///{doc_id}/{jwk_key}"),
            key_ref_heads: key_heads,
            encoding_parameters: Rfc8188EncodingParameters {
                record_size: encoding.record_size,
                padding: match encoding.padding {
                    Padding::Padme => "padme",
                }
                .into(),
                extra: Default::default(),
            },
            extra: Default::default(),
        };
        let mut set = BTreeMap::new();
        if let Some(jwk) = new_jwk {
            set.insert(jwk_key.clone(), jwk);
        }
        set.insert(cipher_key.clone(), serde_json::to_value(cipher)?);
        if let Err(reason) = self.write(context, doc_id, heads, set).await? {
            return Ok(Err(reason));
        }
        Ok(Ok(RetainedRepresentation {
            blob_key: blob_key.clone(),
            cipher_key: cipher_key.clone(),
            plaintext: *plaintext,
            ciphertext,
            length_octets: c_len,
        }))
    }

    async fn key(
        &self,
        context: &RequestContext,
        doc_id: &DocId,
        cipher_key: &DatumKey,
        cipher_version: &DatumVersion,
        cipher: &CipherBlob,
    ) -> Attempt<MasterKey> {
        let Some((key_doc, jwk_key, query_heads)) = legacy_reference(doc_id, &cipher.key_ref)
        else {
            return Ok(Err(DeferralReason::UnsupportedReference));
        };
        if jwk_key.tag().as_str() != Jwk::ID {
            return Ok(Err(DeferralReason::UnsupportedReference));
        }
        // ADR 026 §2: an in-doc (`self`/co-located) reference is accepted
        // when the cipherBlob follows the naming convention, i.e. its JWK
        // sits in the same doc at the same slot. Truly external references
        // still require pinned heads, as before.
        let in_doc_naming = key_doc == *doc_id
            && DatumKey::new(Jwk::ID, cipher_key.slot_bytes().to_vec())
                .ok()
                .as_ref()
                == Some(&jwk_key);
        let (at, read_doc) = if cipher.key_ref_heads.is_empty() {
            if !in_doc_naming {
                return Ok(Err(DeferralReason::UnsupportedReference));
            }
            if query_heads.is_some() {
                return Ok(Err(DeferralReason::UnsupportedReference));
            }
            // Empty heads on an in-doc reference mean "the same heads as
            // the cipherBlob holding the reference": the cipherBlob's own
            // datum version is the write that also wrote (or already
            // contained) the JWK, so resolving there pins the exact key
            // even if the JWK datum is later overwritten. Fixtures that
            // carry no version heads fall back to the current state.
            if cipher_version.heads.is_empty() {
                (At::Current, key_doc.clone())
            } else {
                let mut heads = cipher_version.heads.clone();
                heads.sort();
                heads.dedup();
                (At::Heads(heads), key_doc.clone())
            }
        } else {
            let mut heads = cipher
                .key_ref_heads
                .iter()
                .map(|head| head.parse::<ChangeHash>())
                .collect::<Result<Vec<_>, _>>()?;
            heads.sort();
            heads.dedup();
            if query_heads.is_some_and(|query| query != heads) {
                return Ok(Err(DeferralReason::UnsupportedReference));
            }
            (At::Heads(heads), key_doc.clone())
        };
        let read = match self
            .read(context, &read_doc, at, Some([jwk_key.clone()].into()))
            .await?
        {
            Ok(read) => read,
            Err(reason) => return Ok(Err(reason)),
        };
        let Some(DatumRead::Accepted { value, .. }) = read.datums.get(&jwk_key) else {
            return Ok(Err(DeferralReason::DatumUnavailable {
                doc_id: key_doc,
                datum_key: jwk_key,
            }));
        };
        Ok(master_key(value).ok_or(DeferralReason::UnsupportedKey))
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "each argument is a distinct pipeline input (context, capability, doc identity, datum keys, cursors); a params struct would only hide the flow"
    )]
    async fn reuse(
        &self,
        context: &RequestContext,
        doc_id: &DocId,
        read: &DatumsRead,
        _heads: &mut Vec<ChangeHash>,
        blob_key: &DatumKey,
        blob: &Blob,
        plaintext: &BlobId,
        cipher_key: &DatumKey,
    ) -> Attempt<RetainedRepresentation> {
        let Some(DatumRead::Accepted { value, version }) = read.datums.get(cipher_key) else {
            return Ok(Err(DeferralReason::DatumUnavailable {
                doc_id: doc_id.clone(),
                datum_key: cipher_key.clone(),
            }));
        };
        let cipher_version = version.clone();
        let content_encoding: String = serde_json::from_value(value["contentEncoding"].clone())?;
        if content_encoding != CONTENT_ENCODING_AES128GCM {
            return Ok(Err(DeferralReason::UnsupportedEncoding {
                content_encoding,
            }));
        }
        let cipher: CipherBlob = serde_json::from_value(value.clone())?;
        let padding = match cipher.encoding_parameters.padding.as_str() {
            "padme" => Padding::Padme,

            _ => {
                return Ok(Err(DeferralReason::UnsupportedEncoding {
                    content_encoding,
                }));
            }
        };
        let encoding = EncodingParams::new(cipher.encoding_parameters.record_size, padding)?;
        let ciphertext: BlobId = cipher.representation.digest.parse()?;
        let c_hash = Hash::from(ciphertext.to_bytes32());
        let p_hash = Hash::from(plaintext.to_bytes32());
        let key = match self
            .key(context, doc_id, cipher_key, &cipher_version, &cipher)
            .await?
        {
            Ok(key) => key,
            Err(reason) => return Ok(Err(reason)),
        };
        let mut plaintext_tag = self.plaintext(blob, plaintext).await?;
        let _ciphertext_tag = self
            .store
            .tags()
            .temp_tag(HashAndFormat::raw(c_hash))
            .await?;
        let cipher_present = self.store.blobs().has(c_hash).await?;
        if cipher_present {
            let length = self.ciphertext_length(c_hash).await?;
            eyre::ensure!(
                length == cipher.representation.length_octets,
                "ciphertext length differs from CipherBlob metadata"
            );
        }
        // Local plaintext presence normally stands in for ciphertext
        // verification: every production path that writes ciphertext into the
        // store (install, sync download) authenticates it first. The empty
        // blob breaks that inference - iroh-blobs reports `Hash::EMPTY` as
        // complete regardless of backend state, so an empty plaintext is
        // "present" in every store and says nothing about the ciphertext.
        // Decrypt-verify empty content before binding the pair.
        if (plaintext_tag.is_none() || blob.length_octets == 0)
            && cipher_present
            && self.store.sync_reader(c_hash).await?.is_some()
        {
            plaintext_tag = Some(
                self.decrypt(&key, c_hash, blob.length_octets, p_hash)
                    .await?,
            );
        }
        if plaintext_tag.is_none() {
            return Ok(Err(DeferralReason::BytesUnavailable {
                plaintext: *plaintext,
                ciphertext: Some(ciphertext),
            }));
        }
        self.provider
            .register_local_pair(
                &self.store,
                &self.roots,
                VirtualPair {
                    cipher: c_hash,
                    plaintext: p_hash,
                    key,
                    encoding,
                    source: serde_json::json!({
                        "kind": "accepted-cipher-blob",
                        "document": doc_id,
                        "datum": cipher_key,
                        "heads": cipher_version.heads,
                        "keyRef": cipher.key_ref,
                        "keyRefHeads": cipher.key_ref_heads,
                    })
                    .to_string(),
                    claimed_by: Some((doc_id.to_string(), "main".to_owned())),
                },
            )
            .await?;
        eyre::ensure!(
            self.ciphertext_length(c_hash).await? == cipher.representation.length_octets,
            "reconstructed ciphertext length differs from CipherBlob metadata"
        );
        drop(plaintext_tag);
        Ok(Ok(RetainedRepresentation {
            blob_key: blob_key.clone(),
            cipher_key: cipher_key.clone(),
            plaintext: *plaintext,
            ciphertext,
            length_octets: cipher.representation.length_octets,
        }))
    }

    async fn decrypt(
        &self,
        key: &MasterKey,
        ciphertext: Hash,
        length: u64,
        expected: Hash,
    ) -> Res<TempTag> {
        let keys = MapKeySource([(ciphertext, key.clone())].into());
        let (tag, actual) = if length == 0 {
            let size = self.ciphertext_length(ciphertext).await?;
            eyre::ensure!(
                size <= (1 << 20) + 276,
                "empty plaintext has more than one bounded ciphertext record"
            );
            let bytes = get_decrypted(&self.store, &keys, ciphertext).await?;
            eyre::ensure!(
                bytes.is_empty(),
                "ciphertext contradicts empty plaintext metadata"
            );
            ensure_stored(&self.store, futures::stream::iter([Ok(Bytes::from(bytes))])).await?
        } else {
            let reader = CipherReader::open(&self.store, &keys, ciphertext, length).await?;
            let stream =
                futures::stream::try_unfold((reader, 0u64), |(reader, offset)| async move {
                    if offset == reader.plaintext_len() {
                        return Ok(None);
                    }
                    let (reader, chunk) = tokio::task::spawn_blocking(move || {
                        let chunk = reader.read_plaintext_at(offset, 64 * 1024);
                        (reader, chunk)
                    })
                    .await
                    .expect("plaintext decoding task panicked");
                    let chunk = chunk.map_err(std::io::Error::other)?;
                    let next = offset + chunk.len() as u64;
                    Ok(Some((chunk, (reader, next))))
                });
            ensure_stored(&self.store, stream).await?
        };
        eyre::ensure!(
            actual == expected,
            "decrypted plaintext differs from the accepted Blob digest"
        );
        Ok(tag)
    }
}

fn document_error<T>(doc_id: &DocId, error: ClientError<DocsError>) -> Attempt<T> {
    match error {
        ClientError::Method(
            error @ (DocsError::Denied
            | DocsError::DocNotFound
            | DocsError::UnknownHeads { .. }
            | DocsError::Conflict { .. }
            | DocsError::CommandInFlight { .. }
            | DocsError::Pending { .. }
            | DocsError::BaseNotAccepted { .. }
            | DocsError::AuthorityUnavailable(_)),
        ) => Ok(Err(DeferralReason::Document {
            doc_id: doc_id.clone(),
            error,
        })),
        other => Err(other.into()),
    }
}

fn record_result(
    report: &mut DocumentReconcile,
    owner: &DocId,
    key: &DatumKey,
    result: Result<RetainedRepresentation, DeferralReason>,
) -> Res<bool> {
    match result {
        Ok(retained) => {
            if let Some(length) = report.claims.pins.get(&retained.ciphertext) {
                eyre::ensure!(
                    *length == retained.length_octets,
                    "accepted pin disagrees with reconciled representation length"
                );
            }
            if let Some(previous) = report
                .claims
                .representations
                .insert(retained.ciphertext, retained.length_octets)
            {
                eyre::ensure!(
                    previous == retained.length_octets,
                    "accepted claims disagree on reconciled representation length"
                );
            }
            report.retained.push(retained);
            Ok(true)
        }
        Err(reason) => {
            let stop = matches!(
                &reason,
                DeferralReason::Document { doc_id, .. } if doc_id == owner
            );
            let deferral = DocumentDeferral {
                datum_key: Some(key.clone()),
                reason,
            };
            if stop {
                report.claims.deferred.push(deferral.clone());
            }
            if !report.deferred.contains(&deferral) {
                report.deferred.push(deferral);
            }
            Ok(!stop)
        }
    }
}

fn command_id(request: &WriteDatums) -> Res<CommandId> {
    let digest = blake3::hash(&serde_json::to_vec(request)?);
    Ok(format!("big-blobs-v1-{digest}").try_into()?)
}

/// ADR 026 §3 naming convention: the cipherBlob for a Blob lives at
/// `cipherBlob/<the Blob datum's key slot>` and its JWK at
/// `jwk/<the same slot>`, in the same doc. One slot per Blob datum key:
/// no plaintext-digest discriminator on new writes.
fn representation_keys(blob_key: &DatumKey) -> Res<(DatumKey, DatumKey)> {
    let slot = blob_key.slot_bytes().to_vec();
    Ok((
        DatumKey::new(Jwk::ID, slot.clone())?,
        DatumKey::new(CipherBlob::ID, slot)?,
    ))
}

/// Pre-ADR-026 hashed slots (`big-blobs-<blake3(doc||blob||plaintext)>`),
/// retained only as a read fallback for documents already written.
fn legacy_representation_keys(
    doc_id: &DocId,
    blob_key: &DatumKey,
    plaintext: &BlobId,
) -> Res<(DatumKey, DatumKey)> {
    let digest = blake3::hash(&serde_json::to_vec(&(doc_id, blob_key, plaintext))?);
    let slot = format!("big-blobs-{digest}");
    Ok((
        DatumKey::from_text(Jwk::ID, slot.clone())?,
        DatumKey::from_text(CipherBlob::ID, slot)?,
    ))
}

fn master_key(value: &Value) -> Option<MasterKey> {
    serde_json::from_value::<JwkOct>(value.clone())
        .ok()?
        .to_master_key()
        .ok()
}

fn plaintext_id(blob: &Blob) -> Res<BlobId> {
    if let Ok(id) = blob.digest.parse() {
        return Ok(id);
    }
    for value in blob.urls.as_deref().unwrap_or_default() {
        let Ok(url) = Url::parse(value) else {
            continue;
        };
        if matches!(url.scheme(), "db+blob" | "daybook-blob")
            && url.host_str().is_none()
            && let Ok(id) = url.path().trim_start_matches('/').parse()
        {
            return Ok(id);
        }
    }
    eyre::bail!("accepted Blob has no supported plaintext digest")
}

fn legacy_reference(
    owner: &DocId,
    value: &str,
) -> Option<(DocId, DatumKey, Option<Vec<ChangeHash>>)> {
    let url = Url::parse(value).ok()?;
    if url.scheme() != "db+facet"
        || url.host_str().is_some()
        || !url.username().is_empty()
        || url.port().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    let raw = value.split_once(':')?.1.split('?').next()?;
    let path = raw.strip_prefix("///").or_else(|| raw.strip_prefix('/'))?;
    let reference = DatumReference::parse(&format!("db+datum:/{path}")).ok()?;
    let mut heads = None;
    let mut branch_seen = false;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "branch" if !branch_seen && value == "main" => branch_seen = true,
            "at" if heads.is_none() => {
                let mut parsed = value
                    .split('|')
                    .map(str::parse::<ChangeHash>)
                    .collect::<Result<Vec<_>, _>>()
                    .ok()?;
                if parsed.is_empty() {
                    return None;
                }
                parsed.sort();
                parsed.dedup();
                heads = Some(parsed);
            }
            _ => return None,
        }
    }
    Some((
        reference.document().resolve(owner),
        reference.key().clone(),
        heads,
    ))
}

fn resolution_keys(
    doc_id: &DocId,
    blob: &Blob,
    plaintext: &BlobId,
) -> Result<BTreeSet<DatumKey>, DeferralReason> {
    let mut keys = BTreeSet::new();
    for value in blob.urls.as_deref().unwrap_or_default() {
        let Ok(url) = Url::parse(value) else {
            continue;
        };
        if !matches!(url.scheme(), "db+blob" | "daybook-blob")
            || url.host_str().is_some()
            || url
                .path()
                .trim_start_matches('/')
                .parse::<BlobId>()
                .ok()
                .as_ref()
                != Some(plaintext)
        {
            continue;
        }
        for (_, via) in url.query_pairs().filter(|(key, _)| key == "via") {
            let key = if via.starts_with("db+facet:") {
                let Some((owner, key, heads)) = legacy_reference(doc_id, &via) else {
                    return Err(DeferralReason::UnsupportedReference);
                };
                if owner != *doc_id || heads.is_some() {
                    return Err(DeferralReason::UnsupportedReference);
                }
                key
            } else {
                via.parse()
                    .map_err(|_| DeferralReason::UnsupportedReference)?
            };
            if key.tag().as_str() != CipherBlob::ID {
                return Err(DeferralReason::UnsupportedReference);
            }
            keys.insert(key);
        }
    }
    Ok(keys)
}

#[cfg(test)]
mod tests;
