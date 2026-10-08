//! The document-layer half of the key seam (ADR 003 §6/§19).
//!
//! [`CipherKeySource`] itself is deliberately free of repo types: it is the
//! codec's minimal currency, and the codec must not know what a drawer is. This
//! module is where the repo answers it: ciphertext -> the `cipherBlob` facet
//! that names it -> `keyRef` plus the heads it pinned -> the JWK facet in the
//! key document at those heads -> the secret to decrypt with.

use crate::interlude::*;

use crate::blobs::BlobId;
use crate::blobs::encrypt::{CipherKeySource, EncodingParams, JwkOct, MasterKey};
use crate::drawer::DrawerRepo;
use daybook_types::doc::{
    BranchPathBuf, CipherBlob, DocId, FacetKey, FacetRaw, FacetTag, Jwk, WellKnownFacet,
    WellKnownFacetTag,
};
use daybook_types::url::{FACET_SELF_DOC_ID, parse_facet_ref};
use iroh_blobs::Hash;

/// Resolves a representation's key through the document that names it.
///
/// A `cipherBlob` facet lives beside the `Blob` facet it mirrors (ADR 003 §3),
/// so a ciphertext is resolved *from an owning document*: one instance answers
/// for the ciphertexts `doc_id` names, and the caller that holds the document -
/// the one that read the `Blob` facet naming the representation - is the one
/// that constructs it. The key seam itself carries only the ciphertext hash, so
/// there is nothing here to search the repo with.
pub struct DocKeySource {
    drawer: Arc<DrawerRepo>,
    doc_id: DocId,
    branch: BranchPathBuf,
}

impl DocKeySource {
    pub fn new(drawer: Arc<DrawerRepo>, doc_id: DocId, branch: BranchPathBuf) -> Self {
        Self {
            drawer,
            doc_id,
            branch,
        }
    }

    /// The `cipherBlob` facet of the owning document that names the ciphertext.
    ///
    /// Read at the document's current branch heads, because "which key does this
    /// ciphertext use" is asked about the representation the document names
    /// now - the historical form would need heads, and the codec's callers pass
    /// only the hash.
    async fn cipherblob_facet(&self, ct_hash: &Hash) -> Res<CipherBlob> {
        let heads = self
            .drawer
            .get_branch_heads_for_path(&self.doc_id, &self.branch)
            .await?
            .ok_or_else(|| {
                eyre::eyre!(
                    "document {} has no {} branch, so it names no representations",
                    self.doc_id,
                    self.branch
                )
            })?;
        let keys = self
            .drawer
            .facet_keys_at_branch_heads(&self.doc_id, &self.branch, &heads)
            .await?
            .ok_or_else(|| {
                eyre::eyre!(
                    "document {} is not readable at its own heads {heads:?}",
                    self.doc_id
                )
            })?;
        let cipher_keys: Vec<FacetKey> = keys
            .into_iter()
            .filter(|key| key.tag == FacetTag::WellKnown(WellKnownFacetTag::CipherBlob))
            .collect();
        let Some(doc) = self
            .drawer
            .get_doc_with_facets_at_branch_heads(
                &self.doc_id,
                &self.branch,
                &heads,
                Some(cipher_keys),
            )
            .await?
        else {
            eyre::bail!(
                "document {} became unreadable while resolving ciphertext {ct_hash}",
                self.doc_id
            );
        };

        let wanted = blob_id_for_hash(ct_hash);
        let mut named: Vec<(FacetKey, CipherBlob)> = Vec::new();
        for (key, raw) in &doc.facets {
            let cipher = cipherblob_from_raw(raw)?;
            if blob_id_for_facet_digest(&cipher.representation.digest).as_ref() == Some(&wanted) {
                named.push((key.clone(), cipher));
            }
        }
        match named.len() {
            0 => eyre::bail!(
                "no cipherBlob facet in document {} names ciphertext {ct_hash}",
                self.doc_id
            ),
            1 => Ok(named.pop().expect("length checked").1),
            // Two domains in one document can name the same bytes (the same key
            // under the same plaintext is the same ciphertext). Picking one
            // would be picking a key for the caller, so say so instead.
            _ => {
                named.sort_by_key(|(key, _)| key.to_string());
                eyre::bail!(
                    "document {} names ciphertext {ct_hash} in {} cipherBlob facets ({}), so which key it uses is ambiguous",
                    self.doc_id,
                    named.len(),
                    named
                        .iter()
                        .map(|(key, _)| key.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
    }

    /// The JWK facet `cipher.keyRef` names, read at the heads that reference
    /// pinned.
    ///
    /// Cross-document is the only shape this accepts. ADR 003 §6 keeps the key
    /// out of the document whose readers are only entitled to a representation,
    /// and §19 makes the pinned heads the thing that keeps an existing
    /// representation decryptable across a rotation, so a reference that cannot
    /// pin them cannot be honoured.
    async fn jwk_at(&self, cipher: &CipherBlob) -> Res<Jwk> {
        let reference = parse_facet_ref(&cipher.key_ref).wrap_err_with(|| {
            format!(
                "cipherBlob keyRef {:?} is not a facet reference",
                cipher.key_ref
            )
        })?;
        eyre::ensure!(
            reference.doc_id != FACET_SELF_DOC_ID,
            "cipherBlob keyRef names this document ('{FACET_SELF_DOC_ID}'): the key belongs in a document the holders of the representation cannot read (ADR 003 §6, §19)"
        );
        eyre::ensure!(
            reference.facet_key.tag == FacetTag::WellKnown(WellKnownFacetTag::Jwk),
            "cipherBlob keyRef names facet tag {} rather than the JWK tag",
            reference.facet_key.tag
        );
        eyre::ensure!(
            !cipher.key_ref_heads.0.is_empty(),
            "cipherBlob keyRef into document {} pins no heads, so the key state it meant cannot be recovered (ADR 003 §19)",
            reference.doc_id
        );

        // ADR 007 §3: a reference may pin its branch; `main` is what every
        // writer in the workspace uses, and the pinned heads are what select
        // the state either way.
        let branch = BranchPathBuf::from(reference.branch.as_deref().unwrap_or("main"));
        let key_doc = self
            .drawer
            .get_doc_with_facets_at_branch_heads(
                &reference.doc_id,
                &branch,
                &cipher.key_ref_heads,
                Some(vec![reference.facet_key.clone()]),
            )
            .await?
            .ok_or_else(|| {
                eyre::eyre!(
                    "key document {} has no readable state for facet {} at the pinned heads {:?}",
                    reference.doc_id,
                    reference.facet_key,
                    cipher.key_ref_heads
                )
            })?;
        let raw = key_doc.facets.get(&reference.facet_key).ok_or_else(|| {
            eyre::eyre!(
                "key document {} holds no facet {} at the pinned heads {:?}",
                reference.doc_id,
                reference.facet_key,
                cipher.key_ref_heads
            )
        })?;
        match WellKnownFacet::from_json(raw.clone(), WellKnownFacetTag::Jwk)? {
            WellKnownFacet::Jwk(jwk) => Ok(jwk),
            _ => unreachable!("Jwk facet decoded to another well-known variant"),
        }
    }
}

#[async_trait::async_trait]
impl CipherKeySource for DocKeySource {
    async fn key_for(&self, ct_hash: &Hash) -> Res<MasterKey> {
        let cipher = self.cipherblob_facet(ct_hash).await?;
        let jwk = self.jwk_at(&cipher).await?;
        master_key_from_jwk(&jwk, ct_hash)
    }

    /// The framing `C` was built with, from the same facet `key_for` resolves
    /// through: the codec re-installs or serves `C` with it. Decryption itself
    /// reads `rs` from the authenticated header instead.
    async fn encoding_for(&self, ct_hash: &Hash) -> Res<EncodingParams> {
        let cipher = self.cipherblob_facet(ct_hash).await?;
        EncodingParams::from_encoding_parameters(
            &cipher.content_encoding,
            &cipher.encoding_parameters,
        )
    }
}

/// The 32 octets a facet digest names.
///
/// Both spellings in circulation are accepted: the multihash one ADR 003 §3
/// makes canonical (`blob_id_to_digest_str`), and the plain base58 id spelling
/// `BlobId::to_string` produces, which `pin_worker`'s pin derivation accepts.
/// Each decodes to the same 32 octets, so accepting both cannot match a
/// *different* ciphertext.
fn blob_id_for_facet_digest(digest: &str) -> Option<BlobId> {
    crate::blobs::digest_str_to_blob_id(digest)
        .ok()
        .or_else(|| digest.parse().ok())
}

fn blob_id_for_hash(hash: &Hash) -> BlobId {
    BlobId::new(*hash.as_bytes())
}

fn cipherblob_from_raw(raw: &FacetRaw) -> Res<CipherBlob> {
    match WellKnownFacet::from_json(raw.clone(), WellKnownFacetTag::CipherBlob)? {
        WellKnownFacet::CipherBlob(cipher) => Ok(cipher),
        _ => unreachable!("CipherBlob facet decoded to another well-known variant"),
    }
}

/// The secret a JWK facet carries, through the codec's own JWK codec: this seam
/// must not grow a second reading of the same wire shape.
fn master_key_from_jwk(jwk: &Jwk, ct_hash: &Hash) -> Res<MasterKey> {
    let key_material = jwk
        .members
        .get("k")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            eyre::eyre!(
                "JWK (kty {:?}) for ciphertext {ct_hash} carries no `k` member",
                jwk.kty
            )
        })?;
    JwkOct {
        kty: jwk.kty.clone(),
        k: key_material.to_owned(),
    }
    .to_master_key()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::SqlCtx;
    use crate::blobs::encrypt::{
        CONTENT_ENCODING_AES128GCM, CipherBlobProvider, Padding, add_encrypted, get_decrypted,
    };
    use crate::blobs::pair_roots::PairRoots;
    use crate::test_support::{stage_key_doc, test_cx, write_jwk_facet};
    use daybook_types::doc::{AddDocArgs, BranchPath, ChangeHashSet, DocPatch, Representation};
    use iroh_blobs::api::{Store, proto::BlobStatus};
    use iroh_blobs::store::mem::MemStore;

    /// A throwaway root record: these tests exercise the store-level provider, so
    /// the record only has to exist because every rooting path writes it before
    /// the tags it guards.
    async fn test_pair_roots() -> Res<PairRoots> {
        PairRoots::boot(SqlCtx::memory().await?).await
    }

    /// ADR 003 §3 makes the multihash spelling canonical for a facet digest.
    fn multihash_digest(c: Hash) -> String {
        crate::blobs::blob_id_to_digest_str(blob_id_for_hash(&c))
    }

    /// The plain base58 id spelling, which `BlobId::to_string` produces and the
    /// pin worker's `digest.parse::<BlobId>()` rule accepts.
    fn plain_digest(c: Hash) -> String {
        blob_id_for_hash(&c).to_string()
    }

    /// A refusal from the key seam. `Result::expect_err` needs `Debug`, which
    /// key material deliberately does not implement.
    fn expect_error<T>(result: Res<T>, what: &str) -> eyre::Report {
        match result {
            Ok(_) => panic!("{what}"),
            Err(err) => err,
        }
    }

    /// The ciphertext length as the store actually holds it: the facet records
    /// it, resolution never reads it, and a fixture has no business inventing
    /// a length it can ask for.
    async fn ciphertext_len_of(store: &Store, c: Hash) -> Res<u64> {
        match store
            .blobs()
            .status(c)
            .await
            .map_err(|err| eyre::eyre!("{err:?}"))?
        {
            BlobStatus::Complete { size } => Ok(size),
            other => eyre::bail!("fixture ciphertext {c} is not complete: {other:?}"),
        }
    }

    /// A content document with no facets yet - what a caller holding a `Blob`
    /// facet reads its `cipherBlob` facet out of.
    async fn stage_content_doc(drawer: &DrawerRepo) -> Res<DocId> {
        let doc_id = drawer
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: default(),
                user_path: None,

                idempotency_key: "test-key-key_source.rs-0".to_string(),
            })
            .await?;
        Ok(doc_id)
    }

    /// Write a `cipherBlob` facet for `domain` naming the representation in
    /// `cipher`: the shape the encryption worker writes (ADR 003 §19), and the
    /// only thing the key seam reads.
    async fn stage_cipherblob(
        drawer: &DrawerRepo,
        doc_id: &DocId,
        domain: &str,
        cipher: CipherBlob,
    ) -> Res<()> {
        drawer
            .update_at_heads_with_scope(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: [(
                        FacetKey {
                            tag: FacetTag::WellKnown(WellKnownFacetTag::CipherBlob),
                            id: domain.to_string(),
                        },
                        FacetRaw::from(WellKnownFacet::CipherBlob(cipher)),
                    )]
                    .into(),
                    facets_remove: vec![],
                    user_path: None,
                },
                BranchPath::new("main"),
                None,
                crate::drawer::FacetWriteScope::System,
            )
            .await?;
        Ok(())
    }

    /// The key the document layer answers with is the key the ciphertext was
    /// built with, and the framing comes from the facet rather than from the
    /// codec's constant - in both digest spellings, both paddings, and at a
    /// record size that is *not* the default (a constant answer would pass a
    /// default-only test).
    #[tokio::test(flavor = "multi_thread")]
    async fn key_source_resolves_the_representation_key() -> Res<()> {
        let ctx = test_cx(utils_rs::function_full!()).await?;
        let drawer = Arc::clone(&ctx.drawer_repo);
        let (store, virtuals) = MemStore::new_with_virtuals(Default::default());
        let store = Store::from(store);
        let provider = Arc::new(CipherBlobProvider::new());
        let key = MasterKey::random();
        let (key_doc_id, key_heads) = stage_key_doc(&drawer, &key).await?;
        let key_ref = format!("db+facet:///{key_doc_id}/org.example.daybook.jwk/relay");
        let doc_id = stage_content_doc(&drawer).await?;
        let source = DocKeySource::new(
            Arc::clone(&drawer),
            doc_id.clone(),
            BranchPathBuf::from("main"),
        );

        let small = EncodingParams::new(1024, Padding::Record)?;
        let plain_a = b"key source round trip at a non-default record size".to_vec();
        let (c_a, _) = add_encrypted(
            &store,
            &provider,
            &test_pair_roots().await?,
            &key,
            small,
            &plain_a,
        )
        .await?;
        provider.register(&virtuals)?;
        stage_cipherblob(
            &drawer,
            &doc_id,
            "relay",
            CipherBlob {
                representation: Representation {
                    digest: multihash_digest(c_a),
                    length_octets: ciphertext_len_of(&store, c_a).await?,
                },
                content_encoding: CONTENT_ENCODING_AES128GCM.to_string(),
                key_ref: key_ref.parse()?,
                key_ref_heads: key_heads.clone(),
                encoding_parameters: small.to_encoding_parameters(),
            },
        )
        .await?;

        assert_eq!(get_decrypted(&store, &source, c_a).await?, plain_a);
        assert_eq!(
            source.encoding_for(&c_a).await?,
            small,
            "the facet's recordSize drives reconstruction, not the codec's constant"
        );

        // Minimal padding, under the plain id spelling of the same 32 octets.
        let minimal = EncodingParams::new(EncodingParams::DEFAULT.record_size, Padding::Minimal)?;
        let plain_b = b"a short payload, so the final record is the short one".to_vec();
        let (c_b, _) = add_encrypted(
            &store,
            &provider,
            &test_pair_roots().await?,
            &key,
            minimal,
            &plain_b,
        )
        .await?;
        stage_cipherblob(
            &drawer,
            &doc_id,
            "relay-minimal",
            CipherBlob {
                representation: Representation {
                    digest: plain_digest(c_b),
                    length_octets: ciphertext_len_of(&store, c_b).await?,
                },
                content_encoding: CONTENT_ENCODING_AES128GCM.to_string(),
                key_ref: key_ref.parse()?,
                key_ref_heads: key_heads,
                encoding_parameters: minimal.to_encoding_parameters(),
            },
        )
        .await?;

        assert_eq!(get_decrypted(&store, &source, c_b).await?, plain_b);
        assert_eq!(
            source.encoding_for(&c_b).await?,
            minimal,
            "the facet's padding drives reconstruction, not the codec's constant"
        );

        ctx.stop().await?;
        Ok(())
    }

    /// Rotating the JWK in place does not change what an existing
    /// representation decrypts under, because the reference pinned the state it
    /// meant (ADR 003 §15/§19). Asserted in both directions: the pin resolves
    /// the *old* key, and the current state resolves a different key that
    /// cannot decrypt this ciphertext - so the test cannot pass by resolving
    /// nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn key_source_reads_the_pinned_key_state_not_the_latest() -> Res<()> {
        let ctx = test_cx(utils_rs::function_full!()).await?;
        let drawer = Arc::clone(&ctx.drawer_repo);
        let (store, virtuals) = MemStore::new_with_virtuals(Default::default());
        let store = Store::from(store);
        let provider = Arc::new(CipherBlobProvider::new());
        let key_a = MasterKey::random();
        let (key_doc_id, heads_a) = stage_key_doc(&drawer, &key_a).await?;
        let key_ref = format!("db+facet:///{key_doc_id}/org.example.daybook.jwk/relay");

        let plaintext = b"encrypted under the key the facet pinned".to_vec();
        let encoding = EncodingParams::DEFAULT;
        let (c, _) = add_encrypted(
            &store,
            &provider,
            &test_pair_roots().await?,
            &key_a,
            encoding,
            &plaintext,
        )
        .await?;
        provider.register(&virtuals)?;
        let pinned_doc = stage_content_doc(&drawer).await?;
        stage_cipherblob(
            &drawer,
            &pinned_doc,
            "relay",
            CipherBlob {
                representation: Representation {
                    digest: multihash_digest(c),
                    length_octets: ciphertext_len_of(&store, c).await?,
                },
                content_encoding: CONTENT_ENCODING_AES128GCM.to_string(),
                key_ref: key_ref.parse()?,
                key_ref_heads: heads_a.clone(),
                encoding_parameters: encoding.to_encoding_parameters(),
            },
        )
        .await?;

        // Rotate the key material in place, as §15's rotation does.
        let key_b = MasterKey::random();
        let heads_b = write_jwk_facet(&drawer, &key_doc_id, &key_b).await?;
        assert_ne!(heads_a, heads_b, "the rotation must be a real write");

        let pinned =
            DocKeySource::new(Arc::clone(&drawer), pinned_doc, BranchPathBuf::from("main"));
        assert!(
            pinned.key_for(&c).await? == key_a,
            "the pinned heads select the key this representation was built with"
        );
        assert_eq!(get_decrypted(&store, &pinned, c).await?, plaintext);

        let latest_doc = stage_content_doc(&drawer).await?;
        stage_cipherblob(
            &drawer,
            &latest_doc,
            "relay",
            CipherBlob {
                representation: Representation {
                    digest: multihash_digest(c),
                    length_octets: ciphertext_len_of(&store, c).await?,
                },
                content_encoding: CONTENT_ENCODING_AES128GCM.to_string(),
                key_ref: key_ref.parse()?,
                key_ref_heads: heads_b,
                encoding_parameters: encoding.to_encoding_parameters(),
            },
        )
        .await?;
        let latest =
            DocKeySource::new(Arc::clone(&drawer), latest_doc, BranchPathBuf::from("main"));
        assert!(
            latest.key_for(&c).await? == key_b,
            "the rotated state really is a different key"
        );
        assert!(
            get_decrypted(&store, &latest, c).await.is_err(),
            "the rotated key must not decrypt what the pinned key encrypted"
        );

        ctx.stop().await?;
        Ok(())
    }

    /// Every way a key can fail to resolve is loud and names what was wrong: a
    /// half-resolved key silently serving the wrong bytes is the failure this
    /// seam exists to prevent.
    #[tokio::test(flavor = "multi_thread")]
    async fn key_source_refuses_unresolvable_keys() -> Res<()> {
        let ctx = test_cx(utils_rs::function_full!()).await?;
        let drawer = Arc::clone(&ctx.drawer_repo);
        let key = MasterKey::random();
        let (key_doc_id, key_heads) = stage_key_doc(&drawer, &key).await?;
        let key_ref = format!("db+facet:///{key_doc_id}/org.example.daybook.jwk/relay");
        let encoding = EncodingParams::DEFAULT;

        // A resolvable representation: the positive control, and the document
        // that also proves an unstaged ciphertext is not silently answered for.
        let good_doc = stage_content_doc(&drawer).await?;
        let good_c = Hash::new(b"a staged representation");
        stage_cipherblob(
            &drawer,
            &good_doc,
            "relay",
            CipherBlob {
                representation: Representation {
                    digest: multihash_digest(good_c),
                    length_octets: 128,
                },
                content_encoding: CONTENT_ENCODING_AES128GCM.to_string(),
                key_ref: key_ref.parse()?,
                key_ref_heads: key_heads.clone(),
                encoding_parameters: encoding.to_encoding_parameters(),
            },
        )
        .await?;
        let good = DocKeySource::new(Arc::clone(&drawer), good_doc, BranchPathBuf::from("main"));
        assert!(
            good.key_for(&good_c).await? == key,
            "the control case must resolve, or every refusal below proves nothing"
        );
        let err = expect_error(
            good.key_for(&Hash::new(b"never staged anywhere")).await,
            "an unnamed ciphertext has no key",
        );
        assert!(
            err.to_string().contains("no cipherBlob facet in document"),
            "{err}"
        );

        // One document carrying the four unreachable shapes, one domain each.
        let broken_doc = stage_content_doc(&drawer).await?;
        let unknown_heads_c = Hash::new(b"pinned to heads that are not there");
        stage_cipherblob(
            &drawer,
            &broken_doc,
            "unknown-heads",
            CipherBlob {
                representation: Representation {
                    digest: multihash_digest(unknown_heads_c),
                    length_octets: 128,
                },
                content_encoding: CONTENT_ENCODING_AES128GCM.to_string(),
                key_ref: key_ref.parse()?,
                key_ref_heads: ChangeHashSet(Arc::from([automerge::ChangeHash([7u8; 32])])),
                encoding_parameters: encoding.to_encoding_parameters(),
            },
        )
        .await?;
        let missing_doc_c = Hash::new(b"keyRef into a document that does not exist");
        stage_cipherblob(
            &drawer,
            &broken_doc,
            "missing-doc",
            CipherBlob {
                representation: Representation {
                    digest: multihash_digest(missing_doc_c),
                    length_octets: 128,
                },
                content_encoding: CONTENT_ENCODING_AES128GCM.to_string(),
                key_ref: "db+facet:///nosuchkeydoc/org.example.daybook.jwk/relay".parse()?,
                key_ref_heads: key_heads.clone(),
                encoding_parameters: encoding.to_encoding_parameters(),
            },
        )
        .await?;
        let bad_scheme_c = Hash::new(b"a scheme this codec does not implement");
        stage_cipherblob(
            &drawer,
            &broken_doc,
            "bad-scheme",
            CipherBlob {
                representation: Representation {
                    digest: multihash_digest(bad_scheme_c),
                    length_octets: 128,
                },
                content_encoding: "br".to_string(),
                key_ref: key_ref.parse()?,
                key_ref_heads: key_heads.clone(),
                encoding_parameters: encoding.to_encoding_parameters(),
            },
        )
        .await?;
        let bad_params_c = Hash::new(b"parameters its own scheme cannot parse");
        stage_cipherblob(
            &drawer,
            &broken_doc,
            "bad-params",
            CipherBlob {
                representation: Representation {
                    digest: multihash_digest(bad_params_c),
                    length_octets: 128,
                },
                content_encoding: CONTENT_ENCODING_AES128GCM.to_string(),
                key_ref: key_ref.parse()?,
                key_ref_heads: key_heads.clone(),
                encoding_parameters: serde_json::json!({ "recordSize": 8, "padding": "record" }),
            },
        )
        .await?;
        let broken =
            DocKeySource::new(Arc::clone(&drawer), broken_doc, BranchPathBuf::from("main"));

        let err = expect_error(
            broken.key_for(&unknown_heads_c).await,
            "heads the key document does not have cannot resolve",
        );
        assert!(
            err.to_string().contains("no readable state for facet"),
            "{err}"
        );
        let err = expect_error(
            broken.key_for(&missing_doc_c).await,
            "a key document that does not exist cannot resolve",
        );
        assert!(err.to_string().contains("nosuchkeydoc"), "{err}");
        let err = broken
            .encoding_for(&bad_scheme_c)
            .await
            .expect_err("an unimplemented content encoding cannot be interpreted");
        assert!(
            err.to_string().contains("unsupported content encoding"),
            "{err}"
        );
        let err = broken
            .encoding_for(&bad_params_c)
            .await
            .expect_err("parameters that violate the scheme are refused");
        assert!(err.to_string().contains("record size 8"), "{err}");

        // The reference shapes the resolver refuses before it reads anything:
        // a keyRef must name another document, the JWK tag, and the heads it
        // meant.
        let reference = |key_ref: &str, heads: ChangeHashSet| CipherBlob {
            representation: Representation {
                digest: "unused-by-this-case".to_string(),
                length_octets: 0,
            },
            content_encoding: CONTENT_ENCODING_AES128GCM.to_string(),
            key_ref: key_ref.parse().expect("fixture url parses"),
            key_ref_heads: heads,
            encoding_parameters: serde_json::json!({}),
        };
        let err = good
            .jwk_at(&reference(
                "db+facet:///self/org.example.daybook.jwk/relay",
                key_heads.clone(),
            ))
            .await
            .expect_err("a key in the serving document is the hazard §6 names");
        assert!(err.to_string().contains("names this document"), "{err}");

        let err = good
            .jwk_at(&reference(&key_ref, ChangeHashSet::default()))
            .await
            .expect_err("a cross-document reference that pinned no heads cannot resolve");
        assert!(err.to_string().contains("pins no heads"), "{err}");

        let wrong_tag = format!("db+facet:///{key_doc_id}/org.example.daybook.blob/main");
        let err = good
            .jwk_at(&reference(&wrong_tag, key_heads))
            .await
            .expect_err("a keyRef at the wrong facet tag is not a key");
        assert!(err.to_string().contains("rather than the JWK tag"), "{err}");

        ctx.stop().await?;
        Ok(())
    }
}
