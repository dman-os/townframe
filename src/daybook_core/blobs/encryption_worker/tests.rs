use super::*;
use crate::blobs::blob_id_to_iroh_hash;
use crate::blobs::encrypt::{TAG_CT_PREFIX, TAG_PT_PREFIX, get_decrypted};
use crate::index::facet_set::DocFacetTagMembership;
use crate::test_support::{DaybookTestContext, test_cx};
use daybook_types::doc::BlobPin;

/// The worker's context, built the way `rt` builds it (the group comes from
/// the authority, the inventory from the repo config).
async fn test_ctx(ctx: &DaybookTestContext, group: Option<BigKeyhiveGroup>) -> Res<Arc<Ctx>> {
    let authority = crate::authority::ensure(&ctx.rt.rcx.big_repo, &ctx.rt.rcx.sql, None).await?;
    let group = match group {
        Some(group) => group,
        None => authority.encrypted_blob_docs.clone(),
    };
    let inventory = ctx.rt.rcx.encryption_inventory_doc_id.clone();
    Ok(Arc::new(Ctx {
        drawer_repo: Arc::clone(&ctx.drawer_repo),
        sql: ctx.rt.rcx.sql.clone(),
        pair_roots: PairRoots::boot(ctx.rt.rcx.sql.clone()).await?,
        store: ctx.rt.blobs_repo.iroh_store(),
        provider: ctx.rt.blobs_repo.cipher_provider(),
        domain_id: domain_facet_id(&group),
        domain_group: group,
        encryption_inventory_doc_id: ctx
            .drawer_repo
            .resolve_doc_id_for_branch_doc_id(inventory)
            .await?,
        faults: Arc::new(Faults::default()),
    }))
}

/// A document with one `Blob` facet for a plaintext this node stores: the
/// shape a photo's document has. The facet key is the production writer's
/// (`FacetKey::from(WellKnownFacetTag::Blob)`, id `DEFAULT_FACET_ID`) and the
/// digest lives in the facet VALUE — the association plane's contract.
async fn stage_document_with_blob(
    ctx: &DaybookTestContext,
    plaintext: BlobId,
) -> Res<(DocId, FacetKey)> {
    let blob_key = FacetKey::from(WellKnownFacetTag::Blob);
    let doc_id = ctx
        .drawer_repo
        .add(AddDocArgs {
            branch_path: BranchPathBuf::from(MAIN_BRANCH),
            facets: [(
                blob_key.clone(),
                FacetRaw::from(WellKnownFacet::Blob(Blob {
                    mime: "application/octet-stream".to_string(),
                    length_octets: 4096,
                    // §3's canonical spelling, which is what a conformant
                    // writer produces.
                    digest: blob_id_to_digest_str(plaintext.clone()),
                    inline: None,
                    urls: Some(vec![format!(
                        "{}:///{}",
                        crate::blobs::BLOB_SCHEME,
                        plaintext
                    )]),
                })),
            )]
            .into(),
            user_path: None,
        })
        .await?;
    Ok((doc_id, blob_key))
}

async fn read_facet(drawer: &DrawerRepo, doc_id: &DocId, key: &FacetKey) -> Res<Option<FacetRaw>> {
    let Some(heads) = drawer
        .get_branch_heads_for_path(doc_id, &BranchPathBuf::from(MAIN_BRANCH))
        .await?
    else {
        return Ok(None);
    };
    let Some(doc) = drawer
        .get_doc_with_facets_at_branch_heads(
            doc_id,
            &BranchPathBuf::from(MAIN_BRANCH),
            &heads,
            Some(vec![key.clone()]),
        )
        .await?
    else {
        return Ok(None);
    };
    Ok(doc.facets.get(key).cloned())
}

async fn read_cipherblob(
    drawer: &DrawerRepo,
    doc_id: &DocId,
    key: &FacetKey,
) -> Res<Option<CipherBlob>> {
    let Some(raw) = read_facet(drawer, doc_id, key).await? else {
        return Ok(None);
    };
    match WellKnownFacet::from_json(raw, WellKnownFacetTag::CipherBlob)? {
        WellKnownFacet::CipherBlob(cipher) => Ok(Some(cipher)),
        other => eyre::bail!("expected a cipherBlob facet, got {:?}", other.tag()),
    }
}

async fn read_blob(drawer: &DrawerRepo, doc_id: &DocId, key: &FacetKey) -> Res<Option<Blob>> {
    let Some(raw) = read_facet(drawer, doc_id, key).await? else {
        return Ok(None);
    };
    match WellKnownFacet::from_json(raw, WellKnownFacetTag::Blob)? {
        WellKnownFacet::Blob(blob) => Ok(Some(blob)),
        other => eyre::bail!("expected a Blob facet, got {:?}", other.tag()),
    }
}

/// The whole sequence, end to end: after one reconcile the document names a
/// representation that a *reader* - resolving the key through the document,
/// exactly as a serving node does - can fetch and decrypt back to the
/// plaintext.
#[tokio::test(flavor = "multi_thread")]
async fn eligible_document_gets_a_servable_representation() -> Res<()> {
    let ctx = test_cx(utils_rs::function_full!()).await?;
    let worker = Worker::new(test_ctx(&ctx, None).await?, None, None);
    let plaintext_bytes = b"blob-encryption worker: eligible".to_vec();
    let plaintext = ctx.rt.blobs_repo.put(&plaintext_bytes).await?;
    let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext.clone()).await?;
    let cipher_key = worker.cipher_facet_key(&blob_key);
    let heads = ctx
        .drawer_repo
        .get_branch_heads_for_path(&doc_id, &BranchPathBuf::from(MAIN_BRANCH))
        .await?
        .expect("document has a branch");

    worker
        .reconcile_document(&doc_id, &BranchPathBuf::from(MAIN_BRANCH), &heads)
        .await?;

    let cipher = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
        .await?
        .expect("the document now names a representation");
    let c = digest_str_to_blob_id_lenient(&cipher.representation.digest)
        .expect("the facet digest is a blob digest");
    assert_eq!(
        cipher.representation.digest,
        blob_id_to_digest_str(c.clone()),
        "a facet digest is the ADR §3 multihash spelling"
    );
    assert_eq!(cipher.content_encoding, CONTENT_ENCODING_AES128GCM);
    assert_eq!(
        cipher.encoding_parameters,
        EncodingParams::DEFAULT.to_encoding_parameters()
    );
    assert!(
        !cipher.key_ref_heads.0.is_empty(),
        "a cross-document keyRef must pin the key state it meant"
    );
    assert_ne!(
        cipher.representation.digest,
        blob_id_to_digest_str(plaintext),
        "the representation must not be the plaintext digest"
    );

    // Servable: fetch C from the store and decrypt it through the document
    // layer, which is what a peer does.
    let c_hash = blob_id_to_iroh_hash(c);
    assert!(
        worker.blob_status(c_hash).await?.is_some(),
        "the representation entry is complete after install"
    );
    let keys = DocKeySource::new(
        Arc::clone(&ctx.drawer_repo),
        doc_id.clone(),
        BranchPathBuf::from(MAIN_BRANCH),
    );
    let decrypted = crate::blobs::encrypt::get_decrypted(&worker.store, &keys, c_hash).await?;
    assert_eq!(decrypted, plaintext_bytes);

    // The commit point, and it is what makes the document *resolve*.
    let blob = read_blob(&ctx.drawer_repo, &doc_id, &blob_key)
        .await?
        .expect("the Blob facet is still there");
    assert!(
        resolves_through(&blob, &cipher_key),
        "the Blob facet must resolve through the representation, got {:?}",
        blob.urls
    );
    ctx.stop().await?;
    Ok(())
}

/// The gate itself: the same document, the same worker, a group the document
/// is not a member of. Nothing is produced.
///
/// Fails on the eligibility check being absent - the positive test above
/// passes either way, so this is the one that pins "eligible" rather than
/// "every document gets encrypted".
#[tokio::test(flavor = "multi_thread")]
async fn document_outside_the_domain_group_is_not_encrypted() -> Res<()> {
    let ctx = test_cx(utils_rs::function_full!()).await?;
    let outsider_group = ctx
        .rt
        .rcx
        .big_repo
        .create_group_with_parents(Vec::new())
        .await?;
    let worker = Worker::new(test_ctx(&ctx, Some(outsider_group)).await?, None, None);
    let plaintext = ctx
        .rt
        .blobs_repo
        .put(b"blob-encryption worker: outsider")
        .await?;
    let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext).await?;
    let cipher_key = worker.cipher_facet_key(&blob_key);
    let heads = ctx
        .drawer_repo
        .get_branch_heads_for_path(&doc_id, &BranchPathBuf::from(MAIN_BRANCH))
        .await?
        .expect("document has a branch");

    worker
        .reconcile_document(&doc_id, &BranchPathBuf::from(MAIN_BRANCH), &heads)
        .await?;

    assert!(
        read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
            .await?
            .is_none(),
        "a document outside the domain group must not get a representation"
    );
    let blob = read_blob(&ctx.drawer_repo, &doc_id, &blob_key)
        .await?
        .expect("the Blob facet is untouched");
    assert!(
        !resolves_through(&blob, &cipher_key),
        "nothing may point at a representation that was not produced"
    );
    ctx.stop().await?;
    Ok(())
}

/// A second pass reproduces the same representation instead of minting a new
/// key: the salt is a pure function of (key, plaintext), so the only way to
/// stay stable is to reuse the facet's own key.
#[tokio::test(flavor = "multi_thread")]
async fn second_pass_reuses_the_representation() -> Res<()> {
    let ctx = test_cx(utils_rs::function_full!()).await?;
    let worker = Worker::new(test_ctx(&ctx, None).await?, None, None);
    let plaintext = ctx
        .rt
        .blobs_repo
        .put(b"blob-encryption worker: idempotent")
        .await?;
    let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext).await?;
    let cipher_key = worker.cipher_facet_key(&blob_key);
    let branch = BranchPathBuf::from(MAIN_BRANCH);
    let heads = ctx
        .drawer_repo
        .get_branch_heads_for_path(&doc_id, &branch)
        .await?
        .expect("document has a branch");

    worker.reconcile_document(&doc_id, &branch, &heads).await?;
    let first = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
        .await?
        .expect("first pass installs a representation");
    let heads = ctx
        .drawer_repo
        .get_branch_heads_for_path(&doc_id, &branch)
        .await?
        .expect("document has a branch");
    worker.reconcile_document(&doc_id, &branch, &heads).await?;

    let second = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
        .await?
        .expect("second pass keeps a representation");
    assert_eq!(
        first.representation.digest, second.representation.digest,
        "a re-run must not mint a second representation"
    );
    let blob = read_blob(&ctx.drawer_repo, &doc_id, &blob_key)
        .await?
        .expect("the Blob facet is still there");
    let via_count = blob
        .urls
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|url| url.contains("?via="))
        .count();
    assert_eq!(
        via_count, 1,
        "the commit point is written once, got {:?}",
        blob.urls
    );
    ctx.stop().await?;
    Ok(())
}

/// The inventory write the pin worker's `apply_inventory_diff` performs:
/// one BlobPin facet per ciphertext, on the inventory doc's `main` branch
/// through the plain (user-scoped) facet write. Tests drive the same
/// production write when they stand in for the pin diff.
async fn write_inventory_pin(
    drawer: &DrawerRepo,
    inventory_doc_id: &DocId,
    digest: &str,
    pin: Option<BlobPin>,
) -> Res<()> {
    let key = FacetKey {
        tag: WellKnownFacetTag::BlobPin.into(),
        id: digest.to_string(),
    };
    let (facets_set, facets_remove) = match pin {
        Some(pin) => (
            [(key.clone(), FacetRaw::from(WellKnownFacet::BlobPin(pin)))].into(),
            vec![],
        ),
        None => (std::collections::HashMap::new(), vec![key]),
    };
    drawer
        .update_at_heads(
            DocPatch {
                id: inventory_doc_id.clone(),
                user_path: None,
                facets_set,
                facets_remove,
            },
            BranchPath::new(MAIN_BRANCH),
            None,
        )
        .await?;
    Ok(())
}

/// A released representation is not dead: the facet that named it is gone,
/// its inventory pin is gone, and its store roots are gone - but the
/// plaintext is still local, so the next pass re-authorizes it and the
/// document may declare the blob again. The re-install mints a fresh
/// random key (the old pair was released; nothing may reuse released key
/// material), so the new representation names a different digest, while
/// the same document-facing cipherBlob facet id carries it and the
/// released pair's roots stay released.
#[tokio::test(flavor = "multi_thread")]
async fn released_representation_is_reinstalled_with_a_fresh_key_by_the_next_pass() -> Res<()> {
    let ctx = test_cx(utils_rs::function_full!()).await?;
    let worker = Worker::new(test_ctx(&ctx, None).await?, None, None);
    let plaintext_bytes = b"blob-encryption worker: released then redeclared".to_vec();
    let plaintext = ctx.rt.blobs_repo.put(&plaintext_bytes).await?;
    let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext.clone()).await?;
    let cipher_key = worker.cipher_facet_key(&blob_key);
    let branch = BranchPathBuf::from(MAIN_BRANCH);
    let heads = ctx
        .drawer_repo
        .get_branch_heads_for_path(&doc_id, &branch)
        .await?
        .expect("document has a branch");

    worker.reconcile_document(&doc_id, &branch, &heads).await?;
    let first = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
        .await?
        .expect("first pass installs a representation");
    let c1 = digest_str_to_blob_id_lenient(&first.representation.digest)
        .expect("the facet digest is a blob digest");
    let c1_hash = blob_id_to_iroh_hash(c1);
    let p_hash = blob_id_to_iroh_hash(plaintext.clone());
    assert_eq!(
        worker
            .store
            .tags()
            .get(format!("{TAG_CT_PREFIX}{c1_hash}"))
            .await?
            .expect("a registered pair roots its ciphertext")
            .hash,
        c1_hash,
    );
    // The pin worker saw the facet and pinned it (the write shape above).
    write_inventory_pin(
        &ctx.drawer_repo,
        &worker.encryption_inventory_doc_id,
        &first.representation.digest,
        Some(BlobPin {
            length_octets: first.representation.length_octets,
        }),
    )
    .await?;

    // The release, production-shaped end to end: the facet is re-authored
    // away (system-managed, so the writer's scope applies), the inventory
    // pin leaves, and the pin worker's release leaf drops the pair roots.
    ctx.drawer_repo
        .update_at_heads_with_scope(
            DocPatch {
                id: doc_id.clone(),
                user_path: None,
                facets_set: std::collections::HashMap::new(),
                facets_remove: vec![cipher_key.clone()],
            },
            &branch,
            None,
            FacetWriteScope::System,
        )
        .await?;
    write_inventory_pin(
        &ctx.drawer_repo,
        &worker.encryption_inventory_doc_id,
        &first.representation.digest,
        None,
    )
    .await?;
    crate::blobs::encrypt::drop_pair_tags(&worker.store, c1_hash).await?;
    assert!(
        read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
            .await?
            .is_none(),
        "the released facet is gone"
    );
    assert!(
        worker
            .store
            .tags()
            .get(format!("{TAG_CT_PREFIX}{c1_hash}"))
            .await?
            .is_none(),
        "the released pair is un-rooted"
    );

    // The same mechanics cover the re-install: the Blob facet is still on
    // the document, so the keyed task finds a declared blob with no
    // representation and creates one; re-declaring is not a worker input.
    let heads = ctx
        .drawer_repo
        .get_branch_heads_for_path(&doc_id, &branch)
        .await?
        .expect("document has a branch");
    worker.reconcile_document(&doc_id, &branch, &heads).await?;

    let second = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
        .await?
        .expect("the re-derivation re-installs the declared blob's representation");
    assert_ne!(
        second.representation.digest, first.representation.digest,
        "a re-install after a release mints fresh key material; the old key is released"
    );
    let c2 = digest_str_to_blob_id_lenient(&second.representation.digest)
        .expect("the facet digest is a blob digest");
    let c2_hash = blob_id_to_iroh_hash(c2);
    let ct_tag = worker
        .store
        .tags()
        .get(format!("{TAG_CT_PREFIX}{c2_hash}"))
        .await?
        .expect("the re-installed pair is rooted");
    assert_eq!(ct_tag.hash, c2_hash);
    let pt_tag = worker
        .store
        .tags()
        .get(format!("{TAG_PT_PREFIX}{c2_hash}"))
        .await?
        .expect("the re-installed pair roots its plaintext");
    assert_eq!(pt_tag.hash, p_hash);
    assert!(
        worker
            .store
            .tags()
            .get(format!("{TAG_CT_PREFIX}{c1_hash}"))
            .await?
            .is_none(),
        "the released pair's roots stay released across the re-install"
    );
    // A reader resolving through the document decrypts the new
    // representation back to the plaintext.
    let keys = DocKeySource::new(
        Arc::clone(&ctx.drawer_repo),
        doc_id.clone(),
        BranchPathBuf::from(MAIN_BRANCH),
    );
    let decrypted = crate::blobs::encrypt::get_decrypted(&worker.store, &keys, c2_hash).await?;
    assert_eq!(decrypted, plaintext_bytes);
    let blob = read_blob(&ctx.drawer_repo, &doc_id, &blob_key)
        .await?
        .expect("the Blob facet is still there");
    assert!(
        resolves_through(&blob, &cipher_key),
        "the re-installed representation is what the document resolves through, got {:?}",
        blob.urls
    );
    ctx.stop().await?;
    Ok(())
}

/// A stale inventory pin cannot authorize a re-install: the pin worker's
/// row may outlive its facet by one diff, but the pass re-produces a
/// representation only for a declared blob whose plaintext this node
/// stores (§14). Released, with the plaintext gone, the pass must leave
/// every plane exactly as it found it - no facet, no re-rooted pair, and
/// no pin bookkeeping on its own.
#[tokio::test(flavor = "multi_thread")]
async fn released_inventory_pin_with_absent_plaintext_is_not_resurrected_by_the_pass() -> Res<()> {
    let ctx = test_cx(utils_rs::function_full!()).await?;
    let worker = Worker::new(test_ctx(&ctx, None).await?, None, None);
    let plaintext = ctx
        .rt
        .blobs_repo
        .put(b"blob-encryption worker: released without a local plaintext")
        .await?;
    let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext.clone()).await?;
    let cipher_key = worker.cipher_facet_key(&blob_key);
    let branch = BranchPathBuf::from(MAIN_BRANCH);

    worker
        .reconcile_document(&doc_id, &branch, &worker_heads(&ctx, &doc_id).await?)
        .await?;
    let first = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
        .await?
        .expect("first pass installs a representation");
    let c1 = digest_str_to_blob_id_lenient(&first.representation.digest)
        .expect("the facet digest is a blob digest");
    let c1_hash = blob_id_to_iroh_hash(c1.clone());
    let urls_before = read_blob(&ctx.drawer_repo, &doc_id, &blob_key)
        .await?
        .expect("the Blob facet is there")
        .urls;
    // The released state, production-shaped: facet re-authored away, pair
    // roots dropped, and the stale pin that outlives the diff still in the
    // inventory - the window the release contract must not leak through.
    write_inventory_pin(
        &ctx.drawer_repo,
        &worker.encryption_inventory_doc_id,
        &first.representation.digest,
        Some(BlobPin {
            length_octets: first.representation.length_octets,
        }),
    )
    .await?;
    ctx.drawer_repo
        .update_at_heads_with_scope(
            DocPatch {
                id: doc_id.clone(),
                user_path: None,
                facets_set: std::collections::HashMap::new(),
                facets_remove: vec![cipher_key.clone()],
            },
            &branch,
            None,
            FacetWriteScope::System,
        )
        .await?;
    write_inventory_pin(
        &ctx.drawer_repo,
        &worker.encryption_inventory_doc_id,
        &first.representation.digest,
        None,
    )
    .await?;
    crate::blobs::encrypt::drop_pair_tags(&worker.store, c1_hash).await?;
    // Now the plaintext is gone too: a fresh store with nothing in it is
    // the state every released blob ends in once GC has run.
    let fresh = tempfile::tempdir()?;
    let fresh_repo = crate::blobs::BlobsRepo::new(
        fresh.path().join("blobs"),
        daybook_types::doc::UserPathBuf::from("/test-user"),
    )
    .await?;
    let bare = Worker::new(
        Arc::new(Ctx {
            drawer_repo: Arc::clone(&ctx.drawer_repo),
            sql: ctx.rt.rcx.sql.clone(),
            pair_roots: PairRoots::boot(ctx.rt.rcx.sql.clone()).await?,
            store: fresh_repo.iroh_store(),
            provider: fresh_repo.cipher_provider(),
            domain_id: worker.domain_id.clone(),
            domain_group: worker.domain_group.clone(),
            encryption_inventory_doc_id: worker.encryption_inventory_doc_id.clone(),
            faults: Arc::new(Faults::default()),
        }),
        None,
        None,
    );

    bare.reconcile_document(&doc_id, &branch, &worker_heads(&ctx, &doc_id).await?)
        .await?;

    assert!(
        read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
            .await?
            .is_none(),
        "a stale pin row must not produce a representation"
    );
    assert!(
        bare.blob_status(c1_hash).await?.is_none(),
        "the re-derivation must not have written the released ciphertext into the bare store"
    );
    assert!(
        bare.store
            .tags()
            .get(format!("{TAG_CT_PREFIX}{c1_hash}"))
            .await?
            .is_none()
            && bare
                .store
                .tags()
                .get(format!("{TAG_PT_PREFIX}{c1_hash}"))
                .await?
                .is_none(),
        "the re-derivation must not have re-rooted the released pair in the bare store"
    );
    let pin_key = FacetKey {
        tag: WellKnownFacetTag::BlobPin.into(),
        id: first.representation.digest.clone(),
    };
    assert!(
        read_facet(
            &ctx.drawer_repo,
            &worker.encryption_inventory_doc_id,
            &pin_key
        )
        .await?
        .is_none(),
        "the pass owns no pin writes; the stale pin was released above"
    );
    assert_eq!(
        read_blob(&ctx.drawer_repo, &doc_id, &blob_key)
            .await?
            .expect("the Blob facet is still there")
            .urls,
        urls_before,
        "a skipped document is written to nowhere"
    );
    fresh_repo.shutdown().await?;
    ctx.stop().await?;
    Ok(())
}

async fn worker_heads(ctx: &DaybookTestContext, doc_id: &DocId) -> Res<ChangeHashSet> {
    ctx.drawer_repo
        .get_branch_heads_for_path(doc_id, &BranchPathBuf::from(MAIN_BRANCH))
        .await?
        .ok_or_else(|| eyre::eyre!("document {doc_id} has no {MAIN_BRANCH} branch"))
}
/// The facet index has the document listed for the `Blob` tag: the
/// precondition every consumer of the facet stream works from.
async fn await_indexed(ctx: &DaybookTestContext, doc_id: &DocId) -> Res<()> {
    let facet_index = &ctx.rt.doc_facet_set_index_repo;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if facet_index
            .list_docs_for_tag(WellKnownFacetTag::Blob.as_str())
            .await?
            .iter()
            .any(|membership: &DocFacetTagMembership| membership.doc_id == *doc_id)
        {
            return Ok(());
        }
        eyre::ensure!(
            std::time::Instant::now() < deadline,
            "facet index never listed {doc_id} as a Blob-facet document"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// The facet index has the document's value-digest projection row: what the
/// presence line resolves a blob arrival with (`list_docs_for_blob_digest`).
async fn await_digest_indexed(
    ctx: &DaybookTestContext,
    doc_id: &DocId,
    plaintext: &BlobId,
) -> Res<()> {
    let facet_index = &ctx.rt.doc_facet_set_index_repo;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if facet_index
            .list_docs_for_blob_digest(plaintext)
            .await?
            .iter()
            .any(|membership: &DocFacetTagMembership| membership.doc_id == *doc_id)
        {
            return Ok(());
        }
        eyre::ensure!(
            std::time::Instant::now() < deadline,
            "facet index never projected {plaintext} for {doc_id}'s Blob facet value"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// The keyed task's representation became durable: `read_cipherblob`
/// returns it. Named callers pass a label for the deadline error only.
async fn await_representation(
    ctx: &DaybookTestContext,
    doc_id: &DocId,
    cipher_key: &FacetKey,
    label: &str,
) -> Res<CipherBlob> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if let Some(cipher) = read_cipherblob(&ctx.drawer_repo, doc_id, cipher_key).await? {
            return Ok(cipher);
        }
        eyre::ensure!(
            std::time::Instant::now() < deadline,
            "{label}: the representation never became durable"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// A worker started after a document already exists: the walker replays
/// the facet-set revisions from its (fresh) state position, so the
/// pre-existing document's representation is the replay's own work. The
/// returned handle stays live for the delta tests built on it.
async fn spawn_worker_after_pass_barrier(
    ctx: &DaybookTestContext,
    worker_ctx: &Arc<Ctx>,
) -> Res<(tokio::task::JoinHandle<Res<()>>, CancellationToken)> {
    let before = ctx
        .rt
        .blobs_repo
        .put(b"walker replay: before the worker")
        .await?;
    let (doc_id, blob_key) = stage_document_with_blob(ctx, before).await?;
    await_indexed(ctx, &doc_id).await?;
    let mut worker = Worker::new(Arc::clone(worker_ctx), None, None);
    let cancel_token = CancellationToken::new();
    let facet_index = Arc::clone(&ctx.rt.doc_facet_set_index_repo);
    let handle = tokio::spawn({
        let cancel_token = cancel_token.clone();
        let revision_store = facet_index.revision_store();
        async move { worker.run(revision_store, cancel_token).await }
    });
    await_representation(
        ctx,
        &doc_id,
        &worker_ctx.cipher_facet_key(&blob_key),
        "the replay",
    )
    .await?;
    Ok((handle, cancel_token))
}

/// A document whose blob is not local yet is *skipped* by the delta path —
/// and re-armed by the presence stream the moment the bytes land. No
/// writer here touches the document again: the re-arm can only come from
/// the `/blobs` membership event the put publishes.
#[tokio::test(flavor = "multi_thread")]
async fn blob_arriving_later_is_rearmed_by_the_presence_stream() -> Res<()> {
    let ctx = test_cx(utils_rs::function_full!()).await?;
    let worker_ctx = test_ctx(&ctx, None).await?;
    // The facet names a digest whose bytes do not exist yet: the stage
    // helper writes the facet for whatever `BlobId` it is handed.
    let payload = b"presence stream: the bytes arrive second".to_vec();
    let plaintext = BlobId::new(*blake3::hash(&payload).as_bytes());
    let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext.clone()).await?;
    let cipher_key = worker_ctx.cipher_facet_key(&blob_key);

    // The worker (feeder-less machine) sees the facet delta and skips:
    // plaintext not local.
    let worker = Worker::new(Arc::clone(&worker_ctx), None, None);
    let skipped_token = CancellationToken::new();
    let handle = {
        let facet_index = Arc::clone(&ctx.rt.doc_facet_set_index_repo);
        let revision_store = facet_index.revision_store();
        let cancel_token = skipped_token.clone();
        let mut worker = worker;
        tokio::spawn(async move { worker.run(revision_store, cancel_token).await })
    };
    await_indexed(&ctx, &doc_id).await?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        eyre::ensure!(
            std::time::Instant::now() < deadline,
            "the delta path never considered the document"
        );
        if worker_ctx.faults.attempts() > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
            .await?
            .is_none(),
        "a plaintext this node does not hold must not produce a representation"
    );

    // Now the real trigger: a full worker with the feeder + the presence
    // stream, and the bytes landing. Everything from here on is the
    // production spawn; the earlier machine keeps running and cannot be
    // what produces the representation (its trigger channel is None).
    let authority = crate::authority::ensure(&ctx.rt.rcx.big_repo, &ctx.rt.rcx.sql, None).await?;
    let (trigger_tx, trigger_rx) = tokio::sync::mpsc::channel(64);
    let _feeder = spawn_encryption_trigger_feeder(EncryptionFeederArgs {
        repo_part_store: Arc::clone(&ctx.rt.rcx.part_store),
        eligibility_part: authority.encrypted_blob_docs_part_id(),
        presence_store: Arc::clone(&ctx.rt.rcx.blob_presence_store),
        facet_index: Arc::clone(&ctx.rt.doc_facet_set_index_repo),
        sql: ctx.rt.rcx.sql.clone(),
        trigger_tx,
        faults: Arc::clone(&worker_ctx.faults),
    })
    .await?;
    let live_worker = Worker::new(Arc::clone(&worker_ctx), None, Some(trigger_rx));
    let live_token = CancellationToken::new();
    let live_handle = {
        let facet_index = Arc::clone(&ctx.rt.doc_facet_set_index_repo);
        let revision_store = facet_index.revision_store();
        let cancel_token = live_token.clone();
        let mut live_worker = live_worker;
        tokio::spawn(async move { live_worker.run(revision_store, cancel_token).await })
    };
    ctx.rt
        .blobs_repo
        .set_blob_presence_sink(Arc::clone(&ctx.rt.rcx.blob_presence_store));
    // The put is the membership-with-the-fact write: this is where the
    // trigger plane learns the blob exists.
    let put_back = ctx.rt.blobs_repo.put(&payload).await?;
    assert_eq!(put_back, plaintext, "the facet named these bytes all along");

    await_representation(&ctx, &doc_id, &cipher_key, "the presence re-arm").await?;
    skipped_token.cancel();
    live_token.cancel();
    handle.await??;
    live_handle.await??;
    ctx.stop().await?;
    Ok(())
}

/// The other half of the presence stream's contract, with the production
/// facet shape (facet id `DEFAULT_FACET_ID`, digest in the facet VALUE): the
/// document is *ineligible* when the blob's bytes land. The arrival must
/// still resolve the document — by the value-digest projection, the only
/// association key production facets carry — run its task (the skip), and
/// leave the re-arm to the later eligibility join. Nothing else mentions the
/// document between the arrival and the join.
#[tokio::test(flavor = "multi_thread")]
async fn blob_arriving_while_ineligible_rearms_the_presence_plane() -> Res<()> {
    let ctx = test_cx(utils_rs::function_full!()).await?;
    let worker_ctx = test_ctx(&ctx, None).await?;
    let authority = crate::authority::ensure(&ctx.rt.rcx.big_repo, &ctx.rt.rcx.sql, None).await?;

    // The facet names a digest whose bytes do not exist yet, and the document
    // leaves the eligibility group the way the tombstone path revokes it,
    // before any trigger plane exists to see the revoke.
    let payload = b"presence stream: the ineligible arrival".to_vec();
    let plaintext = BlobId::new(*blake3::hash(&payload).as_bytes());
    let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext.clone()).await?;
    let cipher_key = worker_ctx.cipher_facet_key(&blob_key);
    let branch_doc_id = physical_doc_id(&doc_id)?;
    ctx.rt
        .rcx
        .big_repo
        .revoke_doc_access(branch_doc_id.clone(), authority.encrypted_blob_docs.clone())
        .await?;

    // Production spawn: both feeder lines + the trigger-fed machine.
    let (trigger_tx, trigger_rx) = tokio::sync::mpsc::channel(64);
    let _feeder = spawn_encryption_trigger_feeder(EncryptionFeederArgs {
        repo_part_store: Arc::clone(&ctx.rt.rcx.part_store),
        eligibility_part: authority.encrypted_blob_docs_part_id(),
        presence_store: Arc::clone(&ctx.rt.rcx.blob_presence_store),
        facet_index: Arc::clone(&ctx.rt.doc_facet_set_index_repo),
        sql: ctx.rt.rcx.sql.clone(),
        trigger_tx,
        faults: Arc::clone(&worker_ctx.faults),
    })
    .await?;
    let live_worker = Worker::new(Arc::clone(&worker_ctx), None, Some(trigger_rx));
    let live_token = CancellationToken::new();
    let live_handle = {
        let facet_index = Arc::clone(&ctx.rt.doc_facet_set_index_repo);
        let revision_store = facet_index.revision_store();
        let cancel_token = live_token.clone();
        let mut live_worker = live_worker;
        tokio::spawn(async move { live_worker.run(revision_store, cancel_token).await })
    };

    // The replayed eligibility history runs the document's task and the skip
    // on eligibility is pinned before the arrival can add a trigger of its
    // own.
    let settled = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        eyre::ensure!(
            std::time::Instant::now() < settled,
            "the replayed eligibility history never considered the document"
        );
        if worker_ctx.faults.attempts() > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
            .await?
            .is_none(),
        "an ineligible document must not get a representation from the replay"
    );

    // The digest projection the presence line resolves arrivals with. Both it
    // and the facet-tag rows land in one projection revision, so the row's
    // appearance is what makes a subsequent arrival able to resolve the doc.
    await_digest_indexed(&ctx, &doc_id, &plaintext).await?;

    // The arrival while still ineligible. The resolves counter moves only
    // when the presence line actually resolved the digest to the document,
    // which is what old-by-facet-id lookup could never do here.
    let resolved_baseline = worker_ctx.faults.presence_resolves();
    ctx.rt
        .blobs_repo
        .set_blob_presence_sink(Arc::clone(&ctx.rt.rcx.blob_presence_store));
    let put_back = ctx.rt.blobs_repo.put(&payload).await?;
    assert_eq!(put_back, plaintext, "the facet named these bytes all along");
    loop {
        eyre::ensure!(
            std::time::Instant::now() < settled,
            "the arrival never resolved to the document through the value digest"
        );
        if worker_ctx.faults.presence_resolves() > resolved_baseline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
            .await?
            .is_none(),
        "the arrival's task must still skip on eligibility"
    );

    // The re-arm: the join flips eligibility live and lands the group's
    // part-store membership row, the event the eligibility tail tails.
    ctx.rt
        .rcx
        .big_repo
        .add_admin_member_to_doc(branch_doc_id, authority.encrypted_blob_docs.clone())
        .await?;
    await_representation(
        &ctx,
        &doc_id,
        &cipher_key,
        "the eligibility re-arm after the arrival",
    )
    .await?;
    live_token.cancel();
    live_handle.await??;
    ctx.stop().await?;
    Ok(())
}

/// The other ordering of the stream contract: the plaintext is local while
/// the document is *not* eligible, and the document joins the eligibility
/// group afterwards. No writer touches the document again — the presence
/// line's triggers must skip on eligibility, and the join's membership
/// event on the group part is what finishes the work.
///
/// The skip before the join is pinned with the fault-visit counter: the
/// presence arrival provably ran the keyed task, and the task provably
/// produced nothing.
#[tokio::test(flavor = "multi_thread")]
async fn doc_joining_the_group_later_rearms_the_eligibility_stream() -> Res<()> {
    let ctx = test_cx(utils_rs::function_full!()).await?;
    let worker_ctx = test_ctx(&ctx, None).await?;
    let authority = crate::authority::ensure(&ctx.rt.rcx.big_repo, &ctx.rt.rcx.sql, None).await?;

    // The plaintext is local before the worker exists, and the document
    // declares it — both halves of the final state are already there.
    let payload = b"eligibility stream: bytes first, membership later".to_vec();
    let plaintext = ctx.rt.blobs_repo.put(&payload).await?;
    let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext.clone()).await?;
    let cipher_key = worker_ctx.cipher_facet_key(&blob_key);
    let branch_doc_id = physical_doc_id(&doc_id)?;

    // The document leaves the eligibility group the way the tombstone path
    // revokes it. Fresh feeder cursors replay the membership history —
    // allocation's add and this revocation — and every replayed trigger
    // must find the document ineligible.
    ctx.rt
        .rcx
        .big_repo
        .revoke_doc_access(branch_doc_id.clone(), authority.encrypted_blob_docs.clone())
        .await?;

    // Production spawn: both feeder lines + the trigger-fed machine.
    let (trigger_tx, trigger_rx) = tokio::sync::mpsc::channel(64);
    let _feeder = spawn_encryption_trigger_feeder(EncryptionFeederArgs {
        repo_part_store: Arc::clone(&ctx.rt.rcx.part_store),
        eligibility_part: authority.encrypted_blob_docs_part_id(),
        presence_store: Arc::clone(&ctx.rt.rcx.blob_presence_store),
        facet_index: Arc::clone(&ctx.rt.doc_facet_set_index_repo),
        sql: ctx.rt.rcx.sql.clone(),
        trigger_tx,
        faults: Arc::clone(&worker_ctx.faults),
    })
    .await?;
    let worker = Worker::new(Arc::clone(&worker_ctx), None, Some(trigger_rx));
    let live_token = CancellationToken::new();
    let live_handle = {
        let facet_index = Arc::clone(&ctx.rt.doc_facet_set_index_repo);
        let revision_store = facet_index.revision_store();
        let cancel_token = live_token.clone();
        let mut worker = worker;
        tokio::spawn(async move { worker.run(revision_store, cancel_token).await })
    };

    // Wait until the trigger plane has run the document's keyed task at
    // least once, then pin the skip: eligibility gates the work even with
    // the plaintext in hand.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        eyre::ensure!(
            std::time::Instant::now() < deadline,
            "the trigger plane never considered the document"
        );
        if worker_ctx.faults.attempts() > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
            .await?
            .is_none(),
        "an ineligible document must not get a representation, plaintext or not"
    );

    // The re-arm: the join is the same grant the authority migration
    // backfills. The grant flips `doc_is_eligible` live and lands the
    // group's part-store membership row — the event the eligibility tail
    // tails. Nothing else mentions this document.
    ctx.rt
        .rcx
        .big_repo
        .add_admin_member_to_doc(branch_doc_id.clone(), authority.encrypted_blob_docs.clone())
        .await?;

    await_representation(&ctx, &doc_id, &cipher_key, "the eligibility re-arm").await?;
    live_token.cancel();
    live_handle.await??;
    ctx.stop().await?;
    Ok(())
}
/// A document whose facet delta lands after the worker started: this
/// representation can only come from the `ConcurrentDeltaWalker` over the
/// facet-set revisions — the walker replays the revisions behind its
/// position for older documents, and this one's delta arrives live.
#[tokio::test(flavor = "multi_thread")]
async fn document_written_after_worker_start_is_encrypted_by_the_delta_machine() -> Res<()> {
    let ctx = test_cx(utils_rs::function_full!()).await?;
    let worker_ctx = test_ctx(&ctx, None).await?;
    let (handle, cancel_token) = spawn_worker_after_pass_barrier(&ctx, &worker_ctx).await?;

    let after = ctx
        .rt
        .blobs_repo
        .put(b"delta machine: after the worker started")
        .await?;
    let (doc_id, blob_key) = stage_document_with_blob(&ctx, after).await?;
    let cipher_key = worker_ctx.cipher_facet_key(&blob_key);
    let cipher =
        await_representation(&ctx, &doc_id, &cipher_key, "the facet delta machine").await?;

    // §19: the facet names a ciphertext this node holds (step 1's
    // installation), and the Blob facet resolves through it (step 5).
    let c = digest_str_to_blob_id_lenient(&cipher.representation.digest)
        .ok_or_else(|| eyre::eyre!("representation digest is not a blob id"))?;
    assert!(
        worker_ctx
            .blob_status(crate::blobs::blob_id_to_iroh_hash(c))
            .await?
            .is_some(),
        "the representation the machine wrote must be servable"
    );
    // Step 5 is a write of its own, after the facet: wait for the document
    // to resolve through the representation rather than racing that write.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let blob = read_blob(&ctx.drawer_repo, &doc_id, &blob_key)
            .await?
            .expect("the Blob facet is still there");
        if resolves_through(&blob, &cipher_key) {
            break;
        }
        eyre::ensure!(
            std::time::Instant::now() < deadline,
            "the Blob facet never resolved through the representation"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    cancel_token.cancel();
    handle.await??;
    ctx.stop().await?;
    Ok(())
}

/// A delta with nothing to represent produces nothing and does not stall the
/// machine - the documents behind it are still processed.
#[tokio::test(flavor = "multi_thread")]
async fn delta_with_nothing_to_represent_does_not_stall_the_machine() -> Res<()> {
    let ctx = test_cx(utils_rs::function_full!()).await?;
    let worker_ctx = test_ctx(&ctx, None).await?;
    let (handle, cancel_token) = spawn_worker_after_pass_barrier(&ctx, &worker_ctx).await?;

    // A Blob facet that names no plaintext: a peer-authored revision this
    // node cannot act on. Nothing gets stored and no pin is derivable, so
    // this is the delta the machine must skip *without* losing its place.
    let nameless_key = FacetKey {
        tag: WellKnownFacetTag::Blob.into(),
        id: "nameless".to_string(),
    };
    let nameless_doc = ctx
        .drawer_repo
        .add(AddDocArgs {
            branch_path: BranchPathBuf::from(MAIN_BRANCH),
            facets: [(
                nameless_key.clone(),
                FacetRaw::from(WellKnownFacet::Blob(Blob {
                    mime: "application/octet-stream".to_string(),
                    length_octets: 4096,
                    digest: "not-a-digest".to_string(),
                    inline: None,
                    urls: None,
                })),
            )]
            .into(),
            user_path: None,
        })
        .await?;

    // The document behind it: if the machine stalled on the skipped delta,
    // this representation never appears.
    let stored = ctx
        .rt
        .blobs_repo
        .put(b"delta machine: after the skip")
        .await?;
    let (doc_id, blob_key) = stage_document_with_blob(&ctx, stored).await?;
    await_representation(
        &ctx,
        &doc_id,
        &worker_ctx.cipher_facet_key(&blob_key),
        "the machine, after a delta with nothing to represent",
    )
    .await?;
    assert!(
        read_cipherblob(
            &ctx.drawer_repo,
            &nameless_doc,
            &worker_ctx.cipher_facet_key(&nameless_key),
        )
        .await?
        .is_none(),
        "a Blob facet that names no plaintext must not get a representation"
    );

    cancel_token.cancel();
    handle.await??;
    ctx.stop().await?;
    Ok(())
}

/// A document delta whose work fails is not waved through: the walker cursor
/// must not advance past it, and the document must not be lost - it is
/// rescheduled, and the representation appears once the cause clears. No
/// write to the document happens in between, so the rescheduled task is the
/// only thing that can produce it.
#[tokio::test(flavor = "multi_thread")]
async fn failed_document_delta_is_rescheduled_until_its_work_is_durable() -> Res<()> {
    let ctx = test_cx(utils_rs::function_full!()).await?;
    let worker_ctx = test_ctx(&ctx, None).await?;
    let (handle, cancel_token) = spawn_worker_after_pass_barrier(&ctx, &worker_ctx).await?;

    // Armed before the document exists: the delta this document produces is
    // the one that fails. Nothing below writes to the document again, so a
    // representation can only come from the rescheduled task.
    // This machine's handle: the fault lives on the worker's own state, not
    // in the process, so no other concurrently-running machine sees it.
    worker_ctx
        .faults
        .fail_reconcile
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let stored = ctx
        .rt
        .blobs_repo
        .put(b"delta machine: failure then retry")
        .await?;
    let (doc_id, blob_key) = stage_document_with_blob(&ctx, stored).await?;
    let cipher_key = worker_ctx.cipher_facet_key(&blob_key);

    // While the work keeps failing nothing durable may exist, and the delta
    // must come back rather than being waved through. The retry delay is 2s,
    // so 3.5s has to contain more than one attempt.
    tokio::time::sleep(std::time::Duration::from_millis(3500)).await;
    assert!(
        read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
            .await?
            .is_none(),
        "nothing durable may exist for a document whose work failed"
    );
    assert!(
        worker_ctx.faults.attempts() >= 2,
        "a failed delta must be rescheduled, saw {} attempt(s)",
        worker_ctx.faults.attempts()
    );

    // Clearing the cause is what lets the retry succeed.
    worker_ctx
        .faults
        .fail_reconcile
        .store(false, std::sync::atomic::Ordering::SeqCst);
    await_representation(&ctx, &doc_id, &cipher_key, "the rescheduled delta").await?;

    cancel_token.cancel();
    handle.await??;
    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn fault_state_is_per_machine_not_process_wide() -> Res<()> {
    // The previous process-global statics would let machine 2 fire from
    // machine 1's arming; per-machine handles hold the state on each Ctx.
    let ctx = test_cx(utils_rs::function_full!()).await?;
    let machine_1 = test_ctx(&ctx, None).await?;
    let machine_2 = test_ctx(&ctx, None).await?;

    use std::sync::atomic::Ordering::SeqCst;
    machine_1.faults.fail_reconcile.store(true, SeqCst);
    machine_1.faults.fail_rotate.store(true, SeqCst);

    // Machine 1's armed seam fires on its own handle; its attempts move.
    assert!(machine_1.faults.fail_next_reconcile());
    assert_eq!(machine_1.faults.attempts(), 1);

    // Machine 2's seams are its own state: machine 1's arming must not reach
    // it, and counting must not cross in either direction.
    assert!(!machine_2.faults.fail_next_reconcile());
    assert_eq!(machine_1.faults.attempts(), 1);
    assert_eq!(machine_2.faults.attempts(), 1);

    let machine_2_rotate_fired = machine_2.faults.fail_next_rotate(false);
    assert!(
        !machine_2_rotate_fired,
        "machine 1's armed rotation seam must not reach machine 2"
    );
    assert_eq!(
        machine_1.faults.attempts(),
        1,
        "machine 2's rotation call stays off machine 1's counter"
    );

    Ok(())
}

// ---- §15 rotation ----

/// Spawn the pin worker the way the runtime does, so the facet-delta-driven
/// inventory diff — and the release leaf that drops a departing pair's
/// roots — runs while the test rotates.
async fn spawn_pin_worker_for_test(ctx: &DaybookTestContext) -> Res<crate::repos::RepoStopToken> {
    crate::blobs::spawn_blob_pin_worker(crate::blobs::BlobPinWorkerArgs {
        drawer_repo: Arc::clone(&ctx.rt.drawer),
        sql: ctx.rt.rcx.sql.clone(),
        core_inventory_doc_id: ctx.rt.rcx.core_inventory_doc_id.clone(),
        docs_inventory_doc_id: ctx.rt.rcx.docs_inventory_doc_id.clone(),
        encryption_inventory_doc_id: ctx.rt.rcx.encryption_inventory_doc_id.clone(),
        blobs_repo: Arc::clone(&ctx.rt.blobs_repo),
        facet_set_store: ctx.rt.doc_facet_set_index_repo.revision_store(),
        plugs_repo: Arc::clone(&ctx.rt.plugs_repo),
        parent_cancel_token: tokio_util::sync::CancellationToken::new(),
    })
    .await
}

/// The `BlobPin` facet ids on the encrypted-representation inventory.
async fn inventory_cipher_pin_ids(
    drawer: &DrawerRepo,
    inventory_doc_id: &DocId,
) -> Res<Vec<String>> {
    let Some(doc) = drawer
        .get_doc_with_facets_at_branch(inventory_doc_id, BranchPath::new(MAIN_BRANCH), None)
        .await?
    else {
        return Ok(Vec::new());
    };
    Ok(doc
        .facets
        .keys()
        .filter(|key| key.tag == FacetTag::WellKnown(WellKnownFacetTag::BlobPin))
        .map(|key| key.id.clone())
        .collect())
}

async fn wait_for_cipher_pin(
    drawer: &DrawerRepo,
    inventory_doc_id: &DocId,
    id: &str,
    should_exist: bool,
) -> Res<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let present = inventory_cipher_pin_ids(drawer, inventory_doc_id)
            .await?
            .iter()
            .any(|got| got == id);
        if present == should_exist {
            return Ok(());
        }
        eyre::ensure!(
            std::time::Instant::now() < deadline,
            "cipher pin {id} never reached {should_exist} in inventory {inventory_doc_id}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Every `ct:`/`pt:` pair tag name on the store.
async fn pair_tag_names(store: &iroh_blobs::api::Store) -> Res<std::collections::HashSet<String>> {
    use futures::StreamExt;
    let stream = store.tags().list().await?;
    futures::pin_mut!(stream);
    let mut names = std::collections::HashSet::new();
    while let Some(info) = stream.next().await {
        let name = String::from_utf8_lossy(&info?.name.0).into_owned();
        if name.starts_with(TAG_CT_PREFIX) || name.starts_with(TAG_PT_PREFIX) {
            names.insert(name);
        }
    }
    Ok(names)
}

async fn pair_tag_name(cipher: &BlobId, prefix: &str) -> String {
    format!(
        "{prefix}{}",
        iroh_blobs::Hash::from_bytes(cipher.to_bytes32())
    )
}

async fn wait_for_pair_tags(
    store: &iroh_blobs::api::Store,
    cipher: &BlobId,
    should_exist: bool,
) -> Res<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let names = pair_tag_names(store).await?;
        let ct = pair_tag_name(cipher, TAG_CT_PREFIX).await;
        let pt = pair_tag_name(cipher, TAG_PT_PREFIX).await;
        if (names.contains(&ct) && names.contains(&pt)) == should_exist {
            return Ok(());
        }
        eyre::ensure!(
            std::time::Instant::now() < deadline,
            "pair tags for {cipher} never reached {should_exist}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// A facet read at explicit heads — the historical reader's shape.
async fn read_facet_at_heads(
    drawer: &DrawerRepo,
    doc_id: &DocId,
    heads: &ChangeHashSet,
    key: &FacetKey,
) -> Res<Option<FacetRaw>> {
    let Some(doc) = drawer
        .get_doc_with_facets_at_branch_heads(
            doc_id,
            &BranchPathBuf::from(MAIN_BRANCH),
            heads,
            Some(vec![key.clone()]),
        )
        .await?
    else {
        return Ok(None);
    };
    Ok(doc.facets.get(key).cloned())
}

/// One full §15 rotation, with the pin worker watching: the facet body
/// changes (fresh `C` under a fresh key document) while its key stays the
/// same, the inventory pin follows the facet delta, the old pair's roots
/// drop through the release leaf, a reader decrypts the new representation
/// through the unchanged resolution URL, and the old key's naming is gone
/// — which is §19's rule: `key_for` resolves through the facet at the
/// *current* heads, and the updated facet no longer names `C1`.
#[tokio::test(flavor = "multi_thread")]
async fn rotation_mints_fresh_keying_material_and_the_old_representation_releases() -> Res<()> {
    let ctx = test_cx(utils_rs::function_full!()).await?;
    let worker = Worker::new(test_ctx(&ctx, None).await?, None, None);
    let plaintext_bytes = b"blob-rotation: fresh keying".to_vec();
    let plaintext = ctx.rt.blobs_repo.put(&plaintext_bytes).await?;
    let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext.clone()).await?;
    let cipher_key = worker.cipher_facet_key(&blob_key);

    worker
        .reconcile_document(
            &doc_id,
            &BranchPathBuf::from(MAIN_BRANCH),
            &worker_heads(&ctx, &doc_id).await?,
        )
        .await?;
    let cipher1 = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
        .await?
        .expect("the document names a representation before it rotates");
    let c1 = digest_str_to_blob_id_lenient(&cipher1.representation.digest)
        .expect("the facet digest is a blob id");
    let old_key_ref = cipher1.key_ref.to_string();
    let old_key_ref_heads = cipher1.key_ref_heads.clone();
    let old_encoding = cipher1.encoding_parameters.clone();

    let ctx_store = ctx.rt.blobs_repo.iroh_store();
    spawn_pin_worker_for_test(&ctx).await?;
    wait_for_cipher_pin(
        &ctx.drawer_repo,
        &worker.encryption_inventory_doc_id,
        &cipher1.representation.digest,
        true,
    )
    .await?;
    let baseline = pair_tag_names(&ctx_store).await?;
    assert_eq!(
        baseline,
        std::collections::HashSet::from([
            pair_tag_name(&c1, TAG_CT_PREFIX).await,
            pair_tag_name(&c1, TAG_PT_PREFIX).await,
        ]),
        "the pre-rotation store roots only the one pair"
    );

    let heads_before = worker_heads(&ctx, &doc_id).await?;
    worker
        .rotate_document(&doc_id, &BranchPathBuf::from(MAIN_BRANCH))
        .await?;

    let cipher2 = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
        .await?
        .expect("the rotated facet is still there");
    assert_ne!(
        cipher2.representation.digest, cipher1.representation.digest,
        "rotation mints fresh representation material"
    );
    assert_ne!(
        cipher2.key_ref.to_string(),
        old_key_ref,
        "rotation re-points the keyRef at a fresh key document"
    );
    assert!(
        !cipher2.key_ref_heads.0.is_empty(),
        "the repointed keyRef pins the key state it meant"
    );
    assert_eq!(
        cipher2.encoding_parameters, old_encoding,
        "rotation is a keying change, not a framing change"
    );

    // The inventory pin is derived, never authored: the facet delta moves
    // it, and the diff that removes the old pin drives the old pair's
    // release.
    wait_for_cipher_pin(
        &ctx.drawer_repo,
        &worker.encryption_inventory_doc_id,
        &cipher2.representation.digest,
        true,
    )
    .await?;
    wait_for_cipher_pin(
        &ctx.drawer_repo,
        &worker.encryption_inventory_doc_id,
        &cipher1.representation.digest,
        false,
    )
    .await?;
    wait_for_pair_tags(&ctx_store, &c1, false).await?;
    let c2 = digest_str_to_blob_id_lenient(&cipher2.representation.digest)
        .expect("the rotated facet digest is a blob id");
    wait_for_pair_tags(&ctx_store, &c2, true).await?;
    assert_eq!(
        pair_tag_names(&ctx_store).await?,
        std::collections::HashSet::from([
            pair_tag_name(&c2, TAG_CT_PREFIX).await,
            pair_tag_name(&c2, TAG_PT_PREFIX).await,
        ]),
        "the released pair's roots are gone; only the fresh pair is rooted"
    );

    // A reader resolving through the document decrypts the fresh
    // representation.
    let keys = DocKeySource::new(
        Arc::clone(&ctx.drawer_repo),
        doc_id.clone(),
        BranchPathBuf::from(MAIN_BRANCH),
    );
    assert_eq!(
        get_decrypted(
            &ctx_store,
            &keys,
            iroh_blobs::Hash::from_bytes(c2.to_bytes32())
        )
        .await?,
        plaintext_bytes,
        "the rotated representation decrypts to the same plaintext"
    );
    // §19: key resolution reads the facet at *current* heads, so C1's
    // naming is gone the moment the rotated facet commits — even while its
    // bytes may still sit uncollected on the store.
    let old_error = keys
        .key_for(&iroh_blobs::Hash::from_bytes(c1.to_bytes32()))
        .await
        .err()
        .expect("the old key's naming must be gone after rotation");
    assert!(
        format!("{old_error:#}").contains("no cipherBlob facet"),
        "the old key failure must say C1 is no longer named, got: {old_error:#}"
    );
    // The historical form is intact where §16 keeps it: the pre-rotation
    // facet state at pinned heads still decodes, and the old key document
    // still holds its JWK at the heads its keyRef pinned - the byte
    // release above is the only thing that stops a historical read, per
    // §16's own accounting.
    let historical = read_facet_at_heads(&ctx.drawer_repo, &doc_id, &heads_before, &cipher_key)
        .await?
        .expect("the pre-rotation facet state is retained");
    let historical = match WellKnownFacet::from_json(historical, WellKnownFacetTag::CipherBlob)? {
        WellKnownFacet::CipherBlob(cipher) => cipher,
        other => eyre::bail!("expected a cipherBlob facet, got {:?}", other.tag()),
    };
    assert_eq!(
        historical.representation.digest,
        cipher1.representation.digest
    );
    assert_eq!(historical.key_ref.to_string(), old_key_ref);
    let old_key_doc = parse_facet_ref_doc_id(&old_key_ref)?;
    let jwk = ctx
        .drawer_repo
        .get_doc_with_facets_at_branch_heads(
            &old_key_doc,
            &BranchPathBuf::from(MAIN_BRANCH),
            &old_key_ref_heads,
            None,
        )
        .await?
        .expect("the old key document is readable at the pinned heads");
    assert!(
        jwk.facets
            .keys()
            .any(|key| key.tag == FacetTag::WellKnown(WellKnownFacetTag::Jwk)),
        "the old key document still carries its JWK at the pinned heads"
    );
    ctx.stop().await?;
    Ok(())
}

/// A rotation faulted at entry has registered nothing, so the retry runs
/// the same §19 sequence and reaches the same end state: no orphaned pair
/// roots, the facet key unchanged, the old representation released.
#[tokio::test(flavor = "multi_thread")]
async fn rotation_retry_after_a_fault_at_entry_is_idempotent_and_orphan_free() -> Res<()> {
    let ctx = test_cx(utils_rs::function_full!()).await?;
    let worker_ctx = test_ctx(&ctx, None).await?;
    let worker = Worker::new(Arc::clone(&worker_ctx), None, None);
    let plaintext_bytes = b"blob-rotation: retry idempotence".to_vec();
    let plaintext = ctx.rt.blobs_repo.put(&plaintext_bytes).await?;
    let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext.clone()).await?;
    let cipher_key = worker.cipher_facet_key(&blob_key);
    worker
        .reconcile_document(
            &doc_id,
            &BranchPathBuf::from(MAIN_BRANCH),
            &worker_heads(&ctx, &doc_id).await?,
        )
        .await?;
    let cipher1 = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
        .await?
        .expect("the document names a representation");
    let c1 = digest_str_to_blob_id_lenient(&cipher1.representation.digest)
        .expect("the facet digest is a blob id");
    let ctx_store = ctx.rt.blobs_repo.iroh_store();
    spawn_pin_worker_for_test(&ctx).await?;

    worker_ctx
        .faults
        .fail_rotate
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let faulted = worker
        .rotate_document(&doc_id, &BranchPathBuf::from(MAIN_BRANCH))
        .await;
    assert!(
        faulted.is_err(),
        "the injected entry fault must fail the rotation"
    );
    let uncommitted = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
        .await?
        .expect("the facet is untouched by the faulted attempt");
    assert_eq!(
        uncommitted.representation.digest, cipher1.representation.digest,
        "a fault at entry commits nothing"
    );
    let after_fault = pair_tag_names(&ctx_store).await?;
    assert_eq!(
        after_fault.len(),
        2,
        "an entry fault registers no pair: got {after_fault:?}"
    );

    worker_ctx
        .faults
        .fail_rotate
        .store(false, std::sync::atomic::Ordering::SeqCst);
    worker
        .rotate_document(&doc_id, &BranchPathBuf::from(MAIN_BRANCH))
        .await?;
    let cipher2 = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
        .await?
        .expect("the retried rotation committed");
    assert_ne!(cipher2.representation.digest, cipher1.representation.digest);
    assert_ne!(
        cipher2.key_ref.to_string(),
        cipher1.key_ref.to_string(),
        "the retried rotation mints a fresh key document"
    );
    let c2 = digest_str_to_blob_id_lenient(&cipher2.representation.digest).expect("a blob id");
    wait_for_cipher_pin(
        &ctx.drawer_repo,
        &worker.encryption_inventory_doc_id,
        &cipher2.representation.digest,
        true,
    )
    .await?;
    wait_for_cipher_pin(
        &ctx.drawer_repo,
        &worker.encryption_inventory_doc_id,
        &cipher1.representation.digest,
        false,
    )
    .await?;
    wait_for_pair_tags(&ctx_store, &c1, false).await?;
    wait_for_pair_tags(&ctx_store, &c2, true).await?;
    assert_eq!(
        pair_tag_names(&ctx_store).await?,
        std::collections::HashSet::from([
            pair_tag_name(&c2, TAG_CT_PREFIX).await,
            pair_tag_name(&c2, TAG_PT_PREFIX).await,
        ]),
        "the retried rotation leaves exactly one pair rooted: no orphans"
    );
    let keys = DocKeySource::new(
        Arc::clone(&ctx.drawer_repo),
        doc_id.clone(),
        BranchPathBuf::from(MAIN_BRANCH),
    );
    assert_eq!(
        get_decrypted(
            &ctx_store,
            &keys,
            iroh_blobs::Hash::from_bytes(c2.to_bytes32())
        )
        .await?,
        plaintext_bytes,
        "the retried rotation's representation decrypts"
    );
    ctx.stop().await?;
    Ok(())
}

/// §19's declared crash window, asserted rather than asserted away: a fault
/// after the §19 step-1 install leaves the interrupted attempt's pair
/// rooted but unreferenced ("a crash before step 5 leaves unreferenced
/// artifacts"), and the retry recovers to a correct end state while the
/// orphan stays — the pin diff that releases pairs only drives pairs some
/// facet once named, so collecting such an orphan is GC/collect territory
/// (ADR-005-era), not the release path's.
#[tokio::test(flavor = "multi_thread")]
async fn crash_after_install_leaves_the_declared_unreferenced_pair_and_the_retry_recovers()
-> Res<()> {
    let ctx = test_cx(utils_rs::function_full!()).await?;
    let worker_ctx = test_ctx(&ctx, None).await?;
    let worker = Worker::new(Arc::clone(&worker_ctx), None, None);
    let plaintext_bytes = b"blob-rotation: crash window".to_vec();
    let plaintext = ctx.rt.blobs_repo.put(&plaintext_bytes).await?;
    let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext.clone()).await?;
    let cipher_key = worker.cipher_facet_key(&blob_key);
    worker
        .reconcile_document(
            &doc_id,
            &BranchPathBuf::from(MAIN_BRANCH),
            &worker_heads(&ctx, &doc_id).await?,
        )
        .await?;
    let cipher1 = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
        .await?
        .expect("the document names a representation");
    let c1 = digest_str_to_blob_id_lenient(&cipher1.representation.digest).expect("a blob id");
    let ctx_store = ctx.rt.blobs_repo.iroh_store();
    spawn_pin_worker_for_test(&ctx).await?;

    worker_ctx
        .faults
        .fail_rotate_after_install
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let faulted = worker
        .rotate_document(&doc_id, &BranchPathBuf::from(MAIN_BRANCH))
        .await;
    assert!(
        faulted.is_err(),
        "the after-install fault must fail the rotation"
    );
    // The interrupted attempt's pair is rooted; the facet did not move.
    let orphaned = pair_tag_names(&ctx_store).await?;
    assert_eq!(
        orphaned.len(),
        4,
        "the after-install fault leaves the attempt's pair rooted: got {orphaned:?}"
    );
    let still = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
        .await?
        .expect("the facet survives");
    assert_eq!(
        still.representation.digest, cipher1.representation.digest,
        "the commit point was not reached"
    );

    worker_ctx
        .faults
        .fail_rotate_after_install
        .store(false, std::sync::atomic::Ordering::SeqCst);
    worker
        .rotate_document(&doc_id, &BranchPathBuf::from(MAIN_BRANCH))
        .await?;
    let cipher2 = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
        .await?
        .expect("the retried rotation committed");
    let c2 = digest_str_to_blob_id_lenient(&cipher2.representation.digest).expect("a blob id");
    assert_ne!(cipher2.representation.digest, cipher1.representation.digest);
    wait_for_cipher_pin(
        &ctx.drawer_repo,
        &worker.encryption_inventory_doc_id,
        &cipher2.representation.digest,
        true,
    )
    .await?;
    wait_for_cipher_pin(
        &ctx.drawer_repo,
        &worker.encryption_inventory_doc_id,
        &cipher1.representation.digest,
        false,
    )
    .await?;
    wait_for_pair_tags(&ctx_store, &c1, false).await?;
    wait_for_pair_tags(&ctx_store, &c2, true).await?;
    let names = pair_tag_names(&ctx_store).await?;
    assert_eq!(
        names.len(),
        4,
        "c1 released, c2 rooted, the orphaned attempt's pair still rooted: got {names:?}"
    );
    let keys = DocKeySource::new(
        Arc::clone(&ctx.drawer_repo),
        doc_id.clone(),
        BranchPathBuf::from(MAIN_BRANCH),
    );
    assert_eq!(
        get_decrypted(
            &ctx_store,
            &keys,
            iroh_blobs::Hash::from_bytes(c2.to_bytes32())
        )
        .await?,
        plaintext_bytes,
        "the recovered representation decrypts"
    );
    ctx.stop().await?;
    Ok(())
}

/// A rotation request runs as an `EncryptionTask` on the facet machine's
/// own keyed scheduler — same branch key as reconciles, so it cannot race a
/// delta task — and the machine keeps walking afterwards (the carried
/// pending-delta cursor is settled on rotation completion, so the request
/// cannot gate the durable prefix).
#[tokio::test(flavor = "multi_thread")]
async fn a_rotation_request_runs_on_the_facet_machine_and_leaves_it_walking() -> Res<()> {
    let ctx = test_cx(utils_rs::function_full!()).await?;
    let worker_ctx = test_ctx(&ctx, None).await?;
    // The request may land before or after the document's own delta;
    // either way the machine must produce a representation and keep going.
    let (rotation_tx, rotation_rx) = rotation_channel();
    let mut worker = Worker::new(Arc::clone(&worker_ctx), Some(rotation_rx), None);
    let cancel_token = CancellationToken::new();
    let facet_index = Arc::clone(&ctx.rt.doc_facet_set_index_repo);
    let handle = tokio::spawn({
        let cancel_token = cancel_token.clone();
        let revision_store = facet_index.revision_store();
        async move { worker.run(revision_store, cancel_token).await }
    });

    let plaintext = ctx
        .rt
        .blobs_repo
        .put(b"blob-rotation: scheduler path")
        .await?;
    let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext).await?;
    await_indexed(&ctx, &doc_id).await?;
    let cipher_key = worker_ctx.cipher_facet_key(&blob_key);
    rotation_tx
        .send(RotationRequest {
            doc_id: doc_id.clone(),
        })
        .await?;
    let cipher1 = await_representation(&ctx, &doc_id, &cipher_key, "the machine").await?;
    // The rotation either subsumed the reconcile (carried cursor) or ran
    // after it; either way a *further* request must produce a fresh
    // representation through the same scheduler.
    rotation_tx
        .send(RotationRequest {
            doc_id: doc_id.clone(),
        })
        .await?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let cipher2 = loop {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let cipher = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
            .await?
            .expect("the facet survives");
        if cipher.representation.digest != cipher1.representation.digest {
            break cipher;
        }
        eyre::ensure!(
            std::time::Instant::now() < deadline,
            "the second rotation request never committed a fresh representation"
        );
    };
    assert_ne!(
        cipher2.key_ref.to_string(),
        cipher1.key_ref.to_string(),
        "each rotation mints its own key document"
    );

    // The machine is still walking: a new document gets its representation.
    let other_plaintext = ctx
        .rt
        .blobs_repo
        .put(b"blob-rotation: machine still walking")
        .await?;
    let (other_doc, other_blob_key) = stage_document_with_blob(&ctx, other_plaintext).await?;
    await_indexed(&ctx, &other_doc).await?;
    await_representation(
        &ctx,
        &other_doc,
        &worker_ctx.cipher_facet_key(&other_blob_key),
        "the machine after rotations",
    )
    .await?;

    cancel_token.cancel();
    drop(rotation_tx);
    handle.await??;
    ctx.stop().await?;
    Ok(())
}

/// The key document's doc id out of a `db+facet:///{doc_id}/{facet}` ref.
fn parse_facet_ref_doc_id(key_ref: &str) -> Res<DocId> {
    daybook_types::url::parse_facet_ref_str(key_ref)
        .map_err(|err| eyre::eyre!("keyRef {key_ref:?} is not a facet reference: {err}"))
        .map(|reference| reference.doc_id)
}
