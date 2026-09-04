use super::validation::check_breaking_changes;
use super::*;

/// ADR 007 §3: `@daybook/core` cannot be disabled. The guard lives in the
/// plugg config facet mutation, not in the runtime.
const CORE_PLUG_ID: &str = "@daybook/core";

/// Outcome of recording a manifest doc in the config facet.
pub(crate) enum RecordKnownOutcome {
    /// Recorded (or already current) and the derived cache updated.
    Recorded { plug_id: String },
    /// No manifest facet readable at the given heads.
    Unreadable,
    /// The version/compat gate rejected the update; nothing recorded.
    Rejected {
        plug_id: String,
        version: semver::Version,
        reason: String,
    },
}

impl PlugsRepo {
    /// ADR 007 §4: idempotent ensure-on-open. Ensures the core manifest doc
    /// exists (creating it via the single unchecked internal doc-create) and
    /// the plugg config facet exists with `@daybook/core` enabled at the core
    /// doc's initial heads.
    pub async fn ensure_core_plug(&self) -> Res<()> {
        let drawer = self
            .drawer
            .get()
            .ok_or_eyre("plugs repo drawer not attached")?;
        // Register the repo config doc in the drawer (idempotent) so the plugg
        // config facet writes go through the drawer like any facet write.
        drawer
            .register_existing_doc(
                &self.doc_config_id,
                self.doc_config_id
                    .parse()
                    .map_err(|err| ferr!("invalid doc_config id: {err}"))?,
                daybook_types::doc::BranchPath::new("main"),
            )
            .await?;

        if self
            .config_store()?
            .query_sync(|config| config.enabled.contains_key(CORE_PLUG_ID))
            .await
        {
            return Ok(());
        }

        // Create the core manifest doc if missing (unchecked: nothing is
        // registered yet — the single write that skips facet validation).
        let core_manifest = system_plugs()
            .into_iter()
            .find(|manifest| manifest.id() == CORE_PLUG_ID)
            .ok_or_eyre("core plug manifest missing from system_plugs")?;
        let core_doc_id = drawer
            .add_unchecked(daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets: [(
                    Self::plug_manifest_facet_key(),
                    daybook_types::doc::WellKnownFacet::PlugManifest(core_manifest).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;
        let core_doc_id = daybook_types::doc::DocId::from(core_doc_id);

        // Read the core manifest back at its initial heads and seed the derived
        // cache so the config facet write below validates normally.
        let core_heads = drawer
            .get_doc_branches(&core_doc_id)
            .await?
            .and_then(|entry| entry.branches.get("main").cloned())
            .ok_or_eyre("core manifest doc missing main branch")?;
        let core_manifest = self
            .read_manifest_doc(&core_doc_id, &core_heads)
            .await?
            .ok_or_eyre("core manifest doc unreadable at initial heads")?;
        let ref_url = Self::build_enabled_ref(&core_doc_id, "main", &core_heads)?;
        surelock::key::lock_scope(|key| {
            let (mut cache, _key) = key.lock(&self.cache);
            cache.upsert_known(CORE_PLUG_ID, &core_manifest);
            // Core is being enabled by the config write below; seed active
            // so that write validates normally (the plugConfig facet is
            // owned by core itself).
            cache.set_active(CORE_PLUG_ID, core_heads.clone(), Arc::clone(&core_manifest));
        });

        // Write the plugg config facet with core enabled at the core doc's
        // initial heads (validated against core's own plugConfig facet).
        drop(
            self.config_store()?
                .mutate_sync(|config| {
                    config
                        .enabled
                        .insert(CORE_PLUG_ID.to_string(), ref_url.clone());
                    let version = core_manifest.version.to_string();
                    config.known_plugs.insert(
                        CORE_PLUG_ID.to_string(),
                        KnownPlug {
                            latest: ref_url.clone(),
                            latest_version: version.clone(),
                            latest_rejection: None,
                            last_valid: ref_url.clone(),
                            last_valid_version: version.clone(),
                            last_enabled_version: Some(version),
                        },
                    );
                })
                .await?,
        );
        // The manual cache seed above already made core active; re-apply
        // from the ref for consistency.
        self.activate_from_ref(CORE_PLUG_ID, &ref_url).await?;
        Ok(())
    }

    /// ADR 007 §3: enable a plug by pinning a full ref. The ref must point at a
    /// readable `plugManifest/main` facet; the manifest id becomes the key.
    pub async fn enable_plug(&self, ref_url: &url::Url) -> Res<ChangeHashSet> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let parsed = Self::parse_enabled_ref(ref_url)?;
        let Some((heads, manifest)) = self.read_manifest_at_ref(ref_url).await? else {
            eyre::bail!("manifest not readable at ref heads: {ref_url}");
        };
        let plug_id = manifest.id();
        let ref_url = if parsed.at.is_none() {
            Self::build_enabled_ref(&parsed.doc_id, "main", &heads)?
        } else {
            ref_url.clone()
        };
        // ADR 007 §5 gate: an explicit activation must be valid and a
        // valid upgrade of the last seen version.
        if let Some(reason) = self.check_activation(&plug_id, &manifest, &ref_url).await? {
            eyre::bail!("activation rejected for {plug_id}: {reason}");
        }
        let _guard = self.mutation_mutex.lock().await;
        // ADR 007 §2: the plug's config doc is created at enablement and the
        // mapping recorded in the config facet, so an enabled plug always has
        // a config doc. The mapping is retained across disablement (disable
        // only removes the enabled entry). The doc creation is async, so it
        // happens before the store mutate; the closure re-checks under the
        // store lock.
        let (needs_config_doc, already_enabled) = self
            .config_store()?
            .query_sync(|config| {
                (
                    !config.plug_config_doc_ids.contains_key(&plug_id),
                    config.enabled.get(&plug_id).cloned(),
                )
            })
            .await;
        let config_doc_id = if needs_config_doc {
            let drawer = self
                .drawer
                .get()
                .ok_or_eyre("plugs repo drawer not attached")?;
            Some(
                drawer
                    .add(daybook_types::doc::AddDocArgs {
                        branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                        facets: HashMap::new(),
                        user_path: None,
                    })
                    .await?,
            )
        } else {
            None
        };
        let enabled_version = manifest.version.to_string();
        let (_, new_heads) = self
            .config_store()?
            .mutate_sync(|config| {
                config.enabled.insert(plug_id.clone(), ref_url.clone());
                // Track the enabled version per plug. The gate above validated
                // the ref; if it is the latest (or the plug is new), it is also
                // the latest valid. A rollback re-enable must not regress the
                // latest fields.
                let track = config
                    .known_plugs
                    .entry(plug_id.clone())
                    .or_insert_with(|| KnownPlug {
                        latest: ref_url.clone(),
                        latest_version: enabled_version.clone(),
                        latest_rejection: None,
                        last_valid: ref_url.clone(),
                        last_valid_version: enabled_version.clone(),
                        last_enabled_version: None,
                    });
                track.last_enabled_version = Some(enabled_version.clone());
                let is_new_latest = track.latest_version.is_empty()
                    || semver::Version::parse(&track.latest_version)
                        .map_or(true, |ver| ver <= manifest.version);
                if is_new_latest {
                    track.latest = ref_url.clone();
                    track.latest_version = enabled_version.clone();
                    track.latest_rejection = None;
                    track.last_valid = ref_url.clone();
                    track.last_valid_version = enabled_version.clone();
                }
                if let Some(doc_id) = config_doc_id {
                    config
                        .plug_config_doc_ids
                        .entry(plug_id.clone())
                        .or_insert(doc_id);
                }
            })
            .await?;
        if already_enabled.as_ref() != Some(&ref_url) {
            self.activate_from_ref(&plug_id, &ref_url).await?;
        }
        Ok(new_heads)
    }

    /// ADR 007 §3: disable a plug by removing its config entry. `@daybook/core`
    /// cannot be disabled — the guard lives in this config mutation.
    pub async fn disable_plug(&self, plug_id: &str) -> Res<ChangeHashSet> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        if plug_id == CORE_PLUG_ID {
            eyre::bail!("@daybook/core cannot be disabled");
        }
        let _guard = self.mutation_mutex.lock().await;
        let (_, new_heads) = self
            .config_store()?
            .mutate_sync(|config| {
                config.enabled.remove(plug_id);
            })
            .await?;
        surelock::key::lock_scope(|key| {
            let (mut cache, _key) = key.lock(&self.cache);
            cache.clear_active(plug_id);
        });
        Ok(new_heads)
    }

    /// ADR 007 §3: explicit re-pin to the latest main-branch heads. Fails if
    /// the manifest is not readable at the new heads.
    pub async fn update_plug(&self, plug_id: &str) -> Res<ChangeHashSet> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let _guard = self.mutation_mutex.lock().await;
        let Some(old_ref) = self
            .config_store()?
            .query_sync(|config| config.enabled.get(plug_id).cloned())
            .await
        else {
            eyre::bail!("plug not enabled: {plug_id}");
        };
        let parsed = Self::parse_enabled_ref(&old_ref)?;
        let drawer = self
            .drawer
            .get()
            .ok_or_eyre("plugs repo drawer not attached")?;
        let heads = drawer
            .get_branch_heads_for_path(&parsed.doc_id, daybook_types::doc::BranchPath::new("main"))
            .await?
            .ok_or_eyre("doc has no main branch heads")?;
        let new_ref = Self::build_enabled_ref(&parsed.doc_id, "main", &heads)?;
        let Some((_, manifest)) = self.read_manifest_at_ref(&new_ref).await? else {
            eyre::bail!("manifest not readable at latest heads for {plug_id}");
        };
        if manifest.id() != plug_id {
            eyre::bail!("manifest id mismatch at latest heads for {plug_id}");
        }
        // ADR 007 §5 gate: jump-to-latest is an explicit upgrade — a
        // republish without a version bump or a breaking change must not
        // become active.
        if let Some(reason) = self.check_activation(plug_id, &manifest, &new_ref).await? {
            eyre::bail!("update rejected for {plug_id}: {reason}");
        }
        let enabled_version = manifest.version.to_string();
        let (_, new_heads) = self
            .config_store()?
            .mutate_sync(|config| {
                config.enabled.insert(plug_id.to_string(), new_ref.clone());
                let track = config
                    .known_plugs
                    .entry(plug_id.to_string())
                    .or_insert_with(|| KnownPlug {
                        latest: new_ref.clone(),
                        latest_version: enabled_version.clone(),
                        latest_rejection: None,
                        last_valid: new_ref.clone(),
                        last_valid_version: enabled_version.clone(),
                        last_enabled_version: None,
                    });
                track.last_enabled_version = Some(enabled_version.clone());
                let is_new_latest = track.latest_version.is_empty()
                    || semver::Version::parse(&track.latest_version)
                        .map_or(true, |ver| ver <= manifest.version);
                if is_new_latest {
                    track.latest = new_ref.clone();
                    track.latest_version = enabled_version.clone();
                    track.latest_rejection = None;
                    track.last_valid = new_ref.clone();
                    track.last_valid_version = enabled_version.clone();
                }
            })
            .await?;
        self.activate_from_ref(plug_id, &new_ref).await?;
        Ok(new_heads)
    }

    /// ADR 007 §8: import a plug from an existing manifest doc. Reads the
    /// manifest facet at the given heads, validates, grants core-docs-group
    /// access so the doc replicates in the core partition, and enables at
    /// those heads.
    pub async fn import_from_doc_id(
        &self,
        doc_id: &daybook_types::doc::DocId,
        heads: &ChangeHashSet,
        no_enable: bool,
    ) -> Res<ImportedPlug> {
        let manifest = self
            .read_manifest_doc(doc_id, heads)
            .await?
            .ok_or_eyre("no plugManifest facet at given heads")?;
        self.validate_incoming_plug(&manifest).await?;
        let drawer = self
            .drawer
            .get()
            .ok_or_eyre("plugs repo drawer not attached")?;
        let branch_ref = drawer
            .get_branch_ref(doc_id, daybook_types::doc::BranchPath::new("main"))
            .await?
            .ok_or_eyre("manifest doc missing main branch")?;
        let authority =
            crate::authority::ensure(&self.big_repo, drawer.meta_store_sql(), None).await?;
        crate::authority::grant_docs_admin(
            &self.big_repo,
            &authority.core_docs,
            [branch_ref.branch_doc_id],
        )
        .await?;
        let ref_url = Self::build_enabled_ref(doc_id, "main", heads)?;
        match self.record_known_manifest_doc(doc_id, heads).await? {
            RecordKnownOutcome::Rejected { reason, .. } if !no_enable => {
                eyre::bail!("import rejected: {reason}")
            }
            // Known-only import: a version/compat rejection is fine — the
            // plug is recorded (with the rejection) but not enabled.
            RecordKnownOutcome::Rejected { .. } => {}
            RecordKnownOutcome::Unreadable => {
                eyre::bail!("manifest doc unreadable at given heads")
            }
            RecordKnownOutcome::Recorded { .. } => {}
        }
        if !no_enable {
            self.enable_plug(&ref_url).await?;
        }
        Ok(ImportedPlug {
            plug_id: manifest.id(),
            version: manifest.version.clone(),
            doc_id: Some(doc_id.clone()),
            imported_blob_hashes: vec![],
            source_digest: None,
        })
    }

    /// ADR 007 §9: import a plug from a bare doc id at current main branch
    /// heads (or explicit heads).
    pub async fn import_doc_id(
        &self,
        doc_id: &daybook_types::doc::DocId,
        heads: Option<&ChangeHashSet>,
        no_enable: bool,
    ) -> Res<ImportedPlug> {
        let heads = match heads {
            Some(heads) => heads.clone(),
            None => {
                let drawer = self
                    .drawer
                    .get()
                    .ok_or_eyre("plugs repo drawer not attached")?;
                drawer
                    .get_branch_heads_for_path(doc_id, daybook_types::doc::BranchPath::new("main"))
                    .await?
                    .ok_or_eyre("manifest doc missing main branch")?
            }
        };
        self.import_from_doc_id(doc_id, &heads, no_enable).await
    }

    /// ADR 007 §9: enable a plug by full facet ref, or by bare doc id
    /// (current main branch heads, or explicit heads). Doc-id targets get
    /// core-docs-group access so the manifest doc replicates in the core
    /// partition. Returns the pinned ref.
    pub async fn enable_target(
        &self,
        target: &str,
        heads: Option<&ChangeHashSet>,
    ) -> Res<url::Url> {
        if target.starts_with("db+facet://") {
            let ref_url = url::Url::parse(target)?;
            self.enable_plug(&ref_url).await?;
            return Ok(ref_url);
        }
        let drawer = self
            .drawer
            .get()
            .ok_or_eyre("plugs repo drawer not attached")?;
        let doc_id = target.to_string();
        let branch_path = daybook_types::doc::BranchPath::new("main");
        let heads = match heads {
            Some(heads) => heads.clone(),
            None => drawer
                .get_branch_heads_for_path(&doc_id, branch_path)
                .await?
                .ok_or_eyre("manifest doc missing main branch")?,
        };
        let branch_ref = drawer
            .get_branch_ref(&doc_id, branch_path)
            .await?
            .ok_or_eyre("manifest doc missing main branch")?;
        let authority =
            crate::authority::ensure(&self.big_repo, drawer.meta_store_sql(), None).await?;
        crate::authority::grant_docs_admin(
            &self.big_repo,
            &authority.core_docs,
            [branch_ref.branch_doc_id],
        )
        .await?;
        let ref_url = Self::build_enabled_ref(target, "main", &heads)?;
        self.enable_plug(&ref_url).await?;
        Ok(ref_url)
    }

    /// Add a new plug to the repo after validating it (ADR 007 §8: authoring).
    /// Writes a manifest doc through the drawer; the manifest facet is
    /// validated like any other facet write.
    pub async fn add(
        &self,
        mut manifest: manifest::PlugManifest,
    ) -> Res<daybook_types::doc::DocId> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        // we use the mutex to make a critical section
        // to avoid race conditions on the validation checks
        let _guard = self.mutation_mutex.lock().await;
        // 1. Validate the incoming plug manifest
        // We do this first to ensure that we don't pollute the store with invalid data.
        // This includes checking internal consistency, external dependencies,
        // and compatibility with existing versions of the same plug.
        self.validate_incoming_plug(&manifest).await?;

        // 1.5 Convert file:// URLs to db+blob:// URLs
        // This ensures that all components are stored in the BlobsRepo for portability.
        for bundle in manifest.wflow_bundles.values_mut() {
            let bundle = Arc::make_mut(bundle);
            for url in bundle.component_urls.iter_mut() {
                match url.scheme() {
                    "file" => {
                        let path = url
                            .to_file_path()
                            .map_err(|err| eyre::eyre!("invalid path in url {url:?} {err:?}"))?;
                        let data = tokio::fs::read(&path).await.wrap_err_with(|| {
                            format!("failed to read component file: {}", path.display())
                        })?;
                        let hash = self.blobs.put(&data).await?;
                        *url =
                            url::Url::parse(&format!("{}:///{}", crate::blobs::BLOB_SCHEME, hash))?;
                    }
                    crate::blobs::BLOB_SCHEME => {}
                    _ => {
                        eyre::bail!("unsupported component_url scheme: {url}");
                    }
                }
            }
        }

        // 1.9 Bake the plug's blob references as Blob facets on the manifest
        // doc (ADR 001 + the plugs rework): the manifest doc is a static
        // artifact and these facets are its blob declarations, written once
        // at authoring. Pinning is driven from these facets (enablement via
        // the plugs config event stream), never by parsing the manifest back.
        // Blobs we cannot size are skipped: their representation length is
        // unknown until they land in the local blob store.
        let mut blob_lengths = std::collections::HashMap::<String, u64>::new();
        for bundle in manifest.wflow_bundles.values() {
            for url in &bundle.component_urls {
                if url.scheme() != crate::blobs::BLOB_SCHEME {
                    continue;
                }
                let hash = url.path().trim_start_matches('/');
                if blob_lengths.contains_key(hash) {
                    continue;
                }
                if let Ok(blob_id) = hash.parse::<crate::blobs::BlobId>()
                    && let Ok(path) = self.blobs.get_path(&blob_id).await
                    && let Ok(meta) = tokio::fs::metadata(&path).await
                {
                    blob_lengths.insert(hash.to_string(), meta.len());
                } else {
                    tracing::warn!(hash, "authoring: blob not sized; facet skipped");
                }
            }
        }
        let mut facets = std::collections::HashMap::new();
        facets.insert(
            Self::plug_manifest_facet_key(),
            daybook_types::doc::WellKnownFacet::PlugManifest(manifest).into(),
        );
        for (hash, length_octets) in blob_lengths {
            facets.insert(
                daybook_types::doc::FacetKey {
                    tag: daybook_types::doc::WellKnownFacetTag::Blob.into(),
                    id: hash.clone(),
                },
                daybook_types::doc::WellKnownFacet::Blob(daybook_types::doc::Blob {
                    mime: "application/octet-stream".to_string(),
                    length_octets,
                    digest: hash.clone(),
                    inline: None,
                    urls: Some(vec![format!(
                        "{}:///{}",
                        crate::blobs::BLOB_SCHEME,
                        hash
                    )]),
                })
                .into(),
            );
        }

        // 2. Write a manifest doc through the drawer (validated).
        let drawer = self
            .drawer
            .get()
            .ok_or_eyre("plugs repo drawer not attached")?;
        let doc_id = drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets,
                user_path: None,
            })
            .await?;

        // 3. Grant core-docs-group access so the manifest doc replicates in the
        // core partition.
        let branch_ref = drawer
            .get_branch_ref(&doc_id, daybook_types::doc::BranchPath::new("main"))
            .await?
            .ok_or_eyre("manifest doc missing main branch")?;
        let authority =
            crate::authority::ensure(&self.big_repo, drawer.meta_store_sql(), None).await?;
        crate::authority::grant_docs_admin(
            &self.big_repo,
            &authority.core_docs,
            [branch_ref.branch_doc_id],
        )
        .await?;

        // ADR 007 §5: record the new manifest doc in the config facet's
        // known_plugs so the derived cache sees it (authoring validations
        // like tag clashes and version checks run against the cache). The
        // config write takes the mutation mutex itself. The record gate is
        // the same one the remote path uses: a version that breaks the last
        // ENABLED version fails the add loudly.
        drop(_guard);
        let heads = drawer
            .get_doc_branches(&doc_id)
            .await?
            .and_then(|entry| entry.branches.get("main").cloned())
            .ok_or_eyre("manifest doc missing main branch")?;
        match self.record_known_manifest_doc(&doc_id, &heads).await? {
            RecordKnownOutcome::Rejected { reason, .. } => {
                eyre::bail!("add rejected: {reason}")
            }
            RecordKnownOutcome::Unreadable => {
                eyre::bail!("manifest doc unreadable after add")
            }
            RecordKnownOutcome::Recorded { .. } => {}
        }

        Ok(doc_id)
    }

    /// ADR 007 §5: ensure a manifest doc is recorded in the config facet's
    /// known_plugs (plug id -> track at the given heads). Called by the
    /// authoring/import paths and by the durable manifest-doc revision
    /// consumer on manifest doc changes.
    /// Updates the derived cache incrementally for this plug.
    ///
    /// Gate (ADR 007 §5): a manifest update must bump the version over the
    /// latest version seen (republishes and downgrades are rejected), pass
    /// the structural validation, and — for a currently enabled plug — not
    /// break the last enabled version. The latest version's status (valid
    /// or rejection reason) is recorded in the track, so it is durable and
    /// queryable without collecting events, and "update to latest" can be
    /// blocked. Rejections return `Rejected` — never an error — so a bad
    /// remote manifest cannot take down the manifest consumer.
    pub(crate) async fn record_known_manifest_doc(
        &self,
        doc_id: &daybook_types::doc::DocId,
        heads: &ChangeHashSet,
    ) -> Res<RecordKnownOutcome> {
        let _guard = self.mutation_mutex.lock().await;
        let Some(manifest) = self.read_manifest_doc(doc_id, heads).await? else {
            return Ok(RecordKnownOutcome::Unreadable);
        };
        let ref_url = Self::build_enabled_ref(doc_id, "main", heads)?;
        let plug_id = manifest.id();
        let incoming_version = manifest.version.clone();
        let track = self
            .config_store()?
            .query_sync(|config| config.known_plugs.get(&plug_id).cloned())
            .await;
        if let Some(track) = &track
            && track.latest == ref_url
        {
            // Already recorded at these heads — idempotent replay.
            surelock::key::lock_scope(|key| {
                let (mut cache, _key) = key.lock(&self.cache);
                cache.upsert_known(&plug_id, &manifest);
            });
            return Ok(RecordKnownOutcome::Recorded { plug_id });
        }
        let mut reason = None;
        // Gate A: version must strictly bump over the latest version seen.
        if let Some(track) = &track
            && let Ok(latest_version) = semver::Version::parse(&track.latest_version)
            && incoming_version <= latest_version
        {
            reason = Some(format!(
                "version must be greater than the latest version ({latest_version})"
            ));
        }
        // Gate B: structural validity (garde, tag clashes, dependencies) —
        // no version baseline.
        if reason.is_none()
            && let Err(err) = self.validate_structure(&manifest).await
        {
            reason = Some(format!("{err:#}"));
        }
        // Gate C: compat vs the last ENABLED version — the only materialized
        // surface. We can't enforce that every semver bump across the whole
        // (distributed) history is valid; we only care that a new version
        // doesn't break the previously enabled one.
        if reason.is_none() {
            let enabled_ref = self
                .config_store()?
                .query_sync(|config| config.enabled.get(&plug_id).cloned())
                .await;
            if let Some(ref_url) = enabled_ref
                && let Some((_, enabled_manifest)) = self.read_manifest_at_ref(&ref_url).await?
                && let Err(err) = check_breaking_changes(&manifest, &enabled_manifest)
            {
                reason = Some(format!("{err:#}"));
            }
        }
        let rejection = reason.clone();
        self.config_store()?
            .mutate_sync(|config| {
                let track = config
                    .known_plugs
                    .entry(plug_id.clone())
                    .or_insert_with(|| KnownPlug {
                        latest: ref_url.clone(),
                        latest_version: String::new(),
                        latest_rejection: None,
                        last_valid: ref_url.clone(),
                        last_valid_version: String::new(),
                        last_enabled_version: None,
                    });
                // The idempotence check above guarantees we only reach this
                // point when the track is fresh or its latest ref differs —
                // either way the new version must be recorded.
                track.latest = ref_url.clone();
                track.latest_version = incoming_version.to_string();
                track.latest_rejection = rejection.clone();
                if rejection.is_none() {
                    // The new version is the latest VALID one.
                    track.last_valid = ref_url.clone();
                    track.last_valid_version = incoming_version.to_string();
                }
            })
            .await?;
        match reason {
            None => {
                surelock::key::lock_scope(|key| {
                    let (mut cache, _key) = key.lock(&self.cache);
                    cache.upsert_known(&plug_id, &manifest);
                });
                Ok(RecordKnownOutcome::Recorded { plug_id })
            }
            Some(reason) => Ok(RecordKnownOutcome::Rejected {
                plug_id,
                version: incoming_version,
                reason,
            }),
        }
    }

    /// ADR 007 §5: gate an explicit activation (enable / re-pin). Returns a
    /// rejection reason when the manifest must not become active:
    /// - unknown plug: the structural validation must pass;
    /// - same version: blocked when the latest version was rejected (the
    ///   durable status blocks "update to latest"), or when the ref is not
    ///   the recorded one (republish without a version bump);
    /// - downgrade: blocked, except a rollback to the previously enabled
    ///   version (it was validated when first enabled);
    /// - upgrade: structural validation + no breaking changes vs the last
    ///   ENABLED version.
    async fn check_activation(
        &self,
        plug_id: &str,
        manifest: &manifest::PlugManifest,
        ref_url: &url::Url,
    ) -> Res<Option<String>> {
        let track = self
            .config_store()?
            .query_sync(|config| config.known_plugs.get(plug_id).cloned())
            .await;
        let Some(track) = track else {
            // First sighting: no baseline — structural gate only.
            let err = self
                .validate_structure(manifest)
                .await
                .err()
                .map(|err| format!("{err:#}"));
            return Ok(err);
        };
        let latest_version = match semver::Version::parse(&track.latest_version) {
            Ok(version) => version,
            // Corrupt baseline: don't block activation on it.
            Err(_) => return Ok(None),
        };
        let incoming = manifest.version.clone();
        if incoming == latest_version {
            if let Some(reason) = &track.latest_rejection {
                // Block "update to latest" against a rejected latest.
                return Ok(Some(format!(
                    "latest version {incoming} was rejected: {reason}"
                )));
            }
            return Ok((track.latest != *ref_url).then(|| {
                "republish without a version bump: the same version was already validated at different heads"
                    .to_string()
            }));
        }
        if incoming < latest_version {
            // Rolling back to the previously enabled version (e.g. after a
            // disable) is allowed — it was validated when first enabled.
            let last_enabled = track
                .last_enabled_version
                .as_ref()
                .and_then(|ver| semver::Version::parse(ver).ok());
            if last_enabled == Some(incoming.clone()) {
                return Ok(None);
            }
            return Ok(Some(format!(
                "downgrade rejected: version {incoming} is older than the latest version {latest_version}"
            )));
        }
        // Upgrade: structural validity + no breaking changes vs the last
        // ENABLED version (the only materialized baseline).
        if let Err(err) = self.validate_structure(manifest).await {
            return Ok(Some(format!("{err:#}")));
        }
        let enabled_ref = self
            .config_store()?
            .query_sync(|config| config.enabled.get(plug_id).cloned())
            .await;
        if let Some(enabled_ref) = enabled_ref
            && let Some((_, enabled_manifest)) = self.read_manifest_at_ref(&enabled_ref).await?
            && let Err(err) = check_breaking_changes(manifest, &enabled_manifest)
        {
            return Ok(Some(format!("{err:#}")));
        }
        Ok(None)
    }
}
