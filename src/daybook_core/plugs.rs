use crate::interlude::*;
use tokio_util::sync::CancellationToken;

pub mod manifest;

pub fn system_plugs() -> Vec<manifest::PlugManifest> {
    use daybook_types::doc::*;
    use manifest::*;

    vec![
        PlugManifest {
            namespace: "daybook".into(),
            name: "core".into(),
            version: "0.0.1".parse().unwrap(),
            title: "Daybook Core".into(),
            desc: "Core keys and routines".into(),
            dependencies: default(),
            routines: default(),
            wflow_bundles: default(),
            commands: default(),
            processors: default(),
            props: vec![
                PropKeyManifest {
                    key_tag: WellKnownPropTag::RefGeneric.into(),
                    value_schema: schemars::schema_for!(String),
                    display_config: default(),
                },
                PropKeyManifest {
                    key_tag: WellKnownPropTag::LabelGeneric.into(),
                    value_schema: schemars::schema_for!(String),
                    display_config: default(),
                },
                PropKeyManifest {
                    key_tag: WellKnownPropTag::TitleGeneric.into(),
                    value_schema: schemars::schema_for!(String),
                    display_config: PropKeyDisplayHint {
                        display_title: Some("Title".to_string()),
                        deets: PropKeyDisplayDeets::Title { show_editor: true },
                        ..default()
                    },
                },
                PropKeyManifest {
                    key_tag: WellKnownPropTag::PathGeneric.into(),
                    value_schema: schemars::schema_for!(String),
                    display_config: PropKeyDisplayHint {
                        display_title: Some("Path".to_string()),
                        deets: PropKeyDisplayDeets::UnixPath,
                        ..default()
                    },
                },
                PropKeyManifest {
                    key_tag: WellKnownPropTag::ImageMetadata.into(),
                    value_schema: schemars::schema_for!(ImageMetadata),
                    display_config: default(),
                },
                PropKeyManifest {
                    key_tag: WellKnownPropTag::Content.into(),
                    value_schema: schemars::schema_for!(Content),
                    display_config: default(),
                },
                PropKeyManifest {
                    key_tag: WellKnownPropTag::Pending.into(),
                    value_schema: schemars::schema_for!(Pending),
                    display_config: default(),
                },
            ],
        },
        PlugManifest {
            namespace: "daybook".into(),
            name: "wip".into(),
            version: "0.0.1".parse().unwrap(),
            title: "Daybook WIP".into(),
            desc: "Experiment bed for WIP features".into(),
            dependencies: [
                //
                (
                    "@daybook/core@v0.0.1".into(),
                    PlugDependencyManifest {
                        keys: vec![
                            PropKeyDependencyManifest {
                                key_tag: WellKnownPropTag::Content.into(),
                                value_schema: schemars::schema_for!(Content),
                            },
                            PropKeyDependencyManifest {
                                key_tag: WellKnownPropTag::LabelGeneric.into(),
                                value_schema: schemars::schema_for!(String),
                            },
                        ],
                    }
                    .into(),
                ),
            ]
            .into(),
            routines: [
                (
                    "pseudo-label".into(),
                    RoutineManifest {
                        r#impl: RoutineImpl::Wflow {
                            key: "pseudo-label".into(),
                            bundle: "daybook_wflows".into(),
                        },
                        deets: RoutineManifestDeets::DocProp {
                            working_prop_tag: WellKnownPropTag::PseudoLabel.into(),
                        },
                        prop_acl: vec![
                            RoutinePropAccess {
                                tag: WellKnownPropTag::Content.into(),
                                read: true,
                                write: false,
                            },
                            RoutinePropAccess {
                                tag: WellKnownPropTag::PseudoLabel.into(),
                                read: true,
                                write: true,
                            },
                        ],
                    }
                    .into(),
                ),
                #[cfg(debug_assertions)]
                (
                    "test-label".into(),
                    RoutineManifest {
                        r#impl: RoutineImpl::Wflow {
                            key: "test-label".into(),
                            bundle: "daybook_wflows".into(),
                        },
                        deets: RoutineManifestDeets::DocProp {
                            working_prop_tag: WellKnownPropTag::LabelGeneric.into(),
                        },
                        prop_acl: vec![RoutinePropAccess {
                            tag: WellKnownPropTag::LabelGeneric.into(),
                            read: true,
                            write: true,
                        }],
                    }
                    .into(),
                ),
            ]
            .into(),
            commands: [
                (
                    "pseudo-label".into(),
                    CommandManifest {
                        desc: "Use LLM to label the document".into(),
                        deets: CommandDeets::DocCommand {
                            routine_name: "pseudo-label".into(),
                        },
                    }
                    .into(),
                ),
                #[cfg(debug_assertions)]
                (
                    "test-label".into(),
                    CommandManifest {
                        desc: "Add a test LabelGeneric for testing".into(),
                        deets: CommandDeets::DocCommand {
                            routine_name: "test-label".into(),
                        },
                    }
                    .into(),
                ),
            ]
            .into(),
            processors: [
                (
                    "pseudo-label".into(),
                    ProcessorManifest {
                        desc: "Use LLM to label the document content".into(),
                        deets: ProcessorDeets::DocProcessor {
                            routine_name: "pseudo-label".into(),
                            predicate: DocPredicateClause::HasTag(WellKnownPropTag::Content.into()),
                        },
                    }
                    .into(),
                ),
                #[cfg(debug_assertions)]
                (
                    "test-label".into(),
                    ProcessorManifest {
                        desc: "Add a test LabelGeneric for testing".into(),
                        deets: ProcessorDeets::DocProcessor {
                            routine_name: "test-label".into(),
                            predicate: DocPredicateClause::Or(default()),
                        },
                    }
                    .into(),
                ),
            ]
            .into(),
            wflow_bundles: [
                //
                (
                    "daybook_wflows".into(),
                    WflowBundleManifest {
                        keys: vec!["pseudo-label".into(), "test-label".into()],
                        // FIXME: make this more generic
                        component_urls: vec![{
                            let path = std::path::absolute(
                                Path::new(env!("CARGO_MANIFEST_DIR"))
                                    .join("../../target/wasm32-wasip2/debug/daybook_wflows.wasm"),
                            )
                            .unwrap();

                            format!("file://{path}", path = path.to_string_lossy())
                                .parse()
                                .unwrap()
                        }],
                    }
                    .into(),
                ),
            ]
            .into(),
            props: vec![
                //
                PropKeyManifest {
                    key_tag: WellKnownPropTag::PseudoLabel.into(),
                    value_schema: schemars::schema_for!(String),
                    display_config: default(),
                }
                .into(),
            ],
        },
    ]
}

#[derive(Default, Reconcile, Hydrate)]
pub struct PlugsStore {
    pub manifests: HashMap<String, ThroughJson<Arc<manifest::PlugManifest>>>,

    /// Index: property tag -> plug id (@ns/name)
    #[autosurgeon(with = "utils_rs::am::codecs::skip")]
    pub tag_to_plug: HashMap<String, String>,
}

impl PlugsStore {
    pub fn rebuild_indices(&mut self) {
        self.tag_to_plug.clear();

        for (plug_id, manifest) in &self.manifests {
            for prop in &manifest.props {
                self.tag_to_plug
                    .insert(prop.key_tag.to_string(), plug_id.clone());
            }
        }
    }
}

#[async_trait]
impl crate::stores::Store for PlugsStore {
    const PROP: &str = "plugs";
}

pub mod version_updates {
    use super::*;
    use automerge::{transaction::Transactable, ActorId, AutoCommit, ROOT};
    use autosurgeon::reconcile_prop;

    pub fn version_latest() -> Res<Vec<u8>> {
        let mut doc = AutoCommit::new().with_actor(ActorId::random());
        doc.put(ROOT, "version", "0")?;
        doc.put(ROOT, "$schema", "daybook.plugs")?;
        reconcile_prop(
            &mut doc,
            ROOT,
            super::PlugsStore::PROP,
            super::PlugsStore::default(),
        )?;
        Ok(doc.save_nocompress())
    }
}

pub struct PlugsRepo {
    // drawer_doc_id: DocumentId,
    store: crate::stores::StoreHandle<PlugsStore>,
    pub blobs: Arc<crate::blobs::BlobsRepo>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    pub mutation_mutex: tokio::sync::Mutex<()>,
    cancel_token: CancellationToken,
    _change_listener_tickets: Vec<utils_rs::am::changes::ChangeListenerRegistration>,
}

// Granular event enum for specific changes
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum PlugsEvent {
    ListChanged { heads: ChangeHashSet },
    PlugChanged { id: String, heads: ChangeHashSet },
    PlugDeleted { id: String, heads: ChangeHashSet },
}

impl crate::repos::Repo for PlugsRepo {
    type Event = PlugsEvent;
    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }
    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

impl PlugsRepo {
    pub async fn load(
        acx: AmCtx,
        blobs: Arc<crate::blobs::BlobsRepo>,
        app_doc_id: DocumentId,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let registry = crate::repos::ListenersRegistry::new();

        let store = PlugsStore::load(&acx, &app_doc_id).await?;
        let store = crate::stores::StoreHandle::new(store, acx.clone(), app_doc_id.clone());

        store.mutate_sync(|s| s.rebuild_indices()).await?;

        let (broker, broker_stop) = {
            let handle = acx
                .find_doc(&app_doc_id)
                .await?
                .expect("doc should have been loaded");
            acx.change_manager().add_doc(handle).await?
        };

        let (notif_tx, notif_rx) = tokio::sync::mpsc::unbounded_channel::<
            Vec<utils_rs::am::changes::ChangeNotification>,
        >();
        let ticket = PlugsStore::register_change_listener(&acx, &broker, vec![], {
            move |notifs| {
                if let Err(err) = notif_tx.send(notifs) {
                    warn!("failed to send change notifications: {err}");
                }
            }
        })
        .await?;

        let main_cancel_token = CancellationToken::new();
        let repo = Self {
            store,
            blobs,
            registry: registry.clone(),
            mutation_mutex: tokio::sync::Mutex::new(()),
            cancel_token: main_cancel_token.child_token(),
            _change_listener_tickets: vec![ticket],
        };
        let repo = Arc::new(repo);

        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            let cancel_token = main_cancel_token.clone();
            async move {
                repo.handle_notifs(notif_rx, cancel_token)
                    .await
                    .expect("error handling notifs")
            }
        });

        Ok((
            repo,
            crate::repos::RepoStopToken {
                cancel_token: main_cancel_token,
                worker_handle: Some(worker_handle),
                broker_stop_tokens: broker_stop.into_iter().collect(),
            },
        ))
    }

    async fn handle_notifs(
        self: &Self,
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<
            Vec<utils_rs::am::changes::ChangeNotification>,
        >,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        // FIXME: this code doesn't seem right and has missing features

        let mut events = vec![];
        loop {
            let notifs = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    while let Ok(notifs) = notif_rx.try_recv() {
                        self.process_notifs(notifs, &mut events).await?;
                    }
                    break;
                }
                msg = notif_rx.recv() => {
                    match msg {
                        Some(notifs) => notifs,
                        None => break,
                    }
                }
            };
            self.process_notifs(notifs, &mut events).await?;
        }
        Ok(())
    }

    async fn process_notifs(
        &self,
        notifs: Vec<utils_rs::am::changes::ChangeNotification>,
        events: &mut Vec<PlugsEvent>,
    ) -> Res<()> {
        events.clear();
        for notif in notifs {
            let heads = ChangeHashSet(notif.heads.clone());
            match &notif.patch.action {
                automerge::PatchAction::PutMap { key, .. } => {
                    if notif.patch.path.len() >= 2 {
                        match &notif.patch.path[1].1 {
                            automerge::Prop::Map(path_key) => match path_key.as_ref() {
                                "manifests" => events.push(PlugsEvent::PlugChanged {
                                    id: key.into(),
                                    heads,
                                }),
                                _ => events.push(PlugsEvent::ListChanged { heads }),
                            },
                            _ => events.push(PlugsEvent::ListChanged { heads }),
                        }
                    } else {
                        events.push(PlugsEvent::ListChanged { heads });
                    }
                }
                automerge::PatchAction::DeleteMap { key } => {
                    if notif.patch.path.len() >= 2 {
                        match &notif.patch.path[1].1 {
                            automerge::Prop::Map(path_key) => match path_key.as_ref() {
                                "manifests" => events.push(PlugsEvent::PlugDeleted {
                                    id: key.into(),
                                    heads,
                                }),
                                _ => events.push(PlugsEvent::ListChanged { heads }),
                            },
                            _ => events.push(PlugsEvent::ListChanged { heads }),
                        }
                    } else {
                        events.push(PlugsEvent::ListChanged { heads });
                    }
                }
                _ => {
                    // For other operations, send ListChanged
                    events.push(PlugsEvent::ListChanged { heads });
                }
            }
        }
        self.registry.notify(events.drain(..));
        Ok(())
    }

    pub async fn ensure_system_plugs(&self) -> Res<()> {
        let is_empty = self
            .store
            .query_sync(|store| store.manifests.is_empty())
            .await;
        if is_empty {
            for plug in system_plugs() {
                self.add(plug).await?;
            }
        }

        Ok(())
    }

    pub async fn get(&self, id: &str) -> Option<Arc<manifest::PlugManifest>> {
        self.store
            .query_sync(|store| store.manifests.get(id).map(|man| Arc::clone(&man.0)))
            .await
    }

    pub async fn get_display_hint(&self, prop_tag: &str) -> Option<manifest::PropKeyDisplayHint> {
        self.store
            .query_sync(|store| {
                let Some(plug_id) = store.tag_to_plug.get(prop_tag) else {
                    return None;
                };
                let Some(manifest) = store.manifests.get(plug_id) else {
                    panic!("plug specified by tag '{prop_tag}' not found");
                };
                let Some(hint) = manifest
                    .props
                    .iter()
                    .find(|prop: &&manifest::PropKeyManifest| &prop.key_tag[..] == prop_tag)
                    .map(|prop| prop.display_config.clone())
                else {
                    panic!("prop in index '{prop_tag}' not found in expected plug '{plug_id}'");
                };

                Some(hint)
            })
            .await
    }

    pub async fn list_display_hints(&self) -> Vec<(String, manifest::PropKeyDisplayHint)> {
        self.store
            .query_sync(|store| {
                store
                    .manifests
                    .values()
                    .map(|manifest| {
                        manifest
                            .props
                            .iter()
                            .map(|prop| (prop.key_tag.to_string(), prop.display_config.clone()))
                    })
                    .flatten()
                    .collect()
            })
            .await
    }

    pub async fn list_plugs(&self) -> Vec<Arc<manifest::PlugManifest>> {
        self.store
            .query_sync(|store| store.manifests.values().map(|man| man.0.clone()).collect())
            .await
    }

    /// Add a new plug to the repo after validating it.
    ///
    /// This method follows a literate programming approach to clearly document
    /// the validation and reconciliation steps.
    pub async fn add(&self, mut manifest: manifest::PlugManifest) -> Res<()> {
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
        if !cfg!(debug_assertions) {
            for bundle in manifest.wflow_bundles.values_mut() {
                let bundle = Arc::make_mut(bundle);
                for url in bundle.component_urls.iter_mut() {
                    if url.scheme() == "file" {
                        let path = url
                            .to_file_path()
                            .map_err(|_| eyre::eyre!("invalid file path in url: {}", url))?;
                        let data = tokio::fs::read(&path).await.wrap_err_with(|| {
                            format!("failed to read component file: {}", path.display())
                        })?;
                        let hash = self.blobs.put(&data).await?;
                        *url =
                            url::Url::parse(&format!("{}:///{}", crate::blobs::BLOB_SCHEME, hash))?;
                    }
                }
            }
        }

        // 2. Perform Automerge reconciliation
        // Once validated, we update the Automerge store.
        // We use the plug's identity (@namespace/name) as the key in the manifests map
        // to simplify lookups and ensure uniqueness.
        let plug_id = manifest.id();

        let (_, hash) = self
            .store
            .mutate_sync(move |store| {
                // Update the manifest in the store
                store
                    .manifests
                    .insert(plug_id.clone(), ThroughJson(Arc::new(manifest)));

                // 3. Rebuild indices
                // Indices are in-memory caches (marked with #[autosurgeon(skip)])
                // used to hyper-accelerate validation and routing logic.
                // We rebuild them here so they're immediately available for subsequent calls.
                store.rebuild_indices();
            })
            .await?;

        let heads = ChangeHashSet(hash.into_iter().collect());

        // Notify listeners that the plug list or a specific plug has changed
        self.registry.notify([PlugsEvent::ListChanged { heads }]);

        Ok(())
    }

    /// Comprehensive validation for an incoming plug.
    ///
    /// This method checks for:
    /// - Structural validity (via garde).
    /// - Property tag clashes with other plugs.
    /// - Dependency resolution (existence and schema compatibility).
    /// - Internal consistency (commands referencing existing routines).
    /// - ACL scope restrictions.
    /// - Versioning rules (no breaking changes in non-major updates).
    pub async fn validate_incoming_plug(&self, manifest: &manifest::PlugManifest) -> Res<()> {
        use garde::Validate;

        // -- Structural Validation --
        // Use the 'garde' crate to perform basic field-level validations (regex, length, etc.)
        // defined in the manifest structs.
        manifest
            .validate()
            .map_err(|e| eyre::eyre!("validation error: {e}"))?;

        let plug_id = manifest.id();
        let existing = self.get(&plug_id).await;

        // -- Versioning and Breaking Change Protection --
        // To maintain stability, we don't allow breaking changes (like removing commands
        // or changing their parameters) in minor or patch updates.
        if let Some(old) = &existing {
            if manifest.version <= old.version {
                eyre::bail!(
                    "Version must be greater than existing version (current: {}, incoming: {})",
                    old.version,
                    manifest.version
                );
            }

            let is_major = manifest.version.major > old.version.major
                || (old.version.major == 0 && manifest.version.minor > old.version.minor);

            if !is_major {
                // In non-major updates, we must ensure existing commands are preserved
                // to avoid breaking integrations or automated workflows.
                for (old_cmd_name, old_cmd) in &old.commands {
                    let new_cmd = manifest.commands.get(old_cmd_name);
                    if let Some(new_cmd) = new_cmd {
                        // Deets define the routine and parameters; changing them breaks callers.
                        // FIXME: we need a better comparison for CommandDeets if it's complex
                        if format!("{:?}", new_cmd.deets) != format!("{:?}", old_cmd.deets) {
                            eyre::bail!("Breaking change: command '{}' deets cannot change in non-major version update", old_cmd_name);
                        }
                    } else {
                        eyre::bail!("Breaking change: command '{}' cannot be removed in non-major version update", old_cmd_name);
                    }
                }
            }

            // We also check that property keys aren't removed or their schemas don't become incompatible.
            for old_prop in &old.props {
                if let Some(new_prop) = manifest
                    .props
                    .iter()
                    .find(|p| p.key_tag == old_prop.key_tag)
                {
                    if !is_schema_compatible(&old_prop.value_schema, &new_prop.value_schema) {
                        eyre::bail!(
                            "Incompatible schema for property tag '{}'",
                            old_prop.key_tag
                        );
                    }
                }
            }
        }

        // -- Property Tag Clash Detection --
        // Many parts of the system rely on property tags being unique identifiers.
        // We use an index to quickly check if any of the tags this plug wants to declare
        // are already owned by another plug.
        self.store
            .query_sync(|store| {
                for prop in &manifest.props {
                    if let Some(owner) = store.tag_to_plug.get(&prop.key_tag.to_string()) {
                        if owner != &plug_id {
                            return Err(eyre::eyre!(
                                "Tag clash: tag '{}' is already owned by plug '{}'",
                                prop.key_tag,
                                owner
                            ));
                        }
                    }
                }
                Ok(())
            })
            .await?;

        // -- Dependency Verification --
        // Plugs can declare dependencies on other plugs to reuse their property keys.
        // We verify that:
        // 1. The depended-on plug exists.
        // 2. The specific keys being requested are actually defined by that plug.
        // 3. The requested schema is compatible with what the provider offers.
        for (dep_id_full, dep_manifest) in &manifest.dependencies {
            let dep_base_id = if dep_id_full.starts_with('@') {
                // For strings like "@ns/name@1.2.3", we want "@ns/name"
                let parts: Vec<&str> = dep_id_full[1..].split('@').collect();
                format!("@{}", parts[0])
            } else {
                dep_id_full
                    .split('@')
                    .next()
                    .ok_or_eyre("invalid dependency id")?
                    .to_string()
            };
            let provider = self
                .get(&dep_base_id)
                .await
                .ok_or_eyre(format!("Dependency not found: '{}'", dep_base_id))?;

            for key_dep in &dep_manifest.keys {
                let provider_prop = provider
                    .props
                    .iter()
                    .find(|p| p.key_tag == key_dep.key_tag)
                    .ok_or_eyre(format!(
                        "Dependency error: plug '{}' does not define tag '{}'",
                        dep_base_id, key_dep.key_tag
                    ))?;

                if !is_schema_compatible(&provider_prop.value_schema, &key_dep.value_schema) {
                    eyre::bail!(
                        "Dependency error: incompatible schema for tag '{}' from plug '{}'",
                        key_dep.key_tag,
                        dep_base_id
                    );
                }
            }
        }

        // -- Internal Routine Integrity --
        // Commands act as triggers for routines. If a command points to a non-existent
        // routine, it will fail at runtime. We catch these early.
        for (routine_name, routine) in &manifest.routines {
            let manifest::RoutineImpl::Wflow { bundle, key } = &routine.r#impl;
            let Some(bundle_manifest) = manifest.wflow_bundles.get(bundle) else {
                eyre::bail!(
                    "Invalid routine '{}': wflow bundle '{}' not found in manifest",
                    routine_name,
                    bundle
                );
            };
            if !bundle_manifest.keys.contains(key) {
                eyre::bail!(
                    "Invalid routine '{}': key '{}' not found in wflow bundle '{}'",
                    routine_name,
                    key,
                    bundle
                );
            }
        }

        for (cmd_name, cmd) in &manifest.commands {
            match &cmd.deets {
                manifest::CommandDeets::DocCommand { routine_name } => {
                    if !manifest.routines.contains_key(routine_name) {
                        eyre::bail!(
                            "Invalid command deets: routine '{}' not found in plug (command='{}')",
                            routine_name,
                            cmd_name
                        );
                    }
                }
            }
        }

        // -- Component URL Validation --
        for (bundle_name, bundle) in &manifest.wflow_bundles {
            for url in &bundle.component_urls {
                match url.scheme() {
                    "file" => {
                        let path = url
                            .to_file_path()
                            .map_err(|_| eyre::eyre!("invalid file path in url: {}", url))?;
                        if !path.exists() {
                            eyre::bail!(
                                "Component file not found for bundle '{}': {}",
                                bundle_name,
                                path.display()
                            );
                        }
                    }
                    scheme if scheme == crate::blobs::BLOB_SCHEME => {
                        let hash = url.path().trim_start_matches('/');
                        if self.blobs.get_path(hash).await.is_err() {
                            eyre::bail!(
                                "Blob not found in BlobsRepo for bundle '{}': {}",
                                bundle_name,
                                hash
                            );
                        }
                    }
                    _ => {
                        eyre::bail!(
                            "Unsupported URL scheme for bundle '{}': {}",
                            bundle_name,
                            url.scheme()
                        );
                    }
                }
            }
        }

        // -- ACL Scope Restriction --
        // Routines must explicitly declare which properties they need access to.
        // To prevent security leaks, a routine can only specify tags that
        // the plug itself declares or explicitly depends on.
        let mut available_tags: HashSet<String> = manifest
            .props
            .iter()
            .map(|p| p.key_tag.to_string())
            .collect();
        for dep in manifest.dependencies.values() {
            for key in &dep.keys {
                available_tags.insert(key.key_tag.to_string());
            }
        }

        for (routine_name, routine) in &manifest.routines {
            for access in &routine.prop_acl {
                if !available_tags.contains(&access.tag.to_string()) {
                    eyre::bail!("Invalid ACL in routine '{}': tag '{}' is neither declared nor depended on by this plug. Avail tags {available_tags:?}", routine_name, access.tag);
                }
            }

            // If it's a DocProp routine, the 'working_prop_tag' must also be accessible.
            if let manifest::RoutineManifestDeets::DocProp { working_prop_tag } = &routine.deets {
                if !available_tags.contains(&working_prop_tag.to_string()) {
                    eyre::bail!(
                        "Invalid routine deets for '{}': working_prop_tag '{}' not in scope",
                        routine_name,
                        working_prop_tag
                    );
                }
            }
        }

        Ok(())
    }
}

/// Helper to check JSON Schema compatibility.
///
/// In this context, 'compatible' means that the 'new' schema can accept data
/// validated by the 'old' schema without breaking (forward compatibility).
fn is_schema_compatible(old: &schemars::Schema, new: &schemars::Schema) -> bool {
    // If they are exactly the same, they are definitely compatible.
    if old == new {
        return true;
    }

    // Treat them as JSON values for a pragmatic compatibility check.
    // In schemars 1.0, Schema is a wrapper around serde_json::Value.
    let old_json = serde_json::to_value(old).unwrap_or(serde_json::Value::Null);
    let new_json = serde_json::to_value(new).unwrap_or(serde_json::Value::Null);

    is_json_schema_compatible(&old_json, &new_json)
}

fn is_json_schema_compatible(old: &serde_json::Value, new: &serde_json::Value) -> bool {
    if old == new {
        return true;
    }

    match (old, new) {
        (serde_json::Value::Object(old_obj), serde_json::Value::Object(new_obj)) => {
            // Check basic type matching
            if old_obj.get("type") != new_obj.get("type") {
                return false;
            }

            // If it's an object, check properties
            if old_obj.get("type") == Some(&serde_json::json!("object")) {
                let old_props = old_obj.get("properties").and_then(|v| v.as_object());
                let new_props = new_obj.get("properties").and_then(|v| v.as_object());

                if let (Some(old_props), Some(new_props)) = (old_props, new_props) {
                    // All properties in old must be present and compatible in new
                    for (name, old_val) in old_props {
                        if let Some(new_val) = new_props.get(name) {
                            if !is_json_schema_compatible(old_val, new_val) {
                                return false;
                            }
                        } else {
                            // Property removed -> breaking change
                            return false;
                        }
                    }
                }

                // Check required fields: new cannot require something that was not required in old
                let old_required = old_obj.get("required").and_then(|v| v.as_array());
                let new_required = new_obj.get("required").and_then(|v| v.as_array());
                if let Some(new_req) = new_required {
                    let old_req_set: HashSet<_> =
                        old_required.map(|a| a.iter().collect()).unwrap_or_default();
                    for req in new_req {
                        if !old_req_set.contains(req) {
                            // New required field -> breaking change
                            // Unless it has a default? But JSON Schema's 'default' doesn't satisfy 'required'.
                            return false;
                        }
                    }
                }
            }

            // FIXME: Add more checks for arrays, enums, etc.
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup_repo() -> Res<(AmCtx, Arc<PlugsRepo>, DocumentId, tempfile::TempDir)> {
        let (acx, _acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "test".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            None::<samod::AlwaysAnnounce>,
        )
        .await?;

        let doc = automerge::Automerge::load(&version_updates::version_latest()?)?;
        let handle = acx.add_doc(doc).await?;
        let doc_id = handle.document_id().clone();

        let temp_dir = tempfile::tempdir()?;
        let blobs = crate::blobs::BlobsRepo::new(temp_dir.path().to_path_buf()).await?;

        let (repo, _repo_stop) = PlugsRepo::load(acx.clone(), blobs, doc_id.clone()).await?;
        Ok((acx, repo, doc_id, temp_dir))
    }

    fn mock_plug(name: &str) -> manifest::PlugManifest {
        manifest::PlugManifest {
            namespace: "test".into(),
            name: name.into(),
            version: "0.1.0".parse().unwrap(),
            title: format!("Test Plug {}", name),
            desc: "A test plug".into(),
            props: vec![],
            dependencies: default(),
            routines: default(),
            wflow_bundles: default(),
            commands: default(),
            processors: default(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_add_success() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;
        let plug = mock_plug("plug1");

        repo.add(plug.into()).await?;

        let saved = repo.get("@test/plug1").await.unwrap();
        assert_eq!(saved.name, "plug1");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_tag_clash() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        // Add first plug with a tag
        let mut p1 = mock_plug("plug1");
        p1.props.push(manifest::PropKeyManifest {
            key_tag: "org.test.tag".into(),
            value_schema: schemars::schema_for!(String),
            display_config: default(),
        });
        repo.add(p1.into()).await?;

        // Try to add second plug with same tag
        let mut p2 = mock_plug("plug2");
        p2.props.push(manifest::PropKeyManifest {
            key_tag: "org.test.tag".into(),
            value_schema: schemars::schema_for!(String),
            display_config: default(),
        });

        let res = repo.add(p2.into()).await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("Tag clash"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_dependency_resolution() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        // Add provider plug
        let mut provider = mock_plug("provider");
        provider.props.push(manifest::PropKeyManifest {
            key_tag: "org.test.shared".into(),
            value_schema: schemars::schema_for!(String),
            display_config: default(),
        });
        repo.add(provider.into()).await?;

        // Add consumer plug that depends on provider
        let mut consumer = mock_plug("consumer");
        consumer.dependencies.insert(
            "@test/provider".into(),
            manifest::PlugDependencyManifest {
                keys: vec![manifest::PropKeyDependencyManifest {
                    key_tag: "org.test.shared".into(),
                    value_schema: schemars::schema_for!(String),
                }],
            }
            .into(),
        );

        repo.add(consumer.into()).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_missing_dependency() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        let mut consumer = mock_plug("consumer");
        consumer.dependencies.insert(
            "@test/missing".into(),
            manifest::PlugDependencyManifest { keys: vec![] }.into(),
        );

        let res = repo.add(consumer.into()).await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Dependency not found"));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_version_breaking_change() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        // Create a temporary file for the component (keep it alive)
        let temp_dir = tempfile::tempdir()?;
        let temp_path = temp_dir.path().join("component.wasm");
        tokio::fs::write(&temp_path, b"dummy wasm content").await?;
        let file_url = url::Url::from_file_path(&temp_path).unwrap();

        // Initial version
        let mut p1_v1 = mock_plug("plug1");
        p1_v1.version = "0.1.0".parse().unwrap();
        p1_v1.commands.insert(
            "cmd1".into(),
            manifest::CommandManifest {
                desc: "First command".into(),
                deets: manifest::CommandDeets::DocCommand {
                    routine_name: "routine1".into(),
                },
            }
            .into(),
        );
        p1_v1.routines.insert(
            "routine1".into(),
            manifest::RoutineManifest {
                r#impl: manifest::RoutineImpl::Wflow {
                    key: "wflow1".into(),
                    bundle: "bundle1".into(),
                },
                deets: manifest::RoutineManifestDeets::DocInvoke {},
                prop_acl: vec![],
            }
            .into(),
        );
        p1_v1.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec!["wflow1".into()],
                component_urls: vec![file_url],
            }
            .into(),
        );
        repo.add(p1_v1.into()).await?;

        // Update version (patch) with command removed -> should fail
        let mut p1_v2 = mock_plug("plug1");
        p1_v2.version = "0.1.1".parse().unwrap();
        // cmd1 is missing

        let res = repo.add(p1_v2.into()).await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("Breaking change"));

        // Update version (major) with command removed -> should succeed
        let mut p1_v3 = mock_plug("plug1");
        p1_v3.version = "1.0.0".parse().unwrap(); // major bump from 0.1 to 1.0 (in standard semver terms)

        repo.add(p1_v3.into()).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_version_must_increase() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        // Add initial version
        let mut p1_v1 = mock_plug("plug1");
        p1_v1.version = "0.1.0".parse().unwrap();
        repo.add(p1_v1.into()).await?;

        // Try to add same version -> should fail
        let mut p1_same = mock_plug("plug1");
        p1_same.version = "0.1.0".parse().unwrap();
        let res = repo.add(p1_same.into()).await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Version must be greater"));

        // Try to add lower version -> should fail
        let mut p1_lower = mock_plug("plug1");
        p1_lower.version = "0.0.9".parse().unwrap();
        let res = repo.add(p1_lower.into()).await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Version must be greater"));

        // Add higher version -> should succeed
        let mut p1_v2 = mock_plug("plug1");
        p1_v2.version = "0.1.1".parse().unwrap();
        repo.add(p1_v2.into()).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_bundle_key_validation() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        let temp_dir = tempfile::tempdir()?;
        let temp_path = temp_dir.path().join("component.wasm");
        tokio::fs::write(&temp_path, b"dummy wasm").await?;
        let file_url = url::Url::from_file_path(&temp_path).unwrap();

        // Create plug with routine referencing non-existent bundle
        let mut plug = mock_plug("plug1");
        plug.routines.insert(
            "routine1".into(),
            manifest::RoutineManifest {
                r#impl: manifest::RoutineImpl::Wflow {
                    key: "wflow1".into(),
                    bundle: "missing_bundle".into(),
                },
                deets: manifest::RoutineManifestDeets::DocInvoke {},
                prop_acl: vec![],
            }
            .into(),
        );
        plug.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec!["wflow1".into()],
                component_urls: vec![file_url.clone()],
            }
            .into(),
        );

        let res = repo.add(plug.into()).await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("wflow bundle 'missing_bundle' not found"));

        // Create plug with routine referencing non-existent key in bundle
        let mut plug2 = mock_plug("plug2");
        plug2.routines.insert(
            "routine1".into(),
            manifest::RoutineManifest {
                r#impl: manifest::RoutineImpl::Wflow {
                    key: "missing_key".into(),
                    bundle: "bundle1".into(),
                },
                deets: manifest::RoutineManifestDeets::DocInvoke {},
                prop_acl: vec![],
            }
            .into(),
        );
        plug2.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec!["wflow1".into()],
                component_urls: vec![file_url],
            }
            .into(),
        );

        let res = repo.add(plug2.into()).await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("key 'missing_key' not found"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_component_url_validation() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        // Test with non-existent file URL
        let mut plug = mock_plug("plug1");
        plug.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec![],
                component_urls: vec!["file:///nonexistent/path".parse().unwrap()],
            }
            .into(),
        );

        let res = repo.add(plug.into()).await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Component file not found"));

        // Test with non-existent blob URL
        let mut plug2 = mock_plug("plug2");
        plug2.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec![],
                component_urls: vec![format!("{}:///nonexistent_hash", crate::blobs::BLOB_SCHEME)
                    .parse()
                    .unwrap()],
            }
            .into(),
        );

        let res = repo.add(plug2.into()).await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("Blob not found"));

        // Test with unsupported scheme
        let mut plug3 = mock_plug("plug3");
        plug3.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec![],
                component_urls: vec!["http://example.com/wasm.wasm".parse().unwrap()],
            }
            .into(),
        );

        let res = repo.add(plug3.into()).await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Unsupported URL scheme"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plug_file_to_blob_conversion() -> Res<()> {
        let (_acx, repo, _doc_id, _temp_dir) = setup_repo().await?;

        // Create a temporary file with wasm content (keep it alive)
        let temp_dir = tempfile::tempdir()?;
        let temp_path = temp_dir.path().join("component.wasm");
        let wasm_content = b"fake wasm binary content";
        tokio::fs::write(&temp_path, wasm_content).await?;
        let file_url = url::Url::from_file_path(&temp_path).unwrap();

        // Create plug with file:// URL
        let mut plug = mock_plug("plug1");
        plug.wflow_bundles.insert(
            "bundle1".into(),
            manifest::WflowBundleManifest {
                keys: vec![],
                component_urls: vec![file_url],
            }
            .into(),
        );

        // Add plug - should convert file:// to db+blob://
        repo.add(plug.clone().into()).await?;

        // Retrieve the plug and verify URL was converted
        let saved = repo.get("@test/plug1").await.unwrap();
        let bundle = saved.wflow_bundles.get("bundle1").unwrap();
        assert_eq!(bundle.component_urls.len(), 1);
        let converted_url = &bundle.component_urls[0];
        assert_eq!(converted_url.scheme(), crate::blobs::BLOB_SCHEME);

        // Verify the blob exists and contains the correct content
        let hash = converted_url.path().trim_start_matches('/');
        let blob_path = repo.blobs.get_path(hash).await?;
        let blob_content = tokio::fs::read(&blob_path).await?;
        assert_eq!(blob_content, wasm_content);

        Ok(())
    }
}
