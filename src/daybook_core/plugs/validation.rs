use super::*;

impl PlugsRepo {
    /// Comprehensive validation for an incoming plug (authoring path).
    ///
    /// This method checks for:
    /// - Structural validity (via garde).
    /// - Property tag clashes with other plugs.
    /// - Dependency resolution (existence and schema compatibility).
    /// - Internal consistency (commands referencing existing routines).
    /// - ACL scope restrictions.
    /// - Versioning rules (no breaking changes in non-major updates).
    ///
    /// The version/compat baseline is the last VALID version in the derived
    /// cache (the authoring path is strict). The remote manifest-change gate
    /// uses [`validate_structure`] + a last-ENABLED-version baseline instead.
    pub async fn validate_incoming_plug(&self, manifest: &manifest::PlugManifest) -> Res<()> {
        self.validate_structure(manifest).await?;
        let plug_id = manifest.id();
        let existing = self.get_known(&plug_id).await;
        if let Some(old) = &existing {
            if manifest.version <= old.version {
                eyre::bail!(
                    "Version must be greater than existing version (current: {}, incoming: {})",
                    old.version,
                    manifest.version
                );
            }
            check_breaking_changes(manifest, old)?;
        }
        Ok(())
    }

    /// Structural validity + tag clashes + deps + ACLs + components — no
    /// version baseline. Used by the record gate (remote manifest changes)
    /// and by activation gates; the compat diff is applied separately
    /// against the last enabled version.
    pub async fn validate_structure(&self, manifest: &manifest::PlugManifest) -> Res<()> {
        use garde::Validate;

        // -- Structural Validation --
        // Use the 'garde' crate to perform basic field-level validations (regex, length, etc.)
        // defined in the manifest structs.
        manifest
            .validate()
            .map_err(|err| eyre::eyre!("validation error: {err}"))?;

        let mut seen_facet_tags = HashSet::new();
        for facet_manifest in &manifest.facets {
            let facet_tag = facet_manifest.key_tag.to_string();
            if !seen_facet_tags.insert(facet_tag.clone()) {
                eyre::bail!("duplicate facet tag '{}' in plug manifest", facet_tag);
            }

            validate_facet_reference_manifests(
                &facet_manifest.key_tag.to_string(),
                &facet_manifest.value_schema,
                &facet_manifest.references,
            )?;
        }

        let dependency_base_ids: HashSet<String> = manifest
            .dependencies
            .keys()
            .map(|dep_id_full| parse_dep_base_id(dep_id_full))
            .collect::<Res<HashSet<_>>>()?;
        let mut cached_view_target_manifests: HashMap<String, Arc<manifest::PlugManifest>> =
            HashMap::new();

        // -- Property Tag Clash Detection --
        // Many parts of the system rely on property tags being unique identifiers.
        // We use an index to quickly check if any of the tags this plug wants to declare
        // are already owned by another plug.
        {
            let cache = self.cache.lock().await;
            for prop in &manifest.facets {
                if let Some(owner) = cache.tag_to_plug.get(&prop.key_tag.to_string())
                    && owner != &plug_id
                {
                    return Err(eyre::eyre!(
                        "Tag clash: tag '{}' is already owned by plug '{}'",
                        prop.key_tag,
                        owner
                    ));
                }
            }
        }

        // -- Dependency Verification --
        // Plugs can declare dependencies on other plugs to reuse their property keys.
        // We verify that:
        // 1. The depended-on plug exists.
        // 2. The specific keys being requested are actually defined by that plug.
        // 3. The requested schema is compatible with what the provider offers.
        for (dep_id_full, dep_manifest) in &manifest.dependencies {
            let dep_base_id = parse_dep_base_id(dep_id_full)?;
            let provider = self
                .get_known(&dep_base_id)
                .await
                .ok_or_eyre(format!("Dependency not found: '{dep_base_id}'"))?;

            for key_dep in &dep_manifest.keys {
                let provider_prop = provider
                    .facets
                    .iter()
                    .find(|prop| prop.key_tag == key_dep.key_tag)
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

            for local_state_dep in &dep_manifest.local_states {
                let provider_state_kind = provider
                    .local_states
                    .get(&local_state_dep.local_state_key)
                    .ok_or_eyre(format!(
                        "Dependency error: plug '{}' does not define local_state '{}'",
                        dep_base_id, local_state_dep.local_state_key
                    ))?;
                if **provider_state_kind != local_state_dep.state_kind {
                    eyre::bail!(
                        "Dependency error: incompatible local_state kind for '{}' from plug '{}'",
                        local_state_dep.local_state_key,
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
        for (init_name, init_manifest) in &manifest.inits {
            match &init_manifest.deets {
                manifest::InitDeets::InvokeRoutine { routine_name } => {
                    if !manifest.routines.contains_key(routine_name) {
                        eyre::bail!(
                            "Invalid init deets: routine '{}' not found in plug (init='{}')",
                            routine_name,
                            init_name
                        );
                    }
                }
            }
        }

        for (processor_name, processor_manifest) in &manifest.processors {
            match &processor_manifest.deets {
                manifest::ProcessorDeets::DocProcessor {
                    routine_name,
                    predicate: _,
                    event_predicate: _,
                } => {
                    if !manifest.routines.contains_key(routine_name) {
                        eyre::bail!(
                            "Invalid processor deets: routine '{}' not found in plug (processor='{}')",
                            routine_name,
                            processor_name
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
                    "static" => {
                        eyre::bail!("Unrecognized static component_url: {url}");
                    }
                    scheme if scheme == crate::blobs::BLOB_SCHEME => {
                        let hash = url.path().trim_start_matches('/');
                        let blob_id = match hash.parse::<crate::blobs::BlobId>() {
                            Ok(value) => value,
                            Err(_) => {
                                eyre::bail!(
                                    "Blob not found in BlobsRepo for bundle {bundle_name:?}: {hash:?}",
                                );
                            }
                        };
                        if self.blobs.get_path(blob_id).await.is_err() {
                            eyre::bail!(
                                "Blob not found in BlobsRepo for bundle {bundle_name:?}: {hash:?}",
                            );
                        }
                    }
                    _ => {
                        eyre::bail!(
                            "Unsupported URL scheme for bundle {bundle_name:?}: {}",
                            url.scheme()
                        );
                    }
                }
            }
        }

        // -- View Validation --
        // Stateless wasm views must point at a declared wflow bundle and export the canonical
        // stateless-view entrypoint (`render-facet-view`). Otherwise the plug imports cleanly but
        // fails later at render time (see rt::render_facet_view).
        for (view_name, view_manifest) in &manifest.views {
            let manifest::ViewProviderManifest::StatelessWasm { bundle, export } =
                &view_manifest.provider;
            let Some(_) = manifest.wflow_bundles.get(bundle.as_str()) else {
                eyre::bail!(
                    "Invalid view '{}': wflow bundle '{}' not found in manifest",
                    view_name,
                    bundle
                );
            };
            if export.as_str() != "render-facet-view" {
                eyre::bail!(
                    "Invalid view '{}': stateless wasm export '{}' is not the supported 'render-facet-view' entrypoint",
                    view_name,
                    export
                );
            }
        }

        // -- ACL Scope Restriction --
        // Routines must explicitly declare which properties they need access to.
        // To prevent security leaks, a routine can only specify tags that
        // the plug itself declares or explicitly depends on.
        let mut available_tags: HashSet<String> = manifest
            .facets
            .iter()
            .map(|prop| prop.key_tag.to_string())
            .collect();
        for dep in manifest.dependencies.values() {
            for key in &dep.keys {
                available_tags.insert(key.key_tag.to_string());
            }
        }
        let mut cached_command_target_manifests: HashMap<String, Arc<manifest::PlugManifest>> =
            HashMap::new();
        let mut available_local_states: HashSet<(String, String)> = manifest
            .local_states
            .keys()
            .map(|key| (plug_id.clone(), key.to_string()))
            .collect();
        for (dep_id_full, dep_manifest) in &manifest.dependencies {
            let dep_base_id = parse_dep_base_id(dep_id_full)?;
            for local_state in &dep_manifest.local_states {
                available_local_states
                    .insert((dep_base_id.clone(), local_state.local_state_key.to_string()));
            }
        }

        for facet_manifest in &manifest.facets {
            let facet_name = facet_manifest.key_tag.to_string();
            if let manifest::FacetDisplayDeets::CustomView { view, .. } =
                &facet_manifest.display_config.deets
            {
                match view.plug_id.as_deref() {
                    None => {
                        if !manifest.views.contains_key(view.view_key.as_str()) {
                            eyre::bail!(
                                "Invalid display_config in facet '{}': view '{}' not found in this plug",
                                facet_name,
                                view.view_key
                            );
                        }
                    }
                    // An explicit self-reference (the plug's own id) is equivalent to `None`:
                    // validate against the incoming manifest rather than treating the plug as one
                    // of its own dependencies and resolving it through the stored copy.
                    Some(view_plug_id) if view_plug_id == plug_id.as_str() => {
                        if !manifest.views.contains_key(view.view_key.as_str()) {
                            eyre::bail!(
                                "Invalid display_config in facet '{}': view '{}' not found in this plug",
                                facet_name,
                                view.view_key
                            );
                        }
                    }
                    Some(view_plug_id) => {
                        if !dependency_base_ids.contains(view_plug_id) {
                            eyre::bail!(
                                "Invalid display_config in facet '{}': view provider plug '{}' is neither this plug nor a declared dependency",
                                facet_name,
                                view_plug_id
                            );
                        }
                        let target_manifest = if let Some(cached) =
                            cached_view_target_manifests.get(view_plug_id)
                        {
                            Arc::clone(cached)
                        } else {
                            let loaded = self.get_known(view_plug_id).await.ok_or_else(|| {
                                    ferr!(
                                        "Invalid display_config in facet '{}': view provider plug '{}' not found",
                                        facet_name,
                                        view_plug_id
                                    )
                                })?;
                            cached_view_target_manifests
                                .insert(view_plug_id.to_string(), Arc::clone(&loaded));
                            loaded
                        };
                        if !target_manifest.views.contains_key(view.view_key.as_str()) {
                            eyre::bail!(
                                "Invalid display_config in facet '{}': view '{}' not found in view provider plug '{}'",
                                facet_name,
                                view.view_key,
                                view_plug_id
                            );
                        }
                    }
                }
            }
        }

        for (routine_name, routine) in &manifest.routines {
            for access in routine.facet_acl() {
                if !available_tags.contains(&access.tag.to_string()) {
                    eyre::bail!(
                        "Invalid ACL in routine '{}': tag '{}' is neither declared nor depended on by this plug. Avail tags {available_tags:?}",
                        routine_name,
                        access.tag
                    );
                }
            }
            for access in routine.config_facet_acl() {
                let owner_plug_id = access.owner_plug_id.as_deref().unwrap_or(&plug_id);
                if owner_plug_id != plug_id && !dependency_base_ids.contains(owner_plug_id) {
                    eyre::bail!(
                        "Invalid config_facet_acl in routine '{}': owner plug '{}' is neither this plug nor a declared dependency",
                        routine_name,
                        owner_plug_id
                    );
                }
                if !available_tags.contains(&access.tag.to_string()) {
                    eyre::bail!(
                        "Invalid config_facet_acl in routine '{}': tag '{}' is neither declared nor depended on by this plug. Avail tags {available_tags:?}",
                        routine_name,
                        access.tag
                    );
                }
            }
            for access in &routine.local_state_acl {
                if !available_local_states
                    .contains(&(access.plug_id.clone(), access.local_state_key.to_string()))
                {
                    eyre::bail!(
                        "Invalid local_state ACL in routine '{}': '{}:{}' is neither declared nor depended on by this plug",
                        routine_name,
                        access.plug_id,
                        access.local_state_key
                    );
                }
            }
            for target_command_url in routine.command_invoke_acl() {
                let parsed_target =
                    daybook_pdk::parse_command_url(target_command_url).map_err(|err| {
                        eyre::eyre!(
                            "Invalid command_invoke_acl in routine '{}': url '{}' is invalid: {}",
                            routine_name,
                            target_command_url,
                            err
                        )
                    })?;
                if parsed_target.plug_id != plug_id
                    && !dependency_base_ids.contains(&parsed_target.plug_id)
                {
                    eyre::bail!(
                        "Invalid command_invoke_acl in routine '{}': target plug '{}' is neither this plug nor a declared dependency",
                        routine_name,
                        parsed_target.plug_id
                    );
                }
                let command_exists = if parsed_target.plug_id == plug_id {
                    manifest
                        .commands
                        .contains_key(parsed_target.command_name.as_str())
                } else {
                    let target_manifest = if let Some(cached) =
                        cached_command_target_manifests.get(&parsed_target.plug_id)
                    {
                        Arc::clone(cached)
                    } else {
                        let loaded = self
                            .get_known(&parsed_target.plug_id)
                            .await
                            .ok_or_else(|| {
                                ferr!(
                                    "Invalid command_invoke_acl in routine '{}': target plug '{}' not found",
                                    routine_name,
                                    parsed_target.plug_id
                                )
                            })?;
                        cached_command_target_manifests
                            .insert(parsed_target.plug_id.clone(), Arc::clone(&loaded));
                        loaded
                    };
                    target_manifest
                        .commands
                        .contains_key(parsed_target.command_name.as_str())
                };
                if !command_exists {
                    eyre::bail!(
                        "Invalid command_invoke_acl in routine '{}': target command '{}/{}' not found",
                        routine_name,
                        parsed_target.plug_id,
                        parsed_target.command_name
                    );
                }
            }

            // Validate all tags referenced by doc_acls are in scope.
            for tag in routine.referenced_tags() {
                if !available_tags.contains(&tag.to_string()) {
                    eyre::bail!(
                        "Invalid routine ACL for '{}': tag '{}' is neither declared nor depended on by this plug. Avail tags {available_tags:?}",
                        routine_name,
                        tag
                    );
                }
            }
        }

        for (processor_name, processor_manifest) in &manifest.processors {
            match &processor_manifest.deets {
                manifest::ProcessorDeets::DocProcessor {
                    predicate,
                    event_predicate,
                    routine_name: _,
                } => {
                    for referenced_tag in predicate.referenced_tags() {
                        if !available_tags.contains(&referenced_tag.to_string()) {
                            eyre::bail!(
                                "Invalid processor predicate in '{}': tag '{}' is neither declared nor depended on by this plug. Avail tags {available_tags:?}",
                                processor_name,
                                referenced_tag
                            );
                        }
                    }
                    let mut read_tags = HashSet::new();
                    let mut read_keys = HashSet::new();
                    event_predicate
                        .doc_change_predicate
                        .append_referenced_facet_scope(&mut read_tags, &mut read_keys);
                    for referenced_tag in read_tags {
                        if !available_tags.contains(&referenced_tag) {
                            eyre::bail!(
                                "Invalid processor event predicate in '{}': tag '{}' is neither declared nor depended on by this plug. Avail tags {available_tags:?}",
                                processor_name,
                                referenced_tag
                            );
                        }
                    }
                    for referenced_key in read_keys {
                        let referenced_tag = referenced_key.tag.to_string();
                        if !available_tags.contains(&referenced_tag) {
                            eyre::bail!(
                                "Invalid processor event predicate in '{}': tag '{}' (from key '{}') is neither declared nor depended on by this plug. Avail tags {available_tags:?}",
                                processor_name,
                                referenced_tag,
                                referenced_key
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

fn parse_dep_base_id(dep_id: &str) -> Res<String> {
    if dep_id.starts_with('@') {
        let without_prefix = dep_id
            .strip_prefix('@')
            .ok_or_else(|| eyre::eyre!("invalid dependency id: {dep_id}"))?;
        let base = without_prefix
            .split('@')
            .next()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| eyre::eyre!("invalid dependency id: {dep_id}"))?;
        Ok(format!("@{base}"))
    } else {
        let base = dep_id
            .split('@')
            .next()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| eyre::eyre!("invalid dependency id: {dep_id}"))?;
        Ok(base.to_string())
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
                let old_props = old_obj
                    .get("properties")
                    .and_then(|value| value.as_object());
                let new_props = new_obj
                    .get("properties")
                    .and_then(|value| value.as_object());

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
                let old_required = old_obj.get("required").and_then(|value| value.as_array());
                let new_required = new_obj.get("required").and_then(|value| value.as_array());
                if let Some(new_req) = new_required {
                    let old_req_set: HashSet<_> = old_required
                        .map(|array| array.iter().collect())
                        .unwrap_or_default();
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

fn validate_facet_reference_manifests(
    facet_tag: &str,
    value_schema: &schemars::Schema,
    references: &[manifest::FacetReferenceManifest],
) -> Res<()> {
    let schema_json = serde_json::to_value(value_schema)?;
    for reference_manifest in references {
        let reference_path = reference_manifest.json_path();
        let Some(reference_node) =
            daybook_types::reference::schema_node_for_json_path(&schema_json, reference_path)?
        else {
            eyre::bail!(
                "invalid reference json_path '{}' for facet tag '{}': path does not exist in schema",
                reference_path,
                facet_tag
            );
        };

        match reference_manifest {
            manifest::FacetReferenceManifest::UrlString { .. }
            | manifest::FacetReferenceManifest::UrlStringSplit { .. } => {
                if !daybook_types::reference::schema_allows_string(reference_node) {
                    eyre::bail!(
                        "invalid reference json_path '{}' for facet tag '{}': schema node must allow a URL string",
                        reference_path,
                        facet_tag
                    );
                }
            }
            manifest::FacetReferenceManifest::UrlStringMany { .. } => {
                if !daybook_types::reference::schema_allows_array_of_strings(reference_node) {
                    eyre::bail!(
                        "invalid reference json_path '{}' for facet tag '{}': schema node must allow an array of URL strings",
                        reference_path,
                        facet_tag
                    );
                }
            }
            manifest::FacetReferenceManifest::UrlObject { .. }
            | manifest::FacetReferenceManifest::UrlObjectMany { .. } => {
                if !daybook_types::reference::schema_allows_reference_object(reference_node) {
                    eyre::bail!(
                        "invalid reference json_path '{}' for facet tag '{}': schema node must allow a reference object",
                        reference_path,
                        facet_tag
                    );
                }
            }
        }

        if let Some(at_commit_json_path) = reference_manifest.at_commit_json_path() {
            let Some(at_commit_node) = daybook_types::reference::schema_node_for_json_path(
                &schema_json,
                at_commit_json_path,
            )?
            else {
                eyre::bail!(
                    "invalid at_commit_json_path '{}' for facet tag '{}': path does not exist in schema",
                    at_commit_json_path,
                    facet_tag
                );
            };
            if !daybook_types::reference::schema_allows_array_of_strings(at_commit_node) {
                eyre::bail!(
                    "invalid at_commit_json_path '{}' for facet tag '{}': schema node must allow an array of commit hashes",
                    at_commit_json_path,
                    facet_tag
                );
            }
        }
    }
    Ok(())
}

/// Non-major-update breaking-change protection vs an explicit baseline
/// manifest: existing commands must be preserved with unchanged deets, and
/// facet value schemas must stay compatible. Major updates bypass these
/// checks. Used by the authoring path (baseline = last valid) and the
/// update/activation gates (baseline = last enabled).
pub fn check_breaking_changes(
    incoming: &manifest::PlugManifest,
    baseline: &manifest::PlugManifest,
) -> Res<()> {
    let is_major = incoming.version.major > baseline.version.major
        || (baseline.version.major == 0 && incoming.version.minor > baseline.version.minor);

    if !is_major {
        // In non-major updates, we must ensure existing commands are preserved
        // to avoid breaking integrations or automated workflows.
        for (old_cmd_name, old_cmd) in &baseline.commands {
            let new_cmd = incoming.commands.get(old_cmd_name);
            if let Some(new_cmd) = new_cmd {
                // Deets define the routine and parameters; changing them breaks callers.
                // FIXME: we need a better comparison for CommandDeets if it's complex
                if format!("{:?}", new_cmd.deets) != format!("{:?}", old_cmd.deets) {
                    eyre::bail!(
                        "Breaking change: command '{}' deets cannot change in non-major version update",
                        old_cmd_name
                    );
                }
            } else {
                eyre::bail!(
                    "Breaking change: command '{}' cannot be removed in non-major version update",
                    old_cmd_name
                );
            }
        }
    }

    // We also check that property keys aren't removed or their schemas don't become incompatible.
    for old_prop in &baseline.facets {
        if let Some(new_prop) = incoming
            .facets
            .iter()
            .find(|prop| prop.key_tag == old_prop.key_tag)
            && !is_schema_compatible(&old_prop.value_schema, &new_prop.value_schema)
        {
            eyre::bail!(
                "Incompatible schema for property tag '{}'",
                old_prop.key_tag
            );
        }
    }
    Ok(())
}
