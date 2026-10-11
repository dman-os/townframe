use super::*;

/// Reduced node identity for the checkout path; tests construct this
/// directly instead of sourcing ambient configuration.
#[derive(Clone, Debug)]
pub(super) struct NodeHandle {
    pub(super) repo_root: PathBuf,
    pub(super) node_key: String,
    pub(super) drawer: String,
}

pub(super) fn node_handle(context: &daybook_core::repo::RepoCtx) -> NodeHandle {
    NodeHandle {
        repo_root: context.layout.repo_root.clone(),
        node_key: context.iroh_public_key.clone(),
        drawer: context.doc_drawer.document_id().to_string(),
    }
}

/// Old nodes have a core manifest that predates checkout support.
pub(crate) async fn ensure_checkout_support(plugs: &daybook_core::plugs::PlugsRepo) -> Res<()> {
    match plugs
        .get_facet_manifest_by_tag(daybook_types::dpath::DPATH_FACET_TAG)
        .await
    {
        daybook_core::plugs::FacetManifestLookup::Found(_) => Ok(()),
        _ => eyre::bail!(
            "node predates checkout support: no whole-document dpath facet is registered; \
             checkouts require a node whose core manifest declares them"
        ),
    }
}

pub(super) async fn verify_node(node: &NodeHandle, checkout: &Checkout) -> Res<()> {
    eyre::ensure!(
        node.node_key == checkout.node_key,
        "recorded checkout node identity does not match the node at {}",
        node.repo_root.display()
    );
    eyre::ensure!(
        node.drawer == checkout.drawer,
        "recorded checkout drawer identity changed"
    );
    let path = checkout.node_path()?;
    let canonical = tokio::fs::canonicalize(&node.repo_root).await?;
    eyre::ensure!(
        path == canonical,
        "recorded checkout node path is {} but the node is at {}",
        path.display(),
        canonical.display()
    );
    Ok(())
}

pub(super) fn validate_output(path: &RelPath) -> Res<()> {
    eyre::ensure!(!path.is_root(), "checkout output cannot be the root");
    eyre::ensure!(
        path.components().first().map(String::as_str) != Some(MARKER),
        "checkout output conflicts with {MARKER}"
    );
    TokioFs::to_native_path(path)?;
    Ok(())
}

pub(super) async fn inspect(path: &Path) -> Res<Option<std::fs::Metadata>> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).wrap_err_with(|| format!("inspecting {}", path.display())),
    }
}

/// Checks existing ancestors before creating directories or a checkout marker.
pub(super) async fn preflight(root: &Path, output: &RelPath) -> Res<()> {
    let mut prefix = PathBuf::new();
    for component in root.components() {
        prefix.push(component);
        if let Some(metadata) = inspect(&prefix).await? {
            eyre::ensure!(
                metadata.is_dir() && !metadata.is_symlink(),
                "checkout ancestor is not a real directory: {}",
                prefix.display()
            );
        }
    }
    eyre::ensure!(
        inspect(&root.join(MARKER)).await?.is_none(),
        "checkout marker already exists at {}",
        root.join(MARKER).display()
    );
    let native = TokioFs::to_native_path(output)?;
    let mut destination = root.to_path_buf();
    let count = native.components().count();
    for (index, component) in native.components().enumerate() {
        destination.push(component);
        if let Some(metadata) = inspect(&destination).await? {
            if index + 1 == count {
                eyre::bail!("checkout output is occupied: {}", destination.display());
            }
            eyre::ensure!(
                metadata.is_dir() && !metadata.is_symlink(),
                "output ancestor is not a real directory: {}",
                destination.display()
            );
        }
    }
    Ok(())
}

pub(super) async fn create(
    node: NodeHandle,
    drawer: Arc<daybook_core::drawer::DrawerRepo>,
    directory: &Path,
    document: String,
) -> Res<PathBuf> {
    let claims = Projection::select_claims(&drawer, document.clone()).await?;
    let basis = claims.heads.clone();
    let projected = claims.projected();
    // Today's checkout surface handles exactly one projected dpath claim;
    // everything else is a visible outcome, never a silently ignored
    // claim (Q5).
    // Checkout execution stays note-only for now: a raw-blob projection
    // names the deferred blob-routing phase at the recipe-identity check
    // inside Daybook (LensFailure::Recipe surfaces as a Pending-marker
    // failure; nothing silently swallows it).
    let (_, projection, _) = match projected.as_slice() {
        [single] => single.clone(),
        many => {
            let outcomes = claims
                .outcomes
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ");
            eyre::bail!(
                "checkout requires exactly one projected dpath claim; document {} projected {} ({}); outcomes: {outcomes}",
                document,
                many.len(),
                claims.outcomes.len()
            );
        }
    };
    let output = RelPath::parse(&projection.path)?;
    validate_output(&output)?;
    let absolute = std::path::absolute(directory)?;
    preflight(&absolute, &output).await?;
    tokio::fs::create_dir_all(&absolute).await?;
    let root = tokio::fs::canonicalize(&absolute).await?;
    let node_root = tokio::fs::canonicalize(&node.repo_root).await?;
    let id = Uuid::new_v4();
    let checkout = Checkout {
        version: VERSION,
        id,
        node_path: utils_rs::byte_key::encode(node_root.as_os_str().as_bytes()),
        node_key: node.node_key.clone(),
        drawer: node.drawer.clone(),
        projection,
        imports: Vec::new(),
        basis: am_utils_rs::serialize_commit_heads(&basis),
        branch: format!("/tmp/checkout/{id}"),
        branch_id: None,
        render_heads: None,
        state: State::Pending { failure: None },
        receipts: Vec::new(),
        pending_imports: Vec::new(),
        publications: Vec::new(),
    };
    let mut marker = Marker {
        checkout,
        disk_version: VERSION,
    };
    write_initial(&root, marker.checkout()).await?;
    let result = project(&node, drawer, &root, &mut marker, basis).await;
    if let Err(error) = result {
        marker.checkout_mut().state = State::Pending {
            failure: Some(format!("{error:#}")),
        };
        if let Err(marker_error) = replace_marker(&root, &marker).await {
            eyre::bail!(
                "projection failed: {error:#}; recording failure at {} also failed: {marker_error:#}",
                root.join(MARKER).display()
            );
        }
        return Err(error).wrap_err_with(|| {
            format!(
                "checkout incomplete at {} (Pending marker retained)",
                root.display()
            )
        });
    }
    Ok(root)
}

pub(super) async fn project(
    node: &NodeHandle,
    drawer: Arc<daybook_core::drawer::DrawerRepo>,
    root: &Path,
    marker: &mut Marker,
    basis: ChangeHashSet,
) -> Res<()> {
    let checkout = marker.checkout_mut();
    let output = RelPath::parse(&checkout.projection.path)?;
    let native = TokioFs::to_native_path(&output)?;
    tokio::fs::create_dir_all(root.join(native.parent().unwrap())).await?;
    drawer
        .create_checkout_branch(
            &checkout.projection.document,
            BranchPath::new(&checkout.branch),
            BranchPath::new("main"),
            &basis,
        )
        .await?;
    // Checkout-local branch refs live in the drawer's local store, not the
    // replicated entry, so resolve through get_branch_ref.
    let branch_ref = drawer
        .get_branch_ref(
            &checkout.projection.document,
            BranchPath::new(&checkout.branch),
        )
        .await?
        .ok_or_eyre("checkout branch reference missing after creation")?;
    eyre::ensure!(
        branch_ref.branch_kind == daybook_core::drawer::BranchKind::Local,
        "checkout branch must stay local"
    );
    let checkout = marker.checkout_mut();
    checkout.branch_id = Some(branch_ref.branch_doc_id.to_string());
    let bundle = drawer
        .get_doc_bundle_at_branch(
            &checkout.projection.document,
            BranchPath::new(&checkout.branch),
            Some(Vec::new()),
        )
        .await?
        .ok_or_eyre("new checkout branch is missing")?;
    {
        let checkout = marker.checkout_mut();
        checkout.render_heads = Some(am_utils_rs::serialize_commit_heads(&bundle.branch_heads));
    }
    replace_marker(root, marker).await?;
    // Rebound after the marker write: the producer reads the recorded
    // fields, not the mutable borrow.
    let checkout = marker.checkout();
    let producer = Daybook::new(
        Arc::clone(&drawer),
        checkout.backend(),
        checkout.projection.clone(),
        checkout.branch.clone(),
        bundle.branch_heads,
    );
    let store_path = checkout.store_path()?;
    tokio::fs::create_dir_all(store_path.parent().unwrap()).await?;
    let store = VtreeStore::open(&store_path).await?;
    let backend = store.register(producer.id()).await?;
    let version = store
        .replace(backend, &mut producer.observe().await?)
        .await?;
    // Lane B guidance (256-2048) for scan page size: one page per scan round
    // instead of the placeholder 16, without unbounded chunking.
    let mut scan = store.scan(version, NonZeroU32::new(512).unwrap());
    let mut puts = Vec::new();
    while let Some(entry) = scan.next_entry().await? {
        if let Description::File { source, .. } = entry.description {
            puts.push(FilePut {
                path: entry.path,
                source,
                expected: ExpectedFile::Absent,
            });
        }
    }
    eyre::ensure!(
        puts.len() == 1,
        "single-document text projection must describe exactly one file"
    );
    let receiver = TokioFs::new(root);
    let mut prepared = receiver
        .prepare(puts, &ProducerAccess::new(&producer))
        .await?;
    let applied = prepared.apply().await;
    // Cleanup never removes installed targets. Preserve both failures if cleanup fails.
    let cleanup = prepared.cleanup().await;
    let installed = match (applied, cleanup) {
        (Ok(installed), Ok(())) => installed,
        (Err(error), Ok(())) => return Err(error.into()),
        (Ok(_), Err(error)) => return Err(error.into()),
        (Err(error), Err(cleanup)) => {
            eyre::bail!("application failed: {error}; staging cleanup also failed: {cleanup}")
        }
    };
    let [installed] = installed.as_slice() else {
        panic!("single-file batch returned unexpected outcomes");
    };
    eyre::ensure!(
        installed.path == output,
        "installed output differs from recorded binding"
    );
    let checkout = marker.checkout_mut();
    checkout.state = State::Ready {
        length: installed.evidence.length,
        digest: installed.evidence.digest,
        generation: version.generation,
        blocked: None,
    };
    replace_marker(root, marker).await?;
    verify_node(node, marker.checkout()).await?;
    Ok(())
}

pub(super) async fn read_marker(root: &Path) -> Res<Marker> {
    let path = root.join(MARKER);
    let metadata = tokio::fs::symlink_metadata(&path).await?;
    eyre::ensure!(
        metadata.is_file() && !metadata.is_symlink(),
        "checkout marker is not a regular file: {}",
        path.display()
    );
    let bytes = tokio::fs::read(&path).await?;
    // The v2↔v3 distinction is explicit at load: the version byte picks
    // the exact layout, an old (unwritable) version refuses loudly, and a
    // v2 marker loads into the runtime shape without gaining v3 fields.
    let version: u32 = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|value| value.get("version").and_then(|version| version.as_u64()))
        .and_then(|version| version.try_into().ok())
        .ok_or_else(|| eyre::eyre!("checkout marker {} has no readable version", path.display()))?;
    let marker = match version {
        VERSION => Marker {
            checkout: serde_json::from_slice(&bytes)
                .wrap_err_with(|| format!("reading checkout marker {}", path.display()))?,
            disk_version: VERSION,
        },
        2 => {
            let v2: CheckoutV2 = serde_json::from_slice(&bytes)
                .wrap_err_with(|| format!("reading checkout marker {}", path.display()))?;
            Marker {
                checkout: v2.runtime_shape(),
                disk_version: 2,
            }
        }
        other => eyre::bail!(
            "unsupported checkout marker version {other} at {} (this CLI writes {VERSION})",
            path.display()
        ),
    };
    marker
        .checkout()
        .validate()
        .wrap_err_with(|| format!("invalid checkout marker {}", path.display()))?;
    Ok(marker)
}

pub(super) async fn write_initial(root: &Path, checkout: &Checkout) -> Res<()> {
    let path = root.join(MARKER);
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await?;
    file.write_all(&serde_json::to_vec_pretty(checkout)?)
        .await?;
    file.flush().await?;
    Ok(())
}

pub(super) async fn replace_marker(root: &Path, marker: &Marker) -> Res<()> {
    let previous = read_marker(root).await?;
    eyre::ensure!(
        previous.checkout().id == marker.checkout().id,
        "checkout marker identity changed at {}",
        root.join(MARKER).display()
    );
    eyre::ensure!(
        previous.disk_version == marker.disk_version
            && previous.checkout().version == marker.checkout().version,
        "checkout marker version changed under the running verb at {}",
        root.join(MARKER).display()
    );
    let bytes = match marker.disk_version {
        VERSION => serde_json::to_vec_pretty(marker.checkout())?,
        2 => serde_json::to_vec_pretty(&marker.v2_shape())?,
        other => eyre::bail!("unsupported on-disk marker version {other} for write-back"),
    };
    let temporary = root.join(format!("{MARKER}.{}", Uuid::new_v4()));
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .await?;
    file.write_all(&bytes).await?;
    file.flush().await?;
    drop(file);
    // Recheck ownership before replacing; hostile namespace races remain out of scope.
    eyre::ensure!(
        read_marker(root).await?.checkout().id == marker.checkout().id,
        "checkout marker identity changed before replacement"
    );
    tokio::fs::rename(&temporary, root.join(MARKER))
        .await
        .wrap_err_with(|| {
            format!(
                "replacing {} from {}",
                root.join(MARKER).display(),
                temporary.display()
            )
        })?;
    Ok(())
}

pub(super) async fn discover(start: &Path) -> Res<(PathBuf, Marker)> {
    let mut root = std::path::absolute(start)?;
    eyre::ensure!(
        root.is_dir(),
        "checkout search must start at a directory: {}",
        root.display()
    );
    loop {
        if inspect(&root.join(MARKER)).await?.is_some() {
            let marker = read_marker(&root).await?;
            return Ok((root, marker));
        }
        if !root.pop() {
            eyre::bail!("no checkout marker found from {}", start.display());
        }
    }
}
