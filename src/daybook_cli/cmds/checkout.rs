use crate::interlude::*;

#[derive(Debug, clap::Subcommand)]
pub enum CheckoutCommands {
    /// Create a single-document checkout from one text/plain Body Note and dpath
    Create {
        directory: PathBuf,
        #[arg(long)]
        doc: String,
    },
    /// Ingest local file edits as document operations staged on the
    /// checkout-local branch. Never publishes: upstream stays untouched.
    Ingest {
        directory: Option<PathBuf>,
        /// Explicitly import an untracked file as a new document (repeatable;
        /// only the raw-text Note representation is supported)
        #[arg(long, value_name = "PATH")]
        allow: Vec<PathBuf>,
        /// Resolve a claim left by an interrupted import: adopt the document
        /// that was already created for this path on main
        /// (repeatable; PATH=DOC)
        #[arg(long, value_name = "PATH=DOC")]
        adopt_import_claim: Vec<String>,
        /// Resolve a claim left by an interrupted import by dropping it
        /// (repeatable; the file returns to untracked, any created document
        /// stays unbound and remains the user's business)
        #[arg(long, value_name = "PATH")]
        drop_import_claim: Vec<String>,
    },
    /// Report local files without ingesting or publishing (nearest checkout by default)
    Status { directory: Option<PathBuf> },
}

pub async fn run(command: CheckoutCommands) -> Res<ExitCode> {
    #[cfg(unix)]
    { unix::run(command).await }
    #[cfg(not(unix))]
    { let _ = command; eyre::bail!("checkout filesystem delivery currently requires Unix"); }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use std::collections::{BTreeSet, HashMap, HashSet};
    use std::num::NonZeroU32;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use pauperfuse::backends::{
        BackendId, BackendTree, Description, Producer, ProducerAccess, RelPath,
    };
    use pauperfuse::backends::tokio_fs::{CollectedFile, ExpectedFile, FileEvidence, FilePut, FileTake, TokioFs};
    use pauperfuse::vtree::VtreeStore;
    use pauperfuse_daybook::{Daybook, Projection, MAX_RAW_NOTE_BYTES, prepare_note_ingest, validate_note};
    use daybook_types::doc::{BranchPath, ChangeHashSet, DocPatch, FacetKey, WellKnownFacet, WellKnownFacetTag};
    use daybook_types::dpath::Dpath;
    use daybook_types::url::build_facet_ref;
    use tokio::io::AsyncWriteExt;

    const MARKER: &str = ".daybook-checkout";
    const VERSION: u32 = 2;

    /// Ingest is the single writer for a checkout only while one CLI process
    /// runs it; no cross-process lock exists in this slice. Concurrent CLI
    /// invocations against one checkout are a user error until the watch lane
    /// introduces real locking. Marker v2 documents this contract.
    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    #[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
    enum State {
        Pending { failure: Option<String> },
        Ready { length: u64, digest: [u8; 32], generation: u64, blocked: Option<Blocked> },
    }

    /// A blocked checkout: a preceding ingest failed and, under the
    /// whole-checkout blocking rule, automatic flows must not resume until a
    /// successful explicit ingest (or another resolving operation) clears it.
    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct Blocked {
        failure: String,
    }

    /// Durable receipt mapping one ingested file's evidence to the
    /// checkout-branch heads it was staged at. A strictly local checkout fact:
    /// never a vtree row, never an acknowledgement of upstream publication.
    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct Receipt {
        /// RelPath key of the bound output, matching the projection spelling.
        path: String,
        /// Evidence of the exact bytes accepted into the branch.
        file: FileEvidence,
        /// Serialized commit heads of the owning branch AFTER this staging.
        branch_heads: Vec<String>,
        /// Monotonic per-checkout counter; a receipt replaces any earlier
        /// receipt for the same path.
        sequence: u64,
    }

    /// A binding created by an `--allow` import: a document whose first
    /// acknowledged disk state is the imported file itself. Its branch was
    /// forked from main at the importing add, so it holds no other staged work.
    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct Import {
        projection: Projection,
        branch: String,
        branch_id: String,
        /// Heads the import branch was forked at; the recorded disk evidence
        /// corresponds to this state.
        render_heads: Vec<String>,
        /// Evidence of the file at import time; `None` if the file was absent
        /// when the binding was adopted, so there is no known render evidence.
        file: Option<FileEvidence>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct Checkout {
        version: u32,
        id: Uuid,
        node_path: String,
        node_key: String,
        drawer: String,
        projection: Projection,
        /// Additional bindings created by ingest imports (see `Import`).
        imports: Vec<Import>,
        basis: Vec<String>,
        branch: String,
        branch_id: Option<String>,
        render_heads: Option<Vec<String>>,
        state: State,
        receipts: Vec<Receipt>,
        /// Claimed import paths from an interrupted ingest. Presence blocks all
        /// ingestion: the document may or may not exist, and re-importing would
        /// duplicate its identity. Resolution is explicit (see the command flags).
        pending_imports: Vec<String>,
    }

    impl Checkout {
        fn backend(&self) -> BackendId { BackendId(format!("daybook:{}:{}", self.node_key, self.id)) }
        fn node_path(&self) -> Res<PathBuf> {
            let bytes = utils_rs::byte_key::decode(&self.node_path)?;
            eyre::ensure!(!bytes.contains(&0), "NUL in recorded node path");
            let path = PathBuf::from(std::ffi::OsString::from_vec(bytes));
            eyre::ensure!(path.is_absolute(), "recorded node path is not absolute");
            Ok(path)
        }
        fn store_path(&self) -> Res<PathBuf> {
            Ok(self.node_path()?.join("local_state/checkouts").join(self.id.to_string()).join("vtree.sqlite"))
        }
        fn validate(&self) -> Res<()> {
            eyre::ensure!(self.version == VERSION, "unsupported checkout marker version {}", self.version);
            eyre::ensure!(self.branch == format!("/tmp/checkout/{}", self.id), "checkout branch does not match its identity");
            let path = RelPath::parse(&self.projection.path)?;
            validate_output(&path)?;
            self.node_path()?;
            am_utils_rs::parse_commit_heads(&self.basis)?;
            for import in &self.imports {
                let path = RelPath::parse(&import.projection.path)?;
                validate_output(&path)?;
                eyre::ensure!(
                    import.branch == format!("/tmp/checkout/{}/{}", self.id, import.projection.document),
                    "import branch does not match its identity"
                );
                am_utils_rs::parse_commit_heads(&import.render_heads)?;
            }
            for receipt in &self.receipts {
                RelPath::parse(&receipt.path)?;
                am_utils_rs::parse_commit_heads(&receipt.branch_heads)?;
            }
            for claim in &self.pending_imports {
                RelPath::parse(claim)?;
            }
            if let State::Ready { .. } = self.state {
                eyre::ensure!(self.branch_id.is_some() && self.render_heads.is_some(), "Ready checkout is missing its branch/render basis");
            }
            Ok(())
        }
    }

    /// Reduced node identity for the checkout path; tests construct this
    /// directly instead of sourcing ambient configuration.
    #[derive(Clone, Debug)]
    struct NodeHandle {
        repo_root: PathBuf,
        node_key: String,
        drawer: String,
    }

    fn node_handle(context: &daybook_core::repo::RepoCtx) -> NodeHandle {
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

    pub(super) async fn run(command: CheckoutCommands) -> Res<ExitCode> {
        match command {
            CheckoutCommands::Create { directory, doc } => {
                ensure_checkout_support(lazy::plugs_repo().await?.as_ref()).await?;
                let context = lazy::repo_ctx().await?;
                let drawer = lazy::drawer_repo().await?;
                let root = create(node_handle(&context), drawer, &directory, doc).await?;
                println!("created checkout {}", root.display());
            }
            CheckoutCommands::Ingest { directory, allow, adopt_import_claim, drop_import_claim } => {
                ensure_checkout_support(lazy::plugs_repo().await?.as_ref()).await?;
                let start = directory.unwrap_or(std::env::current_dir()?);
                let (root, mut checkout) = discover(&start).await?;
                lazy::select_checkout_repo(checkout.node_path()?);
                let context = lazy::repo_ctx().await?;
                verify_node(&node_handle(&context), &checkout).await?;
                let drawer = lazy::drawer_repo().await?;
                resolve_claims(&root, &mut checkout, drawer.as_ref(), &adopt_import_claim, &drop_import_claim).await?;
                ingest_checked(&root, &mut checkout, drawer.as_ref(), &allow).await?;
            }
            CheckoutCommands::Status { directory } => {
                let start = directory.unwrap_or(std::env::current_dir()?);
                let (root, checkout) = discover(&start).await?;
                lazy::select_checkout_repo(checkout.node_path()?);
                let context = lazy::repo_ctx().await?;
                verify_node(&node_handle(&context), &checkout).await?;
                let drawer = lazy::drawer_repo().await?;
                verify_bindings(drawer.as_ref(), &checkout).await?;
                for line in status(&root, &checkout, drawer.as_ref()).await? { println!("{line}"); }
            }
        }
        Ok(ExitCode::SUCCESS)
    }

    async fn verify_node(node: &NodeHandle, checkout: &Checkout) -> Res<()> {
        eyre::ensure!(node.node_key == checkout.node_key, "recorded checkout node identity does not match the node at {}", node.repo_root.display());
        eyre::ensure!(node.drawer == checkout.drawer, "recorded checkout drawer identity changed");
        let path = checkout.node_path()?;
        let canonical = tokio::fs::canonicalize(&node.repo_root).await?;
        eyre::ensure!(path == canonical, "recorded checkout node path is {} but the node is at {}", path.display(), canonical.display());
        Ok(())
    }

    fn validate_output(path: &RelPath) -> Res<()> {
        eyre::ensure!(!path.is_root(), "checkout output cannot be the root");
        eyre::ensure!(path.components().first().map(String::as_str) != Some(MARKER), "checkout output conflicts with {MARKER}");
        TokioFs::to_native_path(path)?;
        Ok(())
    }

    async fn inspect(path: &Path) -> Res<Option<std::fs::Metadata>> {
        match tokio::fs::symlink_metadata(path).await {
            Ok(metadata) => Ok(Some(metadata)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).wrap_err_with(|| format!("inspecting {}", path.display())),
        }
    }

    /// Checks existing ancestors before creating directories or a checkout marker.
    async fn preflight(root: &Path, output: &RelPath) -> Res<()> {
        let mut prefix = PathBuf::new();
        for component in root.components() {
            prefix.push(component);
            if let Some(metadata) = inspect(&prefix).await? {
                eyre::ensure!(metadata.is_dir() && !metadata.is_symlink(), "checkout ancestor is not a real directory: {}", prefix.display());
            }
        }
        eyre::ensure!(inspect(&root.join(MARKER)).await?.is_none(), "checkout marker already exists at {}", root.join(MARKER).display());
        let native = TokioFs::to_native_path(output)?;
        let mut destination = root.to_path_buf();
        let count = native.components().count();
        for (index, component) in native.components().enumerate() {
            destination.push(component);
            if let Some(metadata) = inspect(&destination).await? {
                if index + 1 == count { eyre::bail!("checkout output is occupied: {}", destination.display()); }
                eyre::ensure!(metadata.is_dir() && !metadata.is_symlink(), "output ancestor is not a real directory: {}", destination.display());
            }
        }
        Ok(())
    }

    async fn create(node: NodeHandle, drawer: Arc<daybook_core::drawer::DrawerRepo>, directory: &Path, document: String) -> Res<PathBuf> {
        let (projection, basis) = Projection::select(&drawer, document).await?;
        let output = RelPath::parse(&projection.path)?;
        validate_output(&output)?;
        let absolute = std::path::absolute(directory)?;
        preflight(&absolute, &output).await?;
        tokio::fs::create_dir_all(&absolute).await?;
        let root = tokio::fs::canonicalize(&absolute).await?;
        let node_root = tokio::fs::canonicalize(&node.repo_root).await?;
        let id = Uuid::new_v4();
        let mut checkout = Checkout {
            version: VERSION, id,
            node_path: utils_rs::byte_key::encode(node_root.as_os_str().as_bytes()),
            node_key: node.node_key.clone(),
            drawer: node.drawer.clone(),
            projection,
            imports: Vec::new(),
            basis: am_utils_rs::serialize_commit_heads(&basis),
            branch: format!("/tmp/checkout/{id}"), branch_id: None, render_heads: None,
            state: State::Pending { failure: None },
            receipts: Vec::new(),
            pending_imports: Vec::new(),
        };
        write_initial(&root, &checkout).await?;
        let result = project(&node, drawer, &root, &mut checkout, basis).await;
        if let Err(error) = result {
            checkout.state = State::Pending { failure: Some(format!("{error:#}")) };
            if let Err(marker_error) = replace_marker(&root, &checkout).await {
                eyre::bail!("projection failed: {error:#}; recording failure at {} also failed: {marker_error:#}", root.join(MARKER).display());
            }
            return Err(error).wrap_err_with(|| format!("checkout incomplete at {} (Pending marker retained)", root.display()));
        }
        Ok(root)
    }

    async fn project(node: &NodeHandle, drawer: Arc<daybook_core::drawer::DrawerRepo>, root: &Path, checkout: &mut Checkout, basis: ChangeHashSet) -> Res<()> {
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
            .get_branch_ref(&checkout.projection.document, BranchPath::new(&checkout.branch))
            .await?
            .ok_or_eyre("checkout branch reference missing after creation")?;
        eyre::ensure!(
            branch_ref.branch_kind == daybook_core::drawer::BranchKind::Local,
            "checkout branch must stay local"
        );
        checkout.branch_id = Some(branch_ref.branch_doc_id.to_string());
        let bundle = drawer
            .get_doc_bundle_at_branch(
                &checkout.projection.document,
                BranchPath::new(&checkout.branch),
                Some(Vec::new()),
            )
            .await?
            .ok_or_eyre("new checkout branch is missing")?;
        checkout.render_heads = Some(am_utils_rs::serialize_commit_heads(&bundle.branch_heads));
        replace_marker(root, checkout).await?;
        let producer = Daybook::new(Arc::clone(&drawer), checkout.backend(), checkout.projection.clone(), checkout.branch.clone(), bundle.branch_heads);
        let store_path = checkout.store_path()?;
        tokio::fs::create_dir_all(store_path.parent().unwrap()).await?;
        let store = VtreeStore::open(&store_path).await?;
        let backend = store.register(producer.id()).await?;
        let version = store.replace(backend, &mut producer.observe().await?).await?;
        // Lane B guidance (256-2048) for scan page size: one page per scan round
        // instead of the placeholder 16, without unbounded chunking.
        let mut scan = store.scan(version, NonZeroU32::new(512).unwrap());
        let mut puts = Vec::new();
        while let Some(entry) = scan.next_entry().await? {
            if let Description::File { source, .. } = entry.description {
                puts.push(FilePut { path: entry.path, source, expected: ExpectedFile::Absent });
            }
        }
        eyre::ensure!(puts.len() == 1, "single-document text projection must describe exactly one file");
        let receiver = TokioFs::new(root);
        let mut prepared = receiver.prepare(puts, &ProducerAccess::new(&producer)).await?;
        let applied = prepared.apply().await;
        // Cleanup never removes installed targets. Preserve both failures if cleanup fails.
        let cleanup = prepared.cleanup().await;
        let installed = match (applied, cleanup) {
            (Ok(installed), Ok(())) => installed,
            (Err(error), Ok(())) => return Err(error.into()),
            (Ok(_), Err(error)) => return Err(error.into()),
            (Err(error), Err(cleanup)) => eyre::bail!("application failed: {error}; staging cleanup also failed: {cleanup}"),
        };
        let [installed] = installed.as_slice() else { panic!("single-file batch returned unexpected outcomes"); };
        eyre::ensure!(installed.path == output, "installed output differs from recorded binding");
        checkout.state = State::Ready { length: installed.evidence.length, digest: installed.evidence.digest, generation: version.generation, blocked: None };
        replace_marker(root, checkout).await?;
        verify_node(node, checkout).await?;
        Ok(())
    }

    async fn read_marker(root: &Path) -> Res<Checkout> {
        let path = root.join(MARKER);
        let metadata = tokio::fs::symlink_metadata(&path).await?;
        eyre::ensure!(metadata.is_file() && !metadata.is_symlink(), "checkout marker is not a regular file: {}", path.display());
        let checkout: Checkout = serde_json::from_slice(&tokio::fs::read(&path).await?)
            .wrap_err_with(|| format!("reading checkout marker {}", path.display()))?;
        checkout.validate().wrap_err_with(|| format!("invalid checkout marker {}", path.display()))?;
        Ok(checkout)
    }

    async fn write_initial(root: &Path, checkout: &Checkout) -> Res<()> {
        let path = root.join(MARKER);
        let mut file = tokio::fs::OpenOptions::new().write(true).create_new(true).open(&path).await?;
        file.write_all(&serde_json::to_vec_pretty(checkout)?).await?;
        file.flush().await?;
        Ok(())
    }

    async fn replace_marker(root: &Path, checkout: &Checkout) -> Res<()> {
        let previous = read_marker(root).await?;
        eyre::ensure!(previous.id == checkout.id, "checkout marker identity changed at {}", root.join(MARKER).display());
        let temporary = root.join(format!("{MARKER}.{}", Uuid::new_v4()));
        let mut file = tokio::fs::OpenOptions::new().write(true).create_new(true).open(&temporary).await?;
        file.write_all(&serde_json::to_vec_pretty(checkout)?).await?;
        file.flush().await?;
        drop(file);
        // Recheck ownership before replacing; hostile namespace races remain out of scope.
        eyre::ensure!(read_marker(root).await?.id == checkout.id, "checkout marker identity changed before replacement");
        tokio::fs::rename(&temporary, root.join(MARKER)).await
            .wrap_err_with(|| format!("replacing {} from {}", root.join(MARKER).display(), temporary.display()))?;
        Ok(())
    }

    async fn discover(start: &Path) -> Res<(PathBuf, Checkout)> {
        let mut root = std::path::absolute(start)?;
        eyre::ensure!(root.is_dir(), "checkout search must start at a directory: {}", root.display());
        loop {
            if inspect(&root.join(MARKER)).await?.is_some() {
                let checkout = read_marker(&root).await?;
                return Ok((root, checkout));
            }
            if !root.pop() { eyre::bail!("no checkout marker found from {}", start.display()); }
        }
    }

    /// One tracked output binding and the recorded render basis it is compared
    /// against. The primary projection comes first; imports follow.
    #[derive(Clone, Debug)]
    struct Tracked {
        projection: Projection,
        branch: String,
        branch_id: String,
        /// Evidence of the bytes whose ingestion last produced the recorded
        /// render heads. None when there is no known acknowledged render state.
        render_evidence: Option<FileEvidence>,
        render_heads: Option<Vec<String>>,
    }

    fn tracked_bindings(checkout: &Checkout) -> Res<Vec<Tracked>> {
        let State::Ready { length, digest, .. } = &checkout.state else {
            eyre::bail!("checkout is not Ready")
        };
        let render_evidence = FileEvidence { length: *length, digest: *digest };
        let primary = Tracked {
            projection: checkout.projection.clone(),
            branch: checkout.branch.clone(),
            branch_id: checkout.branch_id.clone().ok_or_eyre("Ready checkout is missing its branch basis")?,
            render_evidence: Some(render_evidence),
            render_heads: Some(checkout.render_heads.clone().ok_or_eyre("Ready checkout is missing its render heads")?),
        };
        let imports = checkout.imports.iter().map(|import| Tracked {
            projection: import.projection.clone(),
            branch: import.branch.clone(),
            branch_id: import.branch_id.clone(),
            render_evidence: import.file.clone(),
            render_heads: Some(import.render_heads.clone()),
        });
        Ok(std::iter::once(primary).chain(imports).collect())
    }

    /// Verifies every recorded binding still resolves: document registered,
    /// local-branch ref present with unchanged identity.
    async fn verify_bindings(drawer: &daybook_core::drawer::DrawerRepo, checkout: &Checkout) -> Res<()> {
        if !matches!(checkout.state, State::Ready { .. }) { return Ok(()); }
        for track in tracked_bindings(checkout)? {
            let document = &track.projection.document;
            drawer.get_entry(document).await?
                .ok_or_eyre(format!("checkout document {document} is no longer registered"))?;
            let branch_ref = drawer
                .get_branch_ref(document, BranchPath::new(&track.branch))
                .await?
                .ok_or_eyre(format!("checkout branch {} is no longer registered", track.branch))?;
            eyre::ensure!(
                branch_ref.branch_kind == daybook_core::drawer::BranchKind::Local,
                "checkout branch {} must stay local", track.branch
            );
            eyre::ensure!(
                branch_ref.branch_doc_id.to_string() == track.branch_id,
                "checkout branch identity changed for {document} at {}",
                track.branch
            );
        }
        Ok(())
    }

    /// Live branch heads and the recorded Note facet at those heads for one
    /// binding. The branch is the staging surface: base plus staged local work.
    async fn binding_state(
        drawer: &daybook_core::drawer::DrawerRepo,
        track: &Tracked,
    ) -> Res<(Vec<String>, Option<daybook_types::doc::Note>)> {
        let bundle = drawer
            .get_doc_bundle_at_branch(
                &track.projection.document,
                BranchPath::new(&track.branch),
                Some(vec![track.projection.facet.clone()]),
            )
            .await?
            .ok_or_eyre(format!(
                "checkout branch {} for {} is missing",
                track.branch, track.projection.document
            ))?;
        let note = match bundle.doc.facets.get(&track.projection.facet) {
            Some(value) => Some(validate_note(value)?),
            None => None,
        };
        Ok((am_utils_rs::serialize_commit_heads(&bundle.branch_heads), note))
    }

    fn note_evidence(note: &daybook_types::doc::Note) -> FileEvidence {
        FileEvidence {
            length: note.content.len() as u64,
            digest: *blake3::hash(note.content.as_bytes()).as_bytes(),
        }
    }

    /// Exact observation state of one tracked output against its recorded
    /// render basis and the live checkout-branch content. The branch's current
    /// Note (not just the receipt) is compared so a lost or behind receipt can
    /// never make a staged state masquerade as modified, or vice versa.
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Disposition {
        /// Bound path absent from disk. Never ingested; never a deletion.
        Missing,
        /// File matches the acknowledged render and the branch holds nothing
        /// beyond it.
        Clean,
        /// File matches the branch's staged content: already ingested.
        /// `false` = the receipt is missing or behind (crash window).
        Ingested(bool),
        /// File matches the render but the branch carries divergent staged
        /// work; the next ingest stages the revert.
        StagedDiverged,
        /// File differs from both the branch content and the render basis.
        Modified,
    }

    fn disposition(
        observed: &ExpectedFile,
        render_evidence: Option<&FileEvidence>,
        render_heads: &[String],
        live_heads: &[String],
        branch_note: Option<&daybook_types::doc::Note>,
        receipt: Option<&Receipt>,
    ) -> Disposition {
        let ExpectedFile::Present(evidence) = observed else { return Disposition::Missing };
        let staged = live_heads != render_heads;
        if staged && branch_note.map(note_evidence).as_ref() == Some(evidence) {
            let confirmed = matches!(receipt, Some(receipt) if &receipt.file == evidence && receipt.branch_heads == live_heads);
            return Disposition::Ingested(confirmed);
        }
        if Some(evidence) == render_evidence {
            return if staged { Disposition::StagedDiverged } else { Disposition::Clean };
        }
        Disposition::Modified
    }

    fn display(disposition: &Disposition, path: &str) -> String {
        match disposition {
            Disposition::Missing => format!("missing {path}"),
            Disposition::Clean => format!("clean {path}"),
            Disposition::Ingested(true) => format!("ingested {path}"),
            Disposition::Ingested(false) => format!("ingested (unconfirmed) {path}"),
            Disposition::StagedDiverged => format!("staged diverging {path}"),
            Disposition::Modified => format!("modified {path}"),
        }
    }

    /// Files (and symlinks) under the checkout root that no binding, claim, or
    /// the marker owns. Pending checkouts exclude nothing but the marker.
    async fn scan_untracked(root: &Path, excluded: &HashSet<String>) -> Res<BTreeSet<RelPath>> {
        let mut untracked = BTreeSet::new();
        let mut directories = vec![root.to_path_buf()];
        while let Some(directory) = directories.pop() {
            let mut entries = tokio::fs::read_dir(&directory).await?;
            while let Some(entry) = entries.next_entry().await? {
                let native = entry.path();
                let relative = native.strip_prefix(root).unwrap();
                if relative == Path::new(MARKER) { continue; }
                let kind = entry.file_type().await?;
                if kind.is_dir() {
                    directories.push(native);
                } else if let Ok(key) = TokioFs::from_native_path(relative)
                    && !excluded.contains(&key.to_string())
                {
                    untracked.insert(key);
                }
                // Unconvertible native names stay invisible to tracking in this
                // slice; the tokio_fs naming rules refuse them explicitly when
                // a path is actually requested.
            }
        }
        Ok(untracked)
    }

    fn tracked_exclusions(checkout: &Checkout) -> Res<HashSet<String>> {
        let mut excluded: HashSet<String> = HashSet::new();
        excluded.insert(RelPath::parse(MARKER)?.to_string());
        if !matches!(checkout.state, State::Pending { .. }) {
            excluded.insert(RelPath::parse(&checkout.projection.path)?.to_string());
            for import in &checkout.imports {
                excluded.insert(RelPath::parse(&import.projection.path)?.to_string());
            }
        }
        Ok(excluded)
    }

    async fn status(root: &Path, checkout: &Checkout, drawer: &daybook_core::drawer::DrawerRepo) -> Res<Vec<String>> {
        let mut lines = Vec::new();
        match &checkout.state {
            State::Pending { failure } => {
                lines.push(format!("incomplete {}", checkout.projection.path));
                if let Some(failure) = failure { lines.push(format!("failure {failure}")); }
            }
            State::Ready { blocked, .. } => {
                if let Some(blocked) = blocked {
                    lines.push(format!("blocked ingest {}", blocked.failure));
                }
                for track in tracked_bindings(checkout)? {
                    let path_key = track.projection.path.clone();
                    let rel = RelPath::parse(&path_key)?;
                    let observed = TokioFs::new(root).observe(&rel).await?;
                    let (live_heads, branch_note) = binding_state(drawer, &track).await?;
                    let render_heads = track.render_heads.as_ref().ok_or_eyre("Ready checkout is missing its render heads")?;
                    let receipt = checkout.receipts.iter().rev().find(|receipt| receipt.path == path_key);
                    let settled = disposition(&observed, track.render_evidence.as_ref(), render_heads, &live_heads, branch_note.as_ref(), receipt);
                    lines.push(display(&settled, &path_key));
                }
            }
        }
        for claim in &checkout.pending_imports {
            lines.push(format!("unresolved import claim {claim}"));
        }
        let mut excluded = tracked_exclusions(checkout)?;
        for claim in &checkout.pending_imports {
            excluded.insert(RelPath::parse(claim)?.to_string());
        }
        for untracked in scan_untracked(root, &excluded).await? {
            lines.push(format!("untracked {untracked}"));
        }
        Ok(lines)
    }

    /// Records that an ingest blocked the checkout. A Pending checkout stays
    /// Pending: its create-phase failure already blocks everything.
    async fn record_blocked(root: &Path, checkout: &mut Checkout, error: &eyre::Report) -> Res<()> {
        let State::Ready { blocked, .. } = &mut checkout.state else {
            return Ok(());
        };
        *blocked = Some(Blocked { failure: format!("{error:#}") });
        replace_marker(root, checkout).await?;
        Ok(())
    }

    /// Ingest entry with the failure-blocking contract: any failure records the
    /// blocked state in the marker (best effort) before propagating.
    async fn ingest_checked(root: &Path, checkout: &mut Checkout, drawer: &daybook_core::drawer::DrawerRepo, allow: &[PathBuf]) -> Res<()> {
        if let Err(error) = ingest(root, checkout, drawer, allow).await {
            record_blocked(root, checkout, &error).await?;
            return Err(error);
        }
        Ok(())
    }

    /// Resolves import claims left by interrupted ingests. `--adopt-import-claim
    /// path doc` binds `path` to a document that was already created on main;
    /// `--drop-import-claim path` returns the path to untracked.
    async fn resolve_claims(
        root: &Path,
        checkout: &mut Checkout,
        drawer: &daybook_core::drawer::DrawerRepo,
        adopt: &[String],
        drop: &[String],
    ) -> Res<()> {
        if adopt.is_empty() && drop.is_empty() { return Ok(()); }
        eyre::ensure!(
            matches!(checkout.state, State::Ready { .. }),
            "incomplete (Pending) checkouts cannot resolve import claims"
        );
        for pair in adopt {
            eyre::ensure!(pair.split('=').count() == 2, "--adopt-import-claim expects PATH=DOC, got {pair:?}");
            let (path_arg, document) = pair.split_once('=').expect("checked above");
            let document: daybook_types::doc::DocId = document.to_string();
            let key = allowed_key(root, Path::new(path_arg))?;
            eyre::ensure!(
                checkout.pending_imports.iter().any(|claim| claim == &key),
                "no pending import claim for {key}"
            );
            let note_key = FacetKey::from(WellKnownFacetTag::Note);
            let dpath_key = Dpath::parse(&format!("/{key}"))?.facet_key();
            // Select exactly the dpath claim: an empty Some(Vec::new()) hydrates
            // nothing even when the facets object exists.
            let bundle = drawer
                .get_doc_bundle_at_branch(&document, BranchPath::new("main"), Some(vec![dpath_key.clone()]))
                .await?
                .ok_or_eyre(format!("document {document} is not registered on main"))?;
            eyre::ensure!(
                bundle.doc.facets.contains_key(&dpath_key),
                "document {document} does not claim {key} through its dpath facet"
            );
            let import_branch = format!("/tmp/checkout/{}/{}", checkout.id, document);
            drawer
                .create_checkout_branch(&document, BranchPath::new(&import_branch), BranchPath::new("main"), &bundle.branch_heads)
                .await?;
            let branch_ref = drawer
                .get_branch_ref(&document, BranchPath::new(&import_branch))
                .await?
                .ok_or_eyre("import branch reference missing after adoption")?;
            eyre::ensure!(
                branch_ref.branch_kind == daybook_core::drawer::BranchKind::Local,
                "import branch must stay local"
            );
            let bundle = drawer
                .get_doc_bundle_at_branch(&document, BranchPath::new(&import_branch), Some(Vec::new()))
                .await?
                .ok_or_eyre("import branch is missing after adoption")?;
            let observed = TokioFs::new(root).observe(&RelPath::parse(&key)?).await?;
            let file = match &observed {
                ExpectedFile::Present(evidence) => Some(evidence.clone()),
                ExpectedFile::Absent => None,
            };
            checkout.imports.push(Import {
                projection: Projection {
                    document: document.to_string(),
                    facet: note_key,
                    path: key.clone(),
                },
                branch: import_branch,
                branch_id: branch_ref.branch_doc_id.to_string(),
                render_heads: am_utils_rs::serialize_commit_heads(&bundle.branch_heads),
                file: file.clone(),
            });
            checkout.pending_imports.retain(|claim| claim != &key);
            if let Some(evidence) = file {
                push_receipt(checkout, key.clone(), evidence, am_utils_rs::serialize_commit_heads(&bundle.branch_heads));
            }
            replace_marker(root, checkout).await?;
            println!("adopted import claim {key} -> {document}");
        }
        for path_arg in drop {
            // Drop must not depend on file existence: a claim may outlive the
            // file it named. Claims are printed as checkout-relative keys, so
            // that exact spelling is what drop takes.
            let key = RelPath::parse(path_arg)
                .map_err(|error| eyre::eyre!("--drop-import-claim expects a checkout-relative path ({error}): {path_arg}"))?
                .to_string();
            eyre::ensure!(
                checkout.pending_imports.iter().any(|claim| claim == &key),
                "no pending import claim for {key}"
            );
            checkout.pending_imports.retain(|claim| claim != &key);
            replace_marker(root, checkout).await?;
            println!("dropped import claim {key}");
        }
        Ok(())
    }

    fn push_receipt(checkout: &mut Checkout, path: String, file: FileEvidence, branch_heads: Vec<String>) {
        let sequence = checkout.receipts.iter().map(|receipt| receipt.sequence).max().unwrap_or(0) + 1;
        checkout.receipts.retain(|receipt| receipt.path != path);
        checkout.receipts.push(Receipt { path, file, branch_heads, sequence });
    }

    /// Resolves an `--allow` argument into a checkout-relative RelPath key.
    /// The file must exist, must be under the checkout root, and symlinks are
    /// refused before canonicalization can hide them.
    fn allowed_key(root: &Path, argument: &Path) -> Res<String> {
        let metadata = std::fs::symlink_metadata(argument)
            .wrap_err_with(|| format!("inspecting --allow argument {}", argument.display()))?;
        eyre::ensure!(!metadata.is_symlink(), "--allow refuses symlinks: {}", argument.display());
        eyre::ensure!(metadata.is_file(), "--allow expects a regular file: {}", argument.display());
        let real = std::fs::canonicalize(argument)?;
        let relative = real
            .strip_prefix(root)
            .map_err(|_| eyre::eyre!("--allow path {} is not under the checkout root {}", real.display(), root.display()))?;
        let key = TokioFs::from_native_path(relative)?;
        eyre::ensure!(!relative.as_os_str().is_empty(), "--allow cannot be the checkout root");
        Ok(key.to_string())
    }


    struct EditTarget {
        track: Tracked,
        live_heads: Vec<String>,
        branch_note: daybook_types::doc::Note,
    }

    /// What the batch must stage: a prepared edit of an existing tracked
    /// binding, or a prepared import of an allowed untracked file.
    enum Target {
        Edit(Box<EditTarget>),
        Import(String),
    }

    /// Recognizes and prepares the whole checkout's batch before staging
    /// anything (ADR 011 §6 steps 1–4, ADR 012 §6). Nothing is written to the
    /// filesystem except the marker's own progress records; upstream `main` is
    /// never read or merged: staging is local-first and publication is out of
    /// this slice.
    async fn ingest(
        root: &Path,
        checkout: &mut Checkout,
        drawer: &daybook_core::drawer::DrawerRepo,
        allow: &[PathBuf],
    ) -> Res<()> {
        eyre::ensure!(
            matches!(checkout.state, State::Ready { .. }),
            "checkout is incomplete (Pending); resolve it before ingesting"
        );
        if let Some(claim) = checkout.pending_imports.first() {
            eyre::bail!(
                "unresolved import claim for {claim} from an interrupted ingest; \
                 resolve with --adopt-import-claim {claim}=<doc-id> or --drop-import-claim {claim} before ingesting"
            );
        }
        let tracked = tracked_bindings(checkout)?;
        let tracked_paths: HashSet<String> = tracked
            .iter()
            .map(|track| track.projection.path.clone())
            .chain(std::iter::once(RelPath::parse(MARKER)?.to_string()))
            .collect();
        let receiver = TokioFs::new(root);

        // Classify the whole checkout first. Per-path failures accumulate and
        // refuse the entire batch before any document operation is staged.
        let mut failures: Vec<String> = Vec::new();
        // Paired take/target entries: collection preserves input order.
        let mut entries: Vec<(FileTake, Target)> = Vec::new();

        for track in &tracked {
            let path_key = track.projection.path.clone();
            let rel = match RelPath::parse(&path_key) {
                Ok(rel) => rel,
                Err(error) => {
                    failures.push(format!("{path_key}: recorded binding path does not parse: {error:#}"));
                    continue;
                }
            };
            let observed = match receiver.observe(&rel).await {
                Ok(observed) => observed,
                Err(error) => {
                    failures.push(format!("{path_key}: obstructed: {error}"));
                    continue;
                }
            };
            let (live_heads, branch_note) = match binding_state(drawer, track).await {
                Ok(state) => state,
                Err(error) => {
                    failures.push(format!("{path_key}: checkout branch state unavailable: {error:#}"));
                    continue;
                }
            };
            let Some(branch_note) = branch_note else {
                failures.push(format!("{path_key}: checkout Note is missing on its branch"));
                continue;
            };
            let render_heads = track.render_heads.as_ref().ok_or_eyre("Ready checkout is missing its render heads")?;
            let receipt = checkout.receipts.iter().rev().find(|receipt| receipt.path == path_key);
            let settled = disposition(&observed, track.render_evidence.as_ref(), render_heads, &live_heads, Some(&branch_note), receipt);
            let ExpectedFile::Present(_) = &observed else {
                println!("missing {path_key}");
                continue;
            };
            match settled {
                Disposition::Clean | Disposition::Ingested(true) => println!("{}", display(&settled, &path_key)),
                // Ingested(false), StagedDiverged, and Modified all enter the
                // batch: the no-op rule against the live branch confirms
                // already-staged bytes or stages their revert.
                _ => {
                    entries.push((
                        FileTake { path: rel, expected: observed },
                        Target::Edit(Box::new(EditTarget {
                            track: track.clone(),
                            live_heads,
                            branch_note,
                        })),
                    ));
                }
            }
        }

        for argument in allow {
            let key = match allowed_key(root, argument) {
                Ok(key) => key,
                Err(error) => {
                    failures.push(format!("--allow {}: {error:#}", argument.display()));
                    continue;
                }
            };
            if tracked_paths.contains(&key) {
                failures.push(format!("--allow {key}: already tracked"));
                continue;
            }
            if checkout.pending_imports.contains(&key) {
                failures.push(format!("--allow {key}: unresolved import claim blocks re-import"));
                continue;
            }
            let rel = match RelPath::parse(&key) {
                Ok(rel) => rel,
                Err(_) => {
                    failures.push(format!("--allow {key}: path does not convert to a checkout tree key"));
                    continue;
                }
            };
            if let Err(error) = validate_output(&rel) {
                failures.push(format!("--allow {key}: {error:#}"));
                continue;
            }
            let observed = match receiver.observe(&rel).await {
                Ok(observed) => observed,
                Err(error) => {
                    failures.push(format!("--allow {key}: obstructed: {error}"));
                    continue;
                }
            };
            if !matches!(observed, ExpectedFile::Present(_)) {
                failures.push(format!("--allow {key}: no such untracked file"));
                continue;
            }
            entries.push((FileTake { path: rel, expected: observed }, Target::Import(key)));
        }

        if !failures.is_empty() {
            eyre::bail!("ingest refused:\n  {}", failures.join("\n  "));
        }

        // Bytes and evidence in one read, capped by the owning representation
        // so the Automerge facet cannot be inflated past its declared bound;
        // the whole-batch recheck rejects changed-under-us files. One bounded
        // refresh-and-retry covers the changed-under-us window.
        let collected = {
            let take_list: Vec<FileTake> = entries.iter().map(|(take, _)| take.clone()).collect();
            match receiver.collect(&take_list, MAX_RAW_NOTE_BYTES).await {
                Ok(collected) => collected,
                Err(first) => {
                    let mut refreshed: Vec<(FileTake, Target)> = Vec::new();
                    for (take, target) in entries {
                        match receiver.observe(&take.path).await {
                            Ok(observed @ ExpectedFile::Present(_)) => {
                                refreshed.push((FileTake { path: take.path, expected: observed }, target));
                            }
                            Ok(ExpectedFile::Absent) => println!("missing {}", take.path),
                            Err(error) => {
                                eyre::bail!("ingest failed at {} and refresh could not re-observe it: {error}; first failure: {first}", take.path);
                            }
                        }
                    }
                    eyre::ensure!(!refreshed.is_empty(), "ingest failed and no files remained to retry: {first}");
                    let take_list: Vec<FileTake> = refreshed.iter().map(|(take, _)| take.clone()).collect();
                    let collected = receiver
                        .collect(&take_list, MAX_RAW_NOTE_BYTES)
                        .await
                        .wrap_err_with(|| format!("ingest failed again after refreshing evidence; first failure: {first}"))?;
                    entries = refreshed;
                    collected
                }
            }
        };

        // Batch gate (ADR 012 §6 step 2): every entry's lens inverse must
        // prepare BEFORE any document operation is staged. One unprepared file
        // refuses the batch; nothing at all is staged or blocked-partially.
        let mut prepared: Vec<(daybook_types::doc::Note, CollectedFile, Target)> =
            Vec::with_capacity(entries.len());
        for (collected, (_, target)) in collected.into_iter().zip(entries) {
            match prepare_note_ingest(&collected.bytes) {
                Ok(note) => prepared.push((note, collected, target)),
                Err(error) => failures.push(format!("{}: {error}", collected.path)),
            }
        }
        if !failures.is_empty() {
            eyre::bail!("ingest refused:\n  {}", failures.join("\n  "));
        }

        let mut staged = 0usize;
        for (note, collected, target) in prepared {
            match target {
                Target::Edit(edit) => {
                    stage_edit(root, checkout, drawer, &receiver, *edit, collected, note).await?;
                }
                Target::Import(key) => {
                    stage_import(root, checkout, drawer, key, collected, note).await?;
                }
            }
            staged += 1;
        }

        // A completed ingest rechecked every path: blocking no longer applies.
        if let State::Ready { blocked, .. } = &mut checkout.state {
            *blocked = None;
        }
        replace_marker(root, checkout).await?;
        println!("staged {staged} document operation(s) on the checkout-local branch; nothing published upstream");
        Ok(())
    }

    /// Stages one prepared edit: file bytes become the candidate Note through
    /// the raw-text lens inverse; a No-op against the live branch state only
    /// writes a confirming receipt (closing a crash window). A real difference
    /// is committed to the checkout branch at the heads it was prepared at,
    /// then recorded as a receipt.
    async fn stage_edit(
        root: &Path,
        checkout: &mut Checkout,
        drawer: &daybook_core::drawer::DrawerRepo,
        receiver: &TokioFs,
        edit: EditTarget,
        collected: pauperfuse::backends::tokio_fs::CollectedFile,
        // Prepared by the batch gate before any staging begins.
        note: daybook_types::doc::Note,
    ) -> Res<()> {
        let EditTarget { track, live_heads, branch_note } = edit;
        let path_key = track.projection.path.clone();
        if note == branch_note {
            // Already staged (round-trip stability, ADR 012 §8): only a
            // confirming receipt is written when one is missing or behind.
            let receipt = checkout.receipts.iter().rev().find(|receipt| receipt.path == path_key);
            let confirmed = matches!(receipt, Some(receipt) if receipt.file == collected.evidence && receipt.branch_heads == live_heads);
            if !confirmed {
                push_receipt(checkout, path_key.clone(), collected.evidence, live_heads);
                replace_marker(root, checkout).await?;
            }
            println!("ingested {path_key}");
            return Ok(());
        }
        // One final expected-state recheck immediately before the drawer write:
        // external tools may have touched the file during preparation.
        let observed = receiver.observe(&collected.path).await?;
        eyre::ensure!(
            observed == ExpectedFile::Present(collected.evidence.clone()),
            "{path_key} changed during ingest; refusing to stage stale bytes"
        );
        let patch = DocPatch {
            id: track.projection.document.clone(),
            facets_set: HashMap::from([(
                track.projection.facet.clone(),
                serde_json::Value::from(WellKnownFacet::Note(note)),
            )]),
            facets_remove: Vec::new(),
            user_path: None,
        };
        let heads: ChangeHashSet = ChangeHashSet(am_utils_rs::parse_commit_heads(&live_heads)?);
        drawer
            .update_at_heads(patch, BranchPath::new(&track.branch), Some(heads))
            .await?;
        // The staged heads come from a fresh readback, not an assumption.
        let (fresh_heads, _) = binding_state(drawer, &track).await?;
        eyre::ensure!(fresh_heads != live_heads, "staging {path_key} did not advance the checkout branch");
        push_receipt(checkout, path_key.clone(), collected.evidence, fresh_heads);
        replace_marker(root, checkout).await?;
        println!("ingested {path_key}");
        Ok(())
    }

    /// Stages one `--allow` import: claim the path first so an interrupted
    /// ingest can never duplicate a document identity, then create the
    /// document on main, fork its checkout-local branch, and record the
    /// binding plus receipt.
    async fn stage_import(
        root: &Path,
        checkout: &mut Checkout,
        drawer: &daybook_core::drawer::DrawerRepo,
        key: String,
        collected: pauperfuse::backends::tokio_fs::CollectedFile,
        // Prepared by the batch gate before any staging begins.
        note: daybook_types::doc::Note,
    ) -> Res<()> {
        checkout.pending_imports.push(key.clone());
        replace_marker(root, checkout).await?;

        let note_key = FacetKey::from(WellKnownFacetTag::Note);
        let note_url = build_facet_ref("self", &note_key)?;
        let args = daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: HashMap::from([
                (note_key.clone(), serde_json::Value::from(WellKnownFacet::Note(note.clone()))),
                (
                    FacetKey::from(WellKnownFacetTag::Body),
                    serde_json::Value::from(WellKnownFacet::Body(daybook_types::doc::Body {
                        order: vec![note_url],
                    })),
                ),
                (Dpath::parse(&format!("/{key}"))?.facet_key(), serde_json::json!({})),
            ]),
            user_path: None,
        };
        let document = drawer.add(args).await.map_err(eyre::Report::from)?;
        let (_, heads) = drawer
            .get_with_heads(&document, BranchPath::new("main"), None)
            .await?
            .ok_or_eyre(format!("imported document {document} is missing on main after creation"))?;
        let import_branch = format!("/tmp/checkout/{}/{}", checkout.id, document);
        drawer
            .create_checkout_branch(&document, BranchPath::new(&import_branch), BranchPath::new("main"), &heads)
            .await?;
        let branch_ref = drawer
            .get_branch_ref(&document, BranchPath::new(&import_branch))
            .await?
            .ok_or_eyre("import branch reference missing after import")?;
        eyre::ensure!(
            branch_ref.branch_kind == daybook_core::drawer::BranchKind::Local,
            "import branch must stay local"
        );
        let bundle = drawer
            .get_doc_bundle_at_branch(&document, BranchPath::new(&import_branch), Some(Vec::new()))
            .await?
            .ok_or_eyre("import branch is missing after import")?;
        checkout.imports.push(Import {
            projection: Projection {
                document: document.clone(),
                facet: note_key,
                path: key.clone(),
            },
            branch: import_branch,
            branch_id: branch_ref.branch_doc_id.to_string(),
            render_heads: am_utils_rs::serialize_commit_heads(&bundle.branch_heads),
            file: Some(collected.evidence.clone()),
        });
        checkout.pending_imports.retain(|claim| claim != &key);
        push_receipt(checkout, key.clone(), collected.evidence, am_utils_rs::serialize_commit_heads(&bundle.branch_heads));
        replace_marker(root, checkout).await?;
        println!("imported {key} -> {document}");
        Ok(())
    }

    #[cfg(test)]
    mod tests;
}
