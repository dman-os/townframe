use crate::interlude::*;

#[derive(Debug, clap::Subcommand)]
pub enum CheckoutCommands {
    /// Create a single-document checkout from one text/plain Body Note and dpath
    Create {
        directory: PathBuf,
        #[arg(long)]
        doc: String,
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
    use std::num::NonZeroU32;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use pauperfuse::backends::{BackendId, BackendTree, Description, Producer, ProducerAccess, RelPath};
    use pauperfuse::backends::tokio_fs::{ExpectedFile, FilePut, TokioFs};
    use pauperfuse::vtree::VtreeStore;
    use pauperfuse_daybook::{Daybook, Projection};
    use daybook_types::doc::{BranchPath, ChangeHashSet};
    use tokio::io::AsyncWriteExt;

    const MARKER: &str = ".daybook-checkout";
    const VERSION: u32 = 1;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
    enum State {
        Pending { failure: Option<String> },
        Ready { length: u64, digest: [u8; 32], generation: u64 },
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
        basis: Vec<String>,
        branch: String,
        branch_id: Option<String>,
        render_heads: Option<Vec<String>>,
        state: State,
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
            if let State::Ready { .. } = self.state {
                eyre::ensure!(self.branch_id.is_some() && self.render_heads.is_some(), "Ready checkout is missing its branch/render basis");
            }
            Ok(())
        }
    }

    pub(super) async fn run(command: CheckoutCommands) -> Res<ExitCode> {
        match command {
            CheckoutCommands::Create { directory, doc } => {
                let context = lazy::repo_ctx().await?;
                let drawer = lazy::drawer_repo().await?;
                let root = create(&context, drawer, &directory, doc).await?;
                println!("created checkout {}", root.display());
            }
            CheckoutCommands::Status { directory } => {
                let start = directory.unwrap_or(std::env::current_dir()?);
                let (root, checkout) = discover(&start).await?;
                lazy::select_checkout_repo(checkout.node_path()?);
                let context = lazy::repo_ctx().await?;
                verify_node(&context, &checkout)?;
                let drawer = lazy::drawer_repo().await?;
                if let State::Ready { .. } = checkout.state {
                    let entry = drawer.get_entry(&checkout.projection.document).await?
                        .ok_or_eyre("checkout document is no longer registered")?;
                    let branch = entry.branches.get(&checkout.branch).ok_or_eyre("checkout branch is no longer registered")?;
                    eyre::ensure!(Some(branch.branch_doc_id.to_string()) == checkout.branch_id, "checkout branch identity changed");
                }
                for line in status(&root, &checkout).await? { println!("{line}"); }
            }
        }
        Ok(ExitCode::SUCCESS)
    }

    fn verify_node(context: &daybook_core::repo::RepoCtx, checkout: &Checkout) -> Res<()> {
        eyre::ensure!(context.iroh_public_key == checkout.node_key, "recorded checkout node identity does not match the node at {}", context.layout.repo_root.display());
        eyre::ensure!(context.doc_drawer.document_id().to_string() == checkout.drawer, "recorded checkout drawer identity changed");
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

    async fn create(context: &daybook_core::repo::RepoCtx, drawer: Arc<daybook_core::drawer::DrawerRepo>, directory: &Path, document: String) -> Res<PathBuf> {
        let (projection, basis) = Projection::select(&drawer, document).await?;
        let output = RelPath::parse(&projection.path)?;
        validate_output(&output)?;
        let absolute = std::path::absolute(directory)?;
        preflight(&absolute, &output).await?;
        tokio::fs::create_dir_all(&absolute).await?;
        let root = tokio::fs::canonicalize(&absolute).await?;
        let node_path = tokio::fs::canonicalize(&context.layout.repo_root).await?;
        let id = Uuid::new_v4();
        let mut checkout = Checkout {
            version: VERSION, id,
            node_path: utils_rs::byte_key::encode(node_path.as_os_str().as_bytes()),
            node_key: context.iroh_public_key.clone(),
            drawer: context.doc_drawer.document_id().to_string(),
            projection, basis: am_utils_rs::serialize_commit_heads(&basis),
            branch: format!("/tmp/checkout/{id}"), branch_id: None, render_heads: None,
            state: State::Pending { failure: None },
        };
        write_initial(&root, &checkout).await?;
        let result = project(context, drawer, &root, &mut checkout, basis).await;
        if let Err(error) = result {
            checkout.state = State::Pending { failure: Some(format!("{error:#}")) };
            if let Err(marker_error) = replace_marker(&root, &checkout).await {
                eyre::bail!("projection failed: {error:#}; recording failure at {} also failed: {marker_error:#}", root.join(MARKER).display());
            }
            return Err(error).wrap_err_with(|| format!("checkout incomplete at {} (Pending marker retained)", root.display()));
        }
        Ok(root)
    }

    async fn project(context: &daybook_core::repo::RepoCtx, drawer: Arc<daybook_core::drawer::DrawerRepo>, root: &Path, checkout: &mut Checkout, basis: ChangeHashSet) -> Res<()> {
        let output = RelPath::parse(&checkout.projection.path)?;
        let native = TokioFs::to_native_path(&output)?;
        tokio::fs::create_dir_all(root.join(native.parent().unwrap())).await?;
        drawer.create_checkout_branch(&checkout.projection.document, BranchPath::new(&checkout.branch), BranchPath::new("main"), &basis).await?;
        let bundle = drawer.get_doc_bundle_at_branch(&checkout.projection.document, BranchPath::new(&checkout.branch), Some(Vec::new())).await?
            .ok_or_eyre("new checkout branch is missing")?;
        checkout.branch_id = Some(bundle.entry.branches.get(&checkout.branch).ok_or_eyre("checkout branch reference missing")?.branch_doc_id.to_string());
        checkout.render_heads = Some(am_utils_rs::serialize_commit_heads(&bundle.branch_heads));
        replace_marker(root, checkout).await?;
        let producer = Daybook::new(Arc::clone(&drawer), checkout.backend(), checkout.projection.clone(), checkout.branch.clone(), bundle.branch_heads);
        let store_path = checkout.store_path()?;
        tokio::fs::create_dir_all(store_path.parent().unwrap()).await?;
        let store = VtreeStore::open(&store_path).await?;
        let backend = store.register(producer.id()).await?;
        let version = store.replace(backend, &mut producer.observe().await?).await?;
        let mut scan = store.scan(version, NonZeroU32::new(16).unwrap());
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
        checkout.state = State::Ready { length: installed.evidence.length, digest: installed.evidence.digest, generation: version.generation };
        replace_marker(root, checkout).await?;
        verify_node(context, checkout)?;
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

    async fn status(root: &Path, checkout: &Checkout) -> Res<Vec<String>> {
        let path = RelPath::parse(&checkout.projection.path)?;
        let mut lines = Vec::new();
        match &checkout.state {
            State::Pending { failure } => {
                lines.push(format!("incomplete {}", checkout.projection.path));
                if let Some(failure) = failure { lines.push(format!("failure {failure}")); }
            }
            State::Ready { length, digest, generation } => {
                let store_path = checkout.store_path()?;
                eyre::ensure!(store_path.is_file(), "recorded checkout tree is missing at {}", store_path.display());
                let store = VtreeStore::open(&store_path).await?;
                let backend = store.lookup(&checkout.backend()).await?.ok_or_eyre("recorded Daybook backend is missing")?;
                eyre::ensure!(store.version(backend).await?.generation == *generation, "recorded checkout observation generation changed");
                let native = TokioFs::to_native_path(&path)?;
                let target = root.join(native);
                if inspect(&target).await?.is_none() {
                    lines.push(format!("missing {path}"));
                } else {
                    let evidence = TokioFs::new(root).observe(&path).await?;
                    let expected = ExpectedFile::Present(pauperfuse::backends::tokio_fs::FileEvidence { length: *length, digest: *digest });
                    lines.push(format!("{} {path}", if evidence == expected { "clean" } else { "modified" }));
                }
            }
        }
        let mut directories = vec![root.to_path_buf()];
        let mut untracked = BTreeSet::new();
        while let Some(directory) = directories.pop() {
            let mut entries = tokio::fs::read_dir(&directory).await?;
            while let Some(entry) = entries.next_entry().await? {
                let native = entry.path();
                let relative = native.strip_prefix(root).unwrap();
                if relative == Path::new(MARKER) { continue; }
                let kind = entry.file_type().await?;
                if kind.is_dir() { directories.push(native); }
                else {
                    let key = TokioFs::from_native_path(relative)?;
                    if key != path || matches!(checkout.state, State::Pending { .. }) { untracked.insert(key); }
                }
            }
        }
        lines.extend(untracked.into_iter().map(|path| format!("untracked {path}")));
        Ok(lines)
    }

    #[cfg(test)]
    mod tests;
}
