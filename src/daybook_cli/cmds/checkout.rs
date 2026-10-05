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
    /// Publish staged, receipt-settled work upstream. Each destination
    /// recomputes the validated candidate merge at fresh upstream heads and
    /// publishes through the expected-heads CAS; refuses unsettled staging
    /// instead of publishing implicitly (ADR 011 §5).
    Publish { directory: Option<PathBuf> },
}

pub async fn run(command: CheckoutCommands) -> Res<ExitCode> {
    #[cfg(unix)]
    {
        unix::run(command).await
    }
    #[cfg(not(unix))]
    {
        let _ = command;
        eyre::bail!("checkout filesystem delivery currently requires Unix");
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use daybook_types::doc::{
        BranchPath, ChangeHashSet, DocPatch, FacetKey, WellKnownFacet, WellKnownFacetTag,
    };
    use daybook_types::dpath::{Dpath, DpathFacet};
    use daybook_types::url::build_facet_ref;
    use pauperfuse::backends::tokio_fs::{
        CollectedFile, ExpectedFile, FileEvidence, FilePut, FileTake, TokioFs,
    };
    use pauperfuse::backends::{
        BackendId, BackendTree, Description, Producer, ProducerAccess, RelPath,
    };
    use pauperfuse::vtree::VtreeStore;
    use pauperfuse_daybook::{
        Daybook, MAX_RAW_NOTE_BYTES, Projection, prepare_note_ingest, validate_note,
    };
    use std::collections::{BTreeSet, HashMap, HashSet};
    use std::num::NonZeroU32;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use tokio::io::AsyncWriteExt;

    const MARKER: &str = ".daybook-checkout";
    const VERSION: u32 = 3;

    /// Retry bound for the publish-side expected-heads CAS against concurrent
    /// upstream writers (ADR 011 §6 step 8: a finite bound, not a backoff; on
    /// exhaustion the checkout blocks with its staged work preserved and an
    /// explicit retry resumes). The upstream environment is what keeps
    /// moving; racing it forever cannot be what "publish" means.
    const PUBLISH_MAX_CAS_ATTEMPTS: u32 = 3;

    /// Test-only injection window between the validated candidate's persist
    /// (§1.1 step 7) and the upstream publish CAS (§1.1 step 8) — exactly the
    /// interval a concurrent upstream writer races. A test arms a channel
    /// pair keyed by the destination document; each loop iteration reaching
    /// the window requests the race and waits for the racing commit's
    /// signal, so the race is deterministic, never timed. The loop drives
    /// the request directly rather than subscribing to heads-advance events:
    /// a no-op intake persist commits nothing and emits no event, and the
    /// first attempt against an unmoved upstream is exactly that case (a
    /// heads-advance event subscription only fires once real commits land).
    /// Closed or absent channels mean no racer is armed and the window is
    /// empty. Absent from non-test builds.
    #[cfg(test)]
    static PUBLISH_RACE_CHANNELS: std::sync::OnceLock<
        std::sync::Mutex<HashMap<String, PublishRaceChannel>>,
    > = std::sync::OnceLock::new();

    /// The loop→racer handshake for one armed destination: the publish loop
    /// sends a request at the persist→publish window; the racer commits its
    /// upstream movement at main's live heads and signals back.
    #[cfg(test)]
    struct PublishRaceChannel {
        requests: tokio::sync::mpsc::UnboundedSender<()>,
        signals: tokio::sync::mpsc::UnboundedReceiver<()>,
    }

    /// Which flow a block was recorded by. Ingest blocking pre-dates
    /// publication (v2 markers carried a bare failure) and its vocabulary
    /// stays `ingest`.
    #[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
    #[serde(rename_all = "snake_case")]
    enum Operation {
        Ingest,
        Publish,
    }

    /// Publication outcome vocabulary, per design §4.1's spellings.
    #[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
    #[serde(rename_all = "snake_case")]
    enum Outcome {
        /// The destination's content reached upstream `main`.
        Published,
        /// The publish was denied (unreachable/authority) or failed on a
        /// runtime error; local bytes and staged history are untouched.
        Refused,
        /// The merge candidate was invalid, or the CAS retry bound ran out;
        /// the checkout-level block records the situation (ADR 011 §7).
        Blocked,
    }

    /// Durable per-destination publication record, design §4.1. JSON field
    /// spellings are the sketch's verbatim (camelCase head fields) — a
    /// deviation would be a design change, not a choice.
    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct Publication {
        /// Per-checkout monotonic, same last-writer-wins counter as receipts.
        seq: u64,
        /// Destination document id (later batches: per destination incl.
        /// cross-doc facet targets).
        #[serde(rename = "doc")]
        document: String,
        /// The upstream heads this attempt's candidate was validated against.
        #[serde(rename = "expectedHeads")]
        expected_heads: Vec<String>,
        /// The checkout branch heads after the validated candidate persisted.
        #[serde(rename = "branchHeads")]
        branch_heads: Vec<String>,
        /// Main heads after the CAS merge; `null` when not published.
        #[serde(rename = "publishedHeads")]
        published_heads: Option<Vec<String>>,
        outcome: Outcome,
        /// `refused`/`blocked` diagnostic; null for published.
        failure: Option<String>,
        /// CAS attempts consumed in this run.
        attempts: u32,
    }

    /// Ingest is the single writer for a checkout only while one CLI process
    /// runs it; no cross-process lock exists in this slice. Concurrent CLI
    /// invocations against one checkout are a user error until the watch lane
    /// introduces real locking. Marker v2 documents this contract.
    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    #[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
    enum State {
        Pending {
            failure: Option<String>,
        },
        Ready {
            length: u64,
            digest: [u8; 32],
            generation: u64,
            blocked: Option<Blocked>,
        },
    }

    /// A blocked checkout: a preceding operation failed and, under the
    /// whole-checkout blocking rule, automatic flows must not resume until a
    /// successful explicit operation clears it. v3 records which verb
    /// blocked (v2 was always ingest; the landed v2 shape is honored read-
    /// back and re-written by ingest below).
    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct Blocked {
        operation: Operation,
        failure: String,
    }

    /// The landed v2 blocking record: ingest blocks only, no operation tag.
    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct BlockedV2 {
        failure: String,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    #[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
    enum StateV2 {
        Pending {
            failure: Option<String>,
        },
        Ready {
            length: u64,
            digest: [u8; 32],
            generation: u64,
            blocked: Option<BlockedV2>,
        },
    }

    /// The landed v2 marker. Status and ingest keep working against it
    /// read-only-of-shape: writes stay in the v2 layout, publication is
    /// what upgrades the marker (on write).
    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct CheckoutV2 {
        version: u32,
        id: Uuid,
        node_path: String,
        node_key: String,
        drawer: String,
        projection: Projection,
        imports: Vec<Import>,
        basis: Vec<String>,
        branch: String,
        branch_id: Option<String>,
        render_heads: Option<Vec<String>>,
        state: StateV2,
        receipts: Vec<Receipt>,
        pending_imports: Vec<String>,
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

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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
        /// Per-destination publication records (design §4.1). v3-only: empty
        /// until this checkout's first publish.
        publications: Vec<Publication>,
    }

    /// A loaded checkout marker, explicit about its on-disk version: the
    /// runtime fields are always the current (v3) shape — a v2 marker loads
    /// into that shape with an inferred `ingest`-only `operation` and no
    /// publications — while writes re-serialize the disk version's exact
    /// layout. Only the publish verb upgrades a marker (on write).
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Marker {
        checkout: Checkout,
        disk_version: u32,
    }

    impl CheckoutV2 {
        /// Loads the landed v2 layout into the runtime (v3) field shape. The
        /// layout cannot record an operation: its blocked record is ingest's
        /// by construction (publish did not exist on v2).
        fn runtime_shape(self) -> Checkout {
            let CheckoutV2 {
                version,
                id,
                node_path,
                node_key,
                drawer,
                projection,
                imports,
                basis,
                branch,
                branch_id,
                render_heads,
                state,
                receipts,
                pending_imports,
            } = self;
            Checkout {
                version,
                id,
                node_path,
                node_key,
                drawer,
                projection,
                imports,
                basis,
                branch,
                branch_id,
                render_heads,
                state: match state {
                    StateV2::Pending { failure } => State::Pending { failure },
                    StateV2::Ready {
                        length,
                        digest,
                        generation,
                        blocked,
                    } => State::Ready {
                        length,
                        digest,
                        generation,
                        blocked: blocked.map(|blocked| Blocked {
                            operation: Operation::Ingest,
                            failure: blocked.failure,
                        }),
                    },
                },
                receipts,
                pending_imports,
                publications: Vec::new(),
            }
        }
    }

    impl Marker {
        fn checkout(&self) -> &Checkout {
            &self.checkout
        }
        fn checkout_mut(&mut self) -> &mut Checkout {
            &mut self.checkout
        }

        /// Publish's upgrade point: the marker's disk layout becomes v3.
        /// A converted v2 record keeps its blocking state with the operation
        /// the layout could not name (ingest, the only v2 blocker).
        fn upgrade(&mut self) {
            if self.disk_version == VERSION {
                return;
            }
            self.checkout.version = VERSION;
            self.disk_version = VERSION;
        }

        /// The marker as v2 layout bytes, for write-back preserving a loaded
        /// v2 marker's shape. Blocked drops its operation tag (v2 blocked is
        /// ingest-only by construction), publications have no key.
        fn v2_shape(&self) -> CheckoutV2 {
            debug_assert!(self.disk_version == 2);
            let Checkout {
                id,
                node_path,
                node_key,
                drawer,
                projection,
                imports,
                basis,
                branch,
                branch_id,
                render_heads,
                state,
                receipts,
                pending_imports,
                ..
            } = &self.checkout;
            CheckoutV2 {
                version: 2,
                id: *id,
                node_path: node_path.clone(),
                node_key: node_key.clone(),
                drawer: drawer.clone(),
                projection: projection.clone(),
                imports: imports.clone(),
                basis: basis.clone(),
                branch: branch.clone(),
                branch_id: branch_id.clone(),
                render_heads: render_heads.clone(),
                state: match state {
                    State::Pending { failure } => StateV2::Pending {
                        failure: failure.clone(),
                    },
                    State::Ready {
                        length,
                        digest,
                        generation,
                        blocked,
                    } => StateV2::Ready {
                        length: *length,
                        digest: *digest,
                        generation: *generation,
                        blocked: blocked.as_ref().map(|blocked| BlockedV2 {
                            failure: blocked.failure.clone(),
                        }),
                    },
                },
                receipts: receipts.clone(),
                pending_imports: pending_imports.clone(),
            }
        }
    }

    impl Checkout {
        fn backend(&self) -> BackendId {
            BackendId(format!("daybook:{}:{}", self.node_key, self.id))
        }
        fn node_path(&self) -> Res<PathBuf> {
            let bytes = utils_rs::byte_key::decode(&self.node_path)?;
            eyre::ensure!(!bytes.contains(&0), "NUL in recorded node path");
            let path = PathBuf::from(std::ffi::OsString::from_vec(bytes));
            eyre::ensure!(path.is_absolute(), "recorded node path is not absolute");
            Ok(path)
        }
        fn store_path(&self) -> Res<PathBuf> {
            Ok(self
                .node_path()?
                .join("local_state/checkouts")
                .join(self.id.to_string())
                .join("vtree.sqlite"))
        }
        fn validate(&self) -> Res<()> {
            eyre::ensure!(
                self.version == VERSION || self.version == 2,
                "unsupported checkout marker version {}",
                self.version
            );
            eyre::ensure!(
                self.branch == format!("/tmp/checkout/{}", self.id),
                "checkout branch does not match its identity"
            );
            let path = RelPath::parse(&self.projection.path)?;
            validate_output(&path)?;
            self.node_path()?;
            am_utils_rs::parse_commit_heads(&self.basis)?;
            for import in &self.imports {
                let path = RelPath::parse(&import.projection.path)?;
                validate_output(&path)?;
                eyre::ensure!(
                    import.branch
                        == format!("/tmp/checkout/{}/{}", self.id, import.projection.document),
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
            // v3 adds the publication ledger; v2 markers (pre-publication
            // capability) load into the runtime shape without one.
            if self.version == VERSION {
                let mut latest_seq = 0_u64;
                for publication in &self.publications {
                    am_utils_rs::parse_commit_heads(&publication.expected_heads)?;
                    am_utils_rs::parse_commit_heads(&publication.branch_heads)?;
                    if let Some(published) = &publication.published_heads {
                        am_utils_rs::parse_commit_heads(published)?;
                    }
                    eyre::ensure!(
                        publication.seq > latest_seq,
                        "publication records must have increasing per-checkout seq"
                    );
                    latest_seq = publication.seq;
                    eyre::ensure!(
                        (publication.outcome == Outcome::Published)
                            == publication.published_heads.is_some(),
                        "only published records carry publishedHeads"
                    );
                }
            }
            if let State::Ready { .. } = self.state {
                eyre::ensure!(
                    self.branch_id.is_some() && self.render_heads.is_some(),
                    "Ready checkout is missing its branch/render basis"
                );
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
            CheckoutCommands::Ingest {
                directory,
                allow,
                adopt_import_claim,
                drop_import_claim,
            } => {
                ensure_checkout_support(lazy::plugs_repo().await?.as_ref()).await?;
                let start = directory.unwrap_or(std::env::current_dir()?);
                let (root, mut marker) = discover(&start).await?;
                lazy::select_checkout_repo(marker.checkout().node_path()?);
                let context = lazy::repo_ctx().await?;
                verify_node(&node_handle(&context), marker.checkout()).await?;
                let drawer = lazy::drawer_repo().await?;
                resolve_claims(
                    &root,
                    &mut marker,
                    drawer.as_ref(),
                    &adopt_import_claim,
                    &drop_import_claim,
                )
                .await?;
                ingest_checked(&root, &mut marker, drawer.as_ref(), &allow).await?;
            }
            CheckoutCommands::Publish { directory } => {
                ensure_checkout_support(lazy::plugs_repo().await?.as_ref()).await?;
                let start = directory.unwrap_or(std::env::current_dir()?);
                let (root, mut marker) = discover(&start).await?;
                lazy::select_checkout_repo(marker.checkout().node_path()?);
                let context = lazy::repo_ctx().await?;
                verify_node(&node_handle(&context), marker.checkout()).await?;
                let drawer = lazy::drawer_repo().await?;
                verify_bindings(drawer.as_ref(), marker.checkout()).await?;
                // Publish is the upgrade point: a pre-publication (v2) marker
                // becomes a v3 marker on this verb's first write.
                marker.upgrade();
                publish_pipeline(&root, &mut marker, &drawer).await?;
            }
            CheckoutCommands::Status { directory } => {
                let start = directory.unwrap_or(std::env::current_dir()?);
                let (root, marker) = discover(&start).await?;
                lazy::select_checkout_repo(marker.checkout().node_path()?);
                let context = lazy::repo_ctx().await?;
                verify_node(&node_handle(&context), marker.checkout()).await?;
                let drawer = lazy::drawer_repo().await?;
                verify_bindings(drawer.as_ref(), marker.checkout()).await?;
                for line in status(&root, marker.checkout(), drawer.as_ref()).await? {
                    println!("{line}");
                }
            }
        }
        Ok(ExitCode::SUCCESS)
    }

    async fn verify_node(node: &NodeHandle, checkout: &Checkout) -> Res<()> {
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

    fn validate_output(path: &RelPath) -> Res<()> {
        eyre::ensure!(!path.is_root(), "checkout output cannot be the root");
        eyre::ensure!(
            path.components().first().map(String::as_str) != Some(MARKER),
            "checkout output conflicts with {MARKER}"
        );
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

    async fn create(
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

    async fn project(
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

    async fn read_marker(root: &Path) -> Res<Marker> {
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
            .ok_or_else(|| {
                eyre::eyre!("checkout marker {} has no readable version", path.display())
            })?;
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

    async fn write_initial(root: &Path, checkout: &Checkout) -> Res<()> {
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

    async fn replace_marker(root: &Path, marker: &Marker) -> Res<()> {
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

    async fn discover(start: &Path) -> Res<(PathBuf, Marker)> {
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
        let render_evidence = FileEvidence {
            length: *length,
            digest: *digest,
        };
        let primary = Tracked {
            projection: checkout.projection.clone(),
            branch: checkout.branch.clone(),
            branch_id: checkout
                .branch_id
                .clone()
                .ok_or_eyre("Ready checkout is missing its branch basis")?,
            render_evidence: Some(render_evidence),
            render_heads: Some(
                checkout
                    .render_heads
                    .clone()
                    .ok_or_eyre("Ready checkout is missing its render heads")?,
            ),
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
    async fn verify_bindings(
        drawer: &daybook_core::drawer::DrawerRepo,
        checkout: &Checkout,
    ) -> Res<()> {
        if !matches!(checkout.state, State::Ready { .. }) {
            return Ok(());
        }
        for track in tracked_bindings(checkout)? {
            let document = &track.projection.document;
            drawer.get_entry(document).await?.ok_or_eyre(format!(
                "checkout document {document} is no longer registered"
            ))?;
            let branch_ref = drawer
                .get_branch_ref(document, BranchPath::new(&track.branch))
                .await?
                .ok_or_eyre(format!(
                    "checkout branch {} is no longer registered",
                    track.branch
                ))?;
            eyre::ensure!(
                branch_ref.branch_kind == daybook_core::drawer::BranchKind::Local,
                "checkout branch {} must stay local",
                track.branch
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
        Ok((
            am_utils_rs::serialize_commit_heads(&bundle.branch_heads),
            note,
        ))
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
        let ExpectedFile::Present(evidence) = observed else {
            return Disposition::Missing;
        };
        let staged = live_heads != render_heads;
        if staged && branch_note.map(note_evidence).as_ref() == Some(evidence) {
            let confirmed = matches!(receipt, Some(receipt) if &receipt.file == evidence && receipt.branch_heads == live_heads);
            return Disposition::Ingested(confirmed);
        }
        if Some(evidence) == render_evidence {
            return if staged {
                Disposition::StagedDiverged
            } else {
                Disposition::Clean
            };
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
                if relative == Path::new(MARKER) {
                    continue;
                }
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

    async fn status(
        root: &Path,
        checkout: &Checkout,
        drawer: &daybook_core::drawer::DrawerRepo,
    ) -> Res<Vec<String>> {
        let mut lines = Vec::new();
        // Per-destination publication vocabulary (design §7): the derived
        // markers and publication lines derive from publication records;
        // publication never rewrites receipts or paths' own state.
        let latest_by_document = |document: &str| -> Option<&Publication> {
            checkout
                .publications
                .iter()
                .max_by_key(|publication| publication.seq)
                .filter(|publication| publication.document == document)
        };
        match &checkout.state {
            State::Pending { failure } => {
                lines.push(format!("incomplete {}", checkout.projection.path));
                if let Some(failure) = failure {
                    lines.push(format!("failure {failure}"));
                }
            }
            State::Ready { blocked, .. } => {
                if let Some(blocked) = blocked {
                    lines.push(match blocked.operation {
                        Operation::Ingest => format!("blocked ingest {}", blocked.failure),
                        Operation::Publish => format!("blocked publish {}", blocked.failure),
                    });
                }
                for track in tracked_bindings(checkout)? {
                    let path_key = track.projection.path.clone();
                    let rel = RelPath::parse(&path_key)?;
                    let observed = TokioFs::new(root).observe(&rel).await?;
                    let (live_heads, branch_note) = binding_state(drawer, &track).await?;
                    let render_heads = track
                        .render_heads
                        .as_ref()
                        .ok_or_eyre("Ready checkout is missing its render heads")?;
                    let receipt = checkout
                        .receipts
                        .iter()
                        .rev()
                        .find(|receipt| receipt.path == path_key);
                    let settled = disposition(
                        &observed,
                        track.render_evidence.as_ref(),
                        render_heads,
                        &live_heads,
                        branch_note.as_ref(),
                        receipt,
                    );
                    let mut line = display(&settled, &path_key);
                    // Derived published marker: the destination's record and
                    // not the receipt is the publication truth (§5).
                    // T11: the derived published marker is the destination's
                    // record's truth (§5), not the receipt's: a staged path whose
                    // record says published is published regardless of the
                    // receipt's confirming heads (publish's own intake persist
                    // legitimately leaves the receipt behind).
                    if matches!(settled, Disposition::Clean | Disposition::Ingested(_))
                        && latest_by_document(&track.projection.document)
                            .is_some_and(|publication| publication.outcome == Outcome::Published)
                    {
                        line.push_str(" published");
                    }
                    lines.push(line);
                }
            }
        }
        for claim in &checkout.pending_imports {
            lines.push(format!("unresolved import claim {claim}"));
        }
        // Per-destination publication lines: destination-scoped truth.
        let mut seen_documents = HashSet::new();
        for track in tracked_bindings(checkout)? {
            if !seen_documents.insert(track.projection.document.clone()) {
                continue;
            }
            lines.extend(
                publication_lines(
                    drawer,
                    &track,
                    latest_by_document(&track.projection.document),
                )
                .await?,
            );
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

    /// Records that the running verb blocked the checkout. A Pending checkout
    /// stays Pending: its create-phase failure already blocks everything.
    /// Best-effort marker write: the operation's own failure propagates
    /// regardless; a failed write compounds both errors explicitly.
    async fn record_blocked(
        marker: &mut Marker,
        root: &Path,
        error: &eyre::Report,
        operation: Operation,
    ) -> Res<()> {
        let State::Ready { blocked, .. } = &mut marker.checkout_mut().state else {
            return Ok(());
        };
        *blocked = Some(Blocked {
            operation,
            failure: format!("{error:#}"),
        });
        replace_marker(root, marker).await?;
        Ok(())
    }

    /// Ingest entry with the failure-blocking contract: any failure records the
    /// blocked state in the marker (best effort) before propagating. A loaded
    /// v2 marker keeps its v2 disk shape (ingest never upgrades).
    async fn ingest_checked(
        root: &Path,
        marker: &mut Marker,
        drawer: &daybook_core::drawer::DrawerRepo,
        allow: &[PathBuf],
    ) -> Res<()> {
        if let Err(error) = ingest(root, marker, drawer, allow).await {
            if let Err(marker_error) = record_blocked(marker, root, &error, Operation::Ingest).await
            {
                eyre::bail!(
                    "ingest failed: {error:#}; recording the block at {} also failed: {marker_error:#}",
                    root.join(MARKER).display()
                );
            }
            return Err(error);
        }
        Ok(())
    }

    /// Resolves import claims left by interrupted ingests. `--adopt-import-claim
    /// path doc` binds `path` to a document that was already created on main;
    /// `--drop-import-claim path` returns the path to untracked.
    async fn resolve_claims(
        root: &Path,
        marker: &mut Marker,
        drawer: &daybook_core::drawer::DrawerRepo,
        adopt: &[String],
        drop_claims: &[String],
    ) -> Res<()> {
        if adopt.is_empty() && drop_claims.is_empty() {
            return Ok(());
        }
        eyre::ensure!(
            matches!(marker.checkout().state, State::Ready { .. }),
            "incomplete (Pending) checkouts cannot resolve import claims"
        );
        for pair in adopt {
            eyre::ensure!(
                pair.split('=').count() == 2,
                "--adopt-import-claim expects PATH=DOC, got {pair:?}"
            );
            let (path_arg, document) = pair.split_once('=').expect("checked above");
            let document: daybook_types::doc::DocId = document.to_string();
            let key = allowed_key(root, Path::new(path_arg))?;
            eyre::ensure!(
                marker
                    .checkout()
                    .pending_imports
                    .iter()
                    .any(|claim| claim == &key),
                "no pending import claim for {key}"
            );
            let note_key = FacetKey::from(WellKnownFacetTag::Note);
            let dpath_key = Dpath::parse(&format!("/{key}"))?.facet_key();
            // Select exactly the dpath claim: an empty Some(Vec::new()) hydrates
            // nothing even when the facets object exists.
            let bundle = drawer
                .get_doc_bundle_at_branch(
                    &document,
                    BranchPath::new("main"),
                    Some(vec![dpath_key.clone()]),
                )
                .await?
                .ok_or_eyre(format!("document {document} is not registered on main"))?;
            eyre::ensure!(
                bundle.doc.facets.contains_key(&dpath_key),
                "document {document} does not claim {key} through its dpath facet"
            );
            let import_branch = format!("/tmp/checkout/{}/{}", marker.checkout().id, document);
            drawer
                .create_checkout_branch(
                    &document,
                    BranchPath::new(&import_branch),
                    BranchPath::new("main"),
                    &bundle.branch_heads,
                )
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
                .get_doc_bundle_at_branch(
                    &document,
                    BranchPath::new(&import_branch),
                    Some(Vec::new()),
                )
                .await?
                .ok_or_eyre("import branch is missing after adoption")?;
            let observed = TokioFs::new(root).observe(&RelPath::parse(&key)?).await?;
            let file = match &observed {
                ExpectedFile::Present(evidence) => Some(evidence.clone()),
                ExpectedFile::Absent => None,
            };
            let checkout = marker.checkout_mut();
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
                push_receipt(
                    checkout,
                    key.clone(),
                    evidence,
                    am_utils_rs::serialize_commit_heads(&bundle.branch_heads),
                );
            }
            replace_marker(root, marker).await?;
            println!("adopted import claim {key} -> {document}");
        }
        for path_arg in drop_claims {
            // Drop must not depend on file existence: a claim may outlive the
            // file it named. Claims are printed as checkout-relative keys, so
            // that exact spelling is what drop takes.
            let key = RelPath::parse(path_arg)
                .map_err(|error| {
                    eyre::eyre!(
                        "--drop-import-claim expects a checkout-relative path ({error}): {path_arg}"
                    )
                })?
                .to_string();
            eyre::ensure!(
                marker
                    .checkout()
                    .pending_imports
                    .iter()
                    .any(|claim| claim == &key),
                "no pending import claim for {key}"
            );
            marker
                .checkout_mut()
                .pending_imports
                .retain(|claim| claim != &key);
            replace_marker(root, marker).await?;
            println!("dropped import claim {key}");
        }
        Ok(())
    }

    fn push_receipt(
        checkout: &mut Checkout,
        path: String,
        file: FileEvidence,
        branch_heads: Vec<String>,
    ) {
        let sequence = checkout
            .receipts
            .iter()
            .map(|receipt| receipt.sequence)
            .max()
            .unwrap_or(0)
            + 1;
        checkout.receipts.retain(|receipt| receipt.path != path);
        checkout.receipts.push(Receipt {
            path,
            file,
            branch_heads,
            sequence,
        });
    }

    /// Resolves an `--allow` argument into a checkout-relative RelPath key.
    /// The file must exist, must be under the checkout root, and symlinks are
    /// refused before canonicalization can hide them.
    fn allowed_key(root: &Path, argument: &Path) -> Res<String> {
        let metadata = std::fs::symlink_metadata(argument)
            .wrap_err_with(|| format!("inspecting --allow argument {}", argument.display()))?;
        eyre::ensure!(
            !metadata.is_symlink(),
            "--allow refuses symlinks: {}",
            argument.display()
        );
        eyre::ensure!(
            metadata.is_file(),
            "--allow expects a regular file: {}",
            argument.display()
        );
        let real = std::fs::canonicalize(argument)?;
        let relative = real.strip_prefix(root).map_err(|_| {
            eyre::eyre!(
                "--allow path {} is not under the checkout root {}",
                real.display(),
                root.display()
            )
        })?;
        let key = TokioFs::from_native_path(relative)?;
        eyre::ensure!(
            !relative.as_os_str().is_empty(),
            "--allow cannot be the checkout root"
        );
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
        marker: &mut Marker,
        drawer: &daybook_core::drawer::DrawerRepo,
        allow: &[PathBuf],
    ) -> Res<()> {
        eyre::ensure!(
            matches!(marker.checkout().state, State::Ready { .. }),
            "checkout is incomplete (Pending); resolve it before ingesting"
        );
        if let Some(claim) = marker.checkout().pending_imports.first() {
            eyre::bail!(
                "unresolved import claim for {claim} from an interrupted ingest; \
                 resolve with --adopt-import-claim {claim}=<doc-id> or --drop-import-claim {claim} before ingesting"
            );
        }
        let tracked = tracked_bindings(marker.checkout())?;
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
                    failures.push(format!(
                        "{path_key}: recorded binding path does not parse: {error:#}"
                    ));
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
                    failures.push(format!(
                        "{path_key}: checkout branch state unavailable: {error:#}"
                    ));
                    continue;
                }
            };
            let Some(branch_note) = branch_note else {
                failures.push(format!(
                    "{path_key}: checkout Note is missing on its branch"
                ));
                continue;
            };
            let render_heads = track
                .render_heads
                .as_ref()
                .ok_or_eyre("Ready checkout is missing its render heads")?;
            let receipt = marker
                .checkout()
                .receipts
                .iter()
                .rev()
                .find(|receipt| receipt.path == path_key);
            let settled = disposition(
                &observed,
                track.render_evidence.as_ref(),
                render_heads,
                &live_heads,
                Some(&branch_note),
                receipt,
            );
            let ExpectedFile::Present(_) = &observed else {
                println!("missing {path_key}");
                continue;
            };
            match settled {
                Disposition::Clean | Disposition::Ingested(true) => {
                    println!("{}", display(&settled, &path_key))
                }
                // Ingested(false), StagedDiverged, and Modified all enter the
                // batch: the no-op rule against the live branch confirms
                // already-staged bytes or stages their revert.
                _ => {
                    entries.push((
                        FileTake {
                            path: rel,
                            expected: observed,
                        },
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
            if marker.checkout().pending_imports.contains(&key) {
                failures.push(format!(
                    "--allow {key}: unresolved import claim blocks re-import"
                ));
                continue;
            }
            let rel = match RelPath::parse(&key) {
                Ok(rel) => rel,
                Err(_) => {
                    failures.push(format!(
                        "--allow {key}: path does not convert to a checkout tree key"
                    ));
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
            entries.push((
                FileTake {
                    path: rel,
                    expected: observed,
                },
                Target::Import(key),
            ));
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
                                refreshed.push((
                                    FileTake {
                                        path: take.path,
                                        expected: observed,
                                    },
                                    target,
                                ));
                            }
                            Ok(ExpectedFile::Absent) => println!("missing {}", take.path),
                            Err(error) => {
                                eyre::bail!(
                                    "ingest failed at {} and refresh could not re-observe it: {error}; first failure: {first}",
                                    take.path
                                );
                            }
                        }
                    }
                    eyre::ensure!(
                        !refreshed.is_empty(),
                        "ingest failed and no files remained to retry: {first}"
                    );
                    let take_list: Vec<FileTake> =
                        refreshed.iter().map(|(take, _)| take.clone()).collect();
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
                    stage_edit(root, marker, drawer, &receiver, *edit, collected, note).await?;
                }
                Target::Import(key) => {
                    stage_import(root, marker, drawer, key, collected, note).await?;
                }
            }
            staged += 1;
        }

        // A completed ingest rechecked every path: blocking no longer applies.
        if let State::Ready { blocked, .. } = &mut marker.checkout_mut().state {
            *blocked = None;
        }
        replace_marker(root, marker).await?;
        println!(
            "staged {staged} document operation(s) on the checkout-local branch; nothing published upstream"
        );
        Ok(())
    }

    /// Stages one prepared edit: file bytes become the candidate Note through
    /// the raw-text lens inverse; a No-op against the live branch state only
    /// writes a confirming receipt (closing a crash window). A real difference
    /// is committed to the checkout branch at the heads it was prepared at,
    /// then recorded as a receipt.
    async fn stage_edit(
        root: &Path,
        marker: &mut Marker,
        drawer: &daybook_core::drawer::DrawerRepo,
        receiver: &TokioFs,
        edit: EditTarget,
        collected: pauperfuse::backends::tokio_fs::CollectedFile,
        // Prepared by the batch gate before any staging begins.
        note: daybook_types::doc::Note,
    ) -> Res<()> {
        let EditTarget {
            track,
            live_heads,
            branch_note,
        } = edit;
        let path_key = track.projection.path.clone();
        if note == branch_note {
            // Already staged (round-trip stability, ADR 012 §8): only a
            // confirming receipt is written when one is missing or behind.
            let receipt = marker
                .checkout()
                .receipts
                .iter()
                .rev()
                .find(|receipt| receipt.path == path_key);
            let confirmed = matches!(receipt, Some(receipt) if receipt.file == collected.evidence && receipt.branch_heads == live_heads);
            if !confirmed {
                push_receipt(
                    marker.checkout_mut(),
                    path_key.clone(),
                    collected.evidence,
                    live_heads,
                );
                replace_marker(root, marker).await?;
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
        eyre::ensure!(
            fresh_heads != live_heads,
            "staging {path_key} did not advance the checkout branch"
        );
        push_receipt(
            marker.checkout_mut(),
            path_key.clone(),
            collected.evidence,
            fresh_heads,
        );
        replace_marker(root, marker).await?;
        println!("ingested {path_key}");
        Ok(())
    }

    /// Stages one `--allow` import: claim the path first so an interrupted
    /// ingest can never duplicate a document identity, then create the
    /// document on main, fork its checkout-local branch, and record the
    /// binding plus receipt.
    async fn stage_import(
        root: &Path,
        marker: &mut Marker,
        drawer: &daybook_core::drawer::DrawerRepo,
        key: String,
        collected: pauperfuse::backends::tokio_fs::CollectedFile,
        // Prepared by the batch gate before any staging begins.
        note: daybook_types::doc::Note,
    ) -> Res<()> {
        marker.checkout_mut().pending_imports.push(key.clone());
        replace_marker(root, marker).await?;

        let note_key = FacetKey::from(WellKnownFacetTag::Note);
        let note_url = build_facet_ref("self", &note_key)?;
        let args = daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: HashMap::from([
                (
                    note_key.clone(),
                    serde_json::Value::from(WellKnownFacet::Note(note.clone())),
                ),
                (
                    FacetKey::from(WellKnownFacetTag::Body),
                    serde_json::Value::from(WellKnownFacet::Body(daybook_types::doc::Body {
                        order: vec![note_url],
                    })),
                ),
                (
                    Dpath::parse(&format!("/{key}"))?.facet_key(),
                    serde_json::json!({}),
                ),
            ]),
            user_path: None,
        };
        let document = drawer.add(args).await.map_err(eyre::Report::from)?;
        let (_, heads) = drawer
            .get_with_heads(&document, BranchPath::new("main"), None)
            .await?
            .ok_or_eyre(format!(
                "imported document {document} is missing on main after creation"
            ))?;
        let import_branch = format!("/tmp/checkout/{}/{}", marker.checkout().id, document);
        drawer
            .create_checkout_branch(
                &document,
                BranchPath::new(&import_branch),
                BranchPath::new("main"),
                &heads,
            )
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
        let checkout = marker.checkout_mut();
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
        push_receipt(
            checkout,
            key.clone(),
            collected.evidence,
            am_utils_rs::serialize_commit_heads(&bundle.branch_heads),
        );
        replace_marker(root, marker).await?;
        println!("imported {key} -> {document}");
        Ok(())
    }

    /// Per-destination publication lines (design §7): published records with
    /// their heads, upstream divergence, and pending destinations.
    async fn publication_lines(
        drawer: &daybook_core::drawer::DrawerRepo,
        track: &Tracked,
        latest: Option<&Publication>,
    ) -> Res<Vec<String>> {
        let document = &track.projection.document;
        let Some(publication) = latest else {
            return Ok(vec![]);
        };
        let (_, main_heads) = drawer
            .get_with_heads(&track.projection.document, BranchPath::new("main"), None)
            .await?
            .ok_or_eyre(format!("document {document} missing from main"))?;
        let live_main_heads = am_utils_rs::serialize_commit_heads(&main_heads);
        match (&publication.outcome, &publication.published_heads) {
            (&Outcome::Published, Some(published)) if *published == live_main_heads => {
                Ok(vec![format!(
                    "publish {document} published {}",
                    published.join(" ")
                )])
            }
            (&Outcome::Published, Some(published)) => Ok(vec![
                format!("publish {document} published {}", published.join(" ")),
                format!(
                    "publish {document} upstream moved since publish ({} -> {})",
                    published.join(" "),
                    live_main_heads.join(" ")
                ),
            ]),
            // validate() makes a published record without recorded heads
            // unrepresentable; loading enforces the invariant.
            (&Outcome::Published, &None) => {
                unreachable!("published records always carry publishedHeads")
            }
            (Outcome::Blocked, ..) => Ok(vec![format!(
                "publish {document} blocked: {}",
                publication
                    .failure
                    .clone()
                    .unwrap_or_else(|| "invalid merge candidate".into())
            )]),
            (Outcome::Refused, ..) => Ok(vec![format!(
                "publish {document} refused: {}",
                publication
                    .failure
                    .clone()
                    .unwrap_or_else(|| "publish was refused".into())
            )]),
        }
    }

    /// The merged candidate's interpretation, re-run at the candidate heads
    /// (ADR 011 §6 step 6; ADR 012 §8 invariants re-expressed on the hydrated
    /// candidate facet map — no daybook_core surface beyond the candidate
    /// APIs is used, per lane C being held). A merged state that retargets
    /// the recorded binding is invalid, not a retargeting (the checkout's
    /// binding identity cannot drift under publication).
    fn validate_candidate_projection(
        candidate: &daybook_core::drawer::types::MergeCandidate,
        track: &Tracked,
    ) -> Res<()> {
        let mut dpaths = Vec::new();
        for (key, value) in &candidate.facets {
            let Some(path) = Dpath::parse_facet_key(key) else {
                continue;
            };
            let path =
                path.map_err(|error| eyre::eyre!("candidate dpath does not parse: {error}"))?;
            let scope = DpathFacet::from_json_value(value)
                .map_err(|error| eyre::eyre!("candidate dpath facet does not parse: {error}"))?;
            eyre::ensure!(
                scope.is_whole_document(),
                "candidate merges to a selective dpath; publication supports only the whole-document interpretation"
            );
            dpaths.push(path);
        }
        eyre::ensure!(
            dpaths.len() == 1,
            "candidate must merge to exactly one whole-document dpath"
        );
        let body_key = FacetKey::from(WellKnownFacetTag::Body);
        let body = candidate
            .facets
            .get(&body_key)
            .ok_or_else(|| eyre::eyre!("candidate merge loses the Body facet"))?;
        let facet: WellKnownFacet = serde_json::from_value(body.clone())?;
        let WellKnownFacet::Body(body) = facet else {
            eyre::bail!("candidate Body facet has the wrong shape");
        };
        eyre::ensure!(
            body.order.len() == 1,
            "candidate Body must select exactly one Note"
        );
        let url = &body.order[0];
        // Existing Daybook URL parsing does not percent-decode. Never silently misresolve it.
        eyre::ensure!(
            url.path().is_ascii() && !url.path().contains('%') && !url.path().contains('\\'),
            "encoded/non-ASCII candidate Body references await the facet-URL correction"
        );
        let reference = daybook_types::url::parse_facet_ref(url)?;
        eyre::ensure!(
            reference.doc_id == "self" || reference.doc_id == track.projection.document,
            "candidate merges a cross-document Body reference"
        );
        eyre::ensure!(
            reference.branch.is_none() && reference.at.is_none(),
            "candidate merges a pinned Body reference"
        );
        let value = candidate
            .facets
            .get(&reference.facet_key)
            .ok_or_else(|| eyre::eyre!("candidate Body's Note is missing"))?;
        validate_note(value)?;
        let path = dpaths
            .pop()
            .unwrap()
            .segments()
            .collect::<Vec<_>>()
            .join("/");
        RelPath::parse(&path)?;
        validate_output(&RelPath::parse(&path)?)?;
        eyre::ensure!(
            path == track.projection.path,
            "candidate merge retargets the binding's dpath: recorded {} became {path}",
            track.projection.path
        );
        eyre::ensure!(
            reference.facet_key == track.projection.facet,
            "candidate merge retargets the binding's facet"
        );
        Ok(())
    }

    /// Settlement gate per binding path, before anything publishes: the disk
    /// file must be exactly the latest receipt's staged bytes (§1.1 step 2
    /// settled-staged); "receipt heads == live heads" is honored as such for
    /// the plain staged case, and the §4.2 row-2 resume window (live heads
    /// advanced by publish's own persist) rides along by content: staged
    /// bytes still live on the branch, or the intake already superseded the
    /// staged facet and the candidate loop re-validates from live heads. A
    /// checkout branch is single-writer in this slice, so byte-equality is
    /// the whole story. Returns the live checkout-branch heads.
    async fn publish_settlement(
        drawer: &daybook_core::drawer::DrawerRepo,
        receiver: &TokioFs,
        track: &Tracked,
        receipt: Option<&Receipt>,
    ) -> Res<Vec<String>> {
        let path_key = track.projection.path.clone();
        let rel = RelPath::parse(&path_key)?;
        let observed = receiver
            .observe(&rel)
            .await
            .wrap_err_with(|| format!("observing {path_key}"))?;
        let ExpectedFile::Present(observed_evidence) = observed else {
            eyre::bail!("missing {path_key}; ingest must complete before publish");
        };
        let Some(receipt) = receipt else {
            eyre::bail!("{path_key} ingested (unconfirmed); ingest must complete before publish");
        };
        eyre::ensure!(
            receipt.file == observed_evidence,
            "modified {path_key}; ingest must complete before publish"
        );
        let (_, branch_note) = binding_state(drawer, track).await?;
        // The receipt ties the disk bytes to the staged Note; the branch's
        // live Note (not its receipt heads) is what carries the staged work
        // through publish's own intake persist, which legitimately advances
        // the branch past the receipt (§4.2 row-2 resume window). Staged
        // evidence absent from the branch is staging loss and refuses.
        eyre::ensure!(
            branch_note.as_ref().map(note_evidence).as_ref() == Some(&observed_evidence),
            "staged work for {path_key} is no longer on its branch; ingest must complete before publish"
        );
        let (live_heads, _) = binding_state(drawer, track).await?;
        Ok(live_heads)
    }

    /// The §2.2 loop's wait in the persist→publish window: consume one racing
    /// writer's signal for this destination, or proceed at once when no
    /// racer is armed (absent receiver) or its sender is gone (channel
    /// closed). See [`PUBLISH_RACE_RECEIVERS`].
    #[cfg(test)]
    async fn publish_race_wait(document: &str) {
        // Taken out for the await: the lock guard must not cross it.
        let mut taken = {
            let mut lock = PUBLISH_RACE_CHANNELS
                .get_or_init(|| std::sync::Mutex::new(HashMap::new()))
                .lock()
                .expect("publish race registry");
            lock.remove(document)
        };
        if let Some(channel) = taken.as_mut() {
            if channel.requests.is_closed() {
                // The racer is gone (its race budget is spent); the window is
                // empty from here on.
            } else {
                channel
                    .requests
                    .send(())
                    .expect("racing channel must stay open until the racer exits");
                // Wait for the racer's post-commit signal; a closed channel is
                // a racer that failed or exited mid-window.
                let _signal = channel.signals.recv().await;
            }
        }
        if let Some(channel) = taken {
            PUBLISH_RACE_CHANNELS
                .get()
                .expect("armed above")
                .lock()
                .expect("publish race registry")
                .insert(document.to_string(), channel);
        }
    }

    /// The §2.2 loop for one destination: fresh heads each attempt, candidate
    /// re-validated at each attempt's basis, validated persist through the
    /// target CAS, publish through the upstream CAS, bounded retries. The
    /// validated candidate's persist re-executes the same CRDT merge, so the
    /// published state is exactly the validated one.
    async fn publish_destination(
        drawer: &daybook_core::drawer::DrawerRepo,
        destination: &Destination,
    ) -> Res<DestinationOutcome> {
        let track = &destination.track;
        let document = &track.projection.document;
        let checkout_branch = BranchPath::new(&track.branch);
        let mut attempts: u32 = 0;
        loop {
            // §2.2: fresh reads each attempt; §1.1 step 4's heads reads.
            let (checkout_heads, main_heads) = {
                let checkout_bundle = drawer
                    .get_doc_bundle_at_branch(document, checkout_branch, Some(Vec::new()))
                    .await?
                    .ok_or_else(|| eyre::eyre!("checkout branch {checkout_branch} is missing"))?;
                let main_bundle = drawer
                    .get_doc_bundle_at_branch(document, BranchPath::new("main"), Some(Vec::new()))
                    .await?
                    .ok_or_else(|| eyre::eyre!("document {document} is not registered on main"))?;
                (checkout_bundle.branch_heads, main_bundle.branch_heads)
            };
            let serialized_h = am_utils_rs::serialize_commit_heads(&main_heads);
            let candidate = match drawer
                .prepare_merge_candidate(
                    document,
                    checkout_branch,
                    BranchPath::new("main"),
                    &main_heads,
                )
                .await
            {
                Ok(candidate) => candidate,
                Err(daybook_core::drawer::types::DrawerError::HeadConcurrency { .. }) => {
                    unreachable!("candidate preparation commits nothing and cannot refuse the CAS")
                }
                Err(error) => {
                    return Ok(DestinationOutcome::Refused {
                        expected_heads: Some(serialized_h),
                        failure: format!("candidate preparation refused for {document}: {error:#}"),
                    });
                }
            };
            if let Err(error) = validate_candidate_projection(&candidate, track) {
                return Ok(DestinationOutcome::Blocked {
                    expected_heads: serialized_h,
                    failure: format!("invalid merge candidate for {document}: {error:#}"),
                    attempts: attempts.max(1),
                });
            }
            if let Err(error) = drawer.validate_merge_candidate(&candidate).await {
                return Ok(DestinationOutcome::Blocked {
                    expected_heads: serialized_h,
                    failure: format!(
                        "candidate for {document} failed facet schema validation: {error:#}"
                    ),
                    attempts: attempts.max(1),
                });
            }
            // §1.1 step 7: validated candidate persists into the checkout
            // branch under the target-side CAS on its live heads.
            match drawer
                .merge_from_heads(
                    document,
                    checkout_branch,
                    Some(&checkout_heads),
                    BranchPath::new("main"),
                    &main_heads,
                    None,
                )
                .await
            {
                Ok(()) => {
                    #[cfg(test)]
                    publish_race_wait(document).await;
                }
                Err(daybook_core::drawer::types::DrawerError::HeadConcurrency {
                    expected,
                    actual,
                    ..
                }) => {
                    attempts += 1;
                    if attempts >= PUBLISH_MAX_CAS_ATTEMPTS {
                        return Ok(DestinationOutcome::Blocked {
                            expected_heads: serialized_h,
                            failure: format!(
                                "the checkout branch left the persist basis (expected {expected}, actual {actual}); the single-writer contract broke or staged work is contested"
                            ),
                            attempts,
                        });
                    }
                    continue;
                }
                Err(error) => {
                    return Ok(DestinationOutcome::Refused {
                        expected_heads: Some(serialized_h),
                        failure: format!("persisting the validated candidate refused: {error:#}"),
                    });
                }
            }
            let branch_heads_persisted = drawer
                .get_doc_bundle_at_branch(document, checkout_branch, Some(Vec::new()))
                .await?
                .ok_or_else(|| {
                    eyre::eyre!("checkout branch {checkout_branch} is missing after persist")
                })?
                .branch_heads;
            // §1.1 step 8: publish only if upstream is still at H; refusal
            // means others moved upstream, retrying with fresh heads.
            match drawer
                .merge_from_heads(
                    document,
                    BranchPath::new("main"),
                    Some(&main_heads),
                    checkout_branch,
                    &branch_heads_persisted,
                    None,
                )
                .await
            {
                Ok(()) => {
                    let published_heads = drawer
                        .get_doc_bundle_at_branch(
                            document,
                            BranchPath::new("main"),
                            Some(Vec::new()),
                        )
                        .await?
                        .ok_or_else(|| {
                            eyre::eyre!("document {document} missing from main after publish")
                        })?
                        .branch_heads;
                    return Ok(DestinationOutcome::Published {
                        expected_heads: am_utils_rs::serialize_commit_heads(&main_heads),
                        branch_heads: am_utils_rs::serialize_commit_heads(&branch_heads_persisted),
                        published_heads: am_utils_rs::serialize_commit_heads(&published_heads),
                        attempts: attempts + 1,
                    });
                }
                Err(daybook_core::drawer::types::DrawerError::HeadConcurrency {
                    expected,
                    actual,
                    ..
                }) => {
                    attempts += 1;
                    if attempts >= PUBLISH_MAX_CAS_ATTEMPTS {
                        return Ok(DestinationOutcome::Blocked {
                            expected_heads: am_utils_rs::serialize_commit_heads(&main_heads),
                            failure: format!(
                                "upstream main kept moving (expected {expected}, actual {actual}); the upstream environment is changing under the publish"
                            ),
                            attempts,
                        });
                    }
                    continue;
                }
                Err(error) => {
                    return Ok(DestinationOutcome::Refused {
                        expected_heads: Some(am_utils_rs::serialize_commit_heads(&main_heads)),
                        failure: format!("publishing {document} to main refused: {error:#}"),
                    });
                }
            }
        }
    }

    /// §1.1 step 10 for a published destination: where the merged render
    /// differs from the acknowledged disk state, project the new bytes
    /// through the exact create-path machinery and advance the binding's
    /// render evidence; a verified no-op advances nothing. Publication
    /// records are never rewritten by this step, and receipts stay
    /// staging-scoped.
    async fn project_published(
        receiver: &TokioFs,
        marker: &mut Marker,
        drawer: &Arc<daybook_core::drawer::DrawerRepo>,
        destination: &Destination,
        published_heads: Vec<String>,
    ) -> Res<()> {
        let track = &destination.track;
        let published_heads: ChangeHashSet =
            ChangeHashSet(am_utils_rs::parse_commit_heads(&published_heads)?);
        let bundle = drawer
            .get_doc_bundle_at_branch(
                &track.projection.document,
                BranchPath::new("main"),
                Some(vec![track.projection.facet.clone()]),
            )
            .await?
            .ok_or_eyre("published document missing from main at its published heads")?;
        let Some(value) = bundle.doc.facets.get(&track.projection.facet) else {
            eyre::bail!("published merge lost the bound Note facet");
        };
        let rendered = validate_note(value)?;
        let rendered_evidence = note_evidence(&rendered);
        let rel = RelPath::parse(&track.projection.path)?;
        let observed = receiver.observe(&rel).await?;
        if observed == ExpectedFile::Present(rendered_evidence.clone()) {
            // Verified no-op: the merged render equals the disk bytes.
            return Ok(());
        }
        // A re-render may only replace this binding's last acknowledged state:
        // the receipt's staged bytes when a receipt exists (the acknowledged
        // disk state after ingest), else the recorded render evidence. Anything
        // else is user divergence publication never accounted for (F6) and
        // refuses without overwriting; prepare/apply re-check the expected
        // evidence at rename time, so a concurrent change in the window also
        // refuses.
        let receipt = marker
            .checkout()
            .receipts
            .iter()
            .rev()
            .find(|receipt| receipt.path == track.projection.path)
            .map(|receipt| receipt.file.clone());
        let acknowledged = receipt.or(track.render_evidence.clone());
        match observed {
            ExpectedFile::Absent => {}
            ExpectedFile::Present(evidence) => eyre::ensure!(
                acknowledged.as_ref() == Some(&evidence),
                "dirty target {}: the disk holds bytes from an unacknowledged state; ingest must complete before publish",
                rel
            ),
        }
        let expected = acknowledged
            .map(ExpectedFile::Present)
            .unwrap_or(ExpectedFile::Absent);
        let producer = Daybook::new(
            Arc::clone(drawer),
            marker.checkout().backend(),
            track.projection.clone(),
            "main".to_string(),
            published_heads.clone(),
        );
        let store_path = marker.checkout().store_path()?;
        tokio::fs::create_dir_all(store_path.parent().unwrap()).await?;
        let store = VtreeStore::open(&store_path).await?;
        let registered = store.register(producer.id()).await?;
        let version = store
            .replace(registered, &mut producer.observe().await?)
            .await?;
        let mut scan = store.scan(version, NonZeroU32::new(512).unwrap());
        let mut puts = Vec::new();
        while let Some(entry) = scan.next_entry().await? {
            if let Description::File { source, .. } = entry.description {
                puts.push(FilePut {
                    path: entry.path,
                    source,
                    expected: expected.clone(),
                });
            }
        }
        eyre::ensure!(
            puts.len() == 1,
            "single-document text projection must describe exactly one file"
        );
        let mut prepared = receiver
            .prepare(puts, &ProducerAccess::new(&producer))
            .await?;
        let applied = prepared.apply().await;
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
            installed.path == rel,
            "installed output differs from the recorded binding"
        );
        let checkout = marker.checkout_mut();
        if track.projection.path == checkout.projection.path {
            checkout.render_heads = Some(am_utils_rs::serialize_commit_heads(&published_heads));
            checkout.state = State::Ready {
                length: installed.evidence.length,
                digest: installed.evidence.digest,
                generation: version.generation,
                blocked: None,
            };
        } else if let Some(binding) = checkout
            .imports
            .iter_mut()
            .find(|import| import.projection.path == track.projection.path)
        {
            binding.render_heads = am_utils_rs::serialize_commit_heads(&published_heads);
            binding.file = Some(installed.evidence.clone());
        } else {
            eyre::bail!(
                "published re-render has no tracked binding: {}",
                track.projection.path
            );
        }
        Ok(())
    }

    /// Per-destination result of the §2.2 loop.
    enum DestinationOutcome {
        Published {
            /// The upstream heads the candidate was validated against (the
            /// final attempt's basis).
            expected_heads: Vec<String>,
            /// Checkout branch heads after the validated persist.
            branch_heads: Vec<String>,
            published_heads: Vec<String>,
            attempts: u32,
        },
        /// Invalid candidate (F2) or CAS exhaustion (F3). The checkout-level
        /// block records `publish`; a retry resumes after fixing the cause.
        Blocked {
            expected_heads: Vec<String>,
            failure: String,
            attempts: u32,
        },
        /// Denied or downstream failure (F4/F5); local bytes and staged
        /// history are untouched.
        Refused {
            expected_heads: Option<Vec<String>>,
            failure: String,
        },
    }

    struct Destination {
        track: Tracked,
    }

    async fn publish_pipeline(
        root: &Path,
        marker: &mut Marker,
        drawer: &Arc<daybook_core::drawer::DrawerRepo>,
    ) -> Res<()> {
        let checkout = marker.checkout_mut();
        eyre::ensure!(
            matches!(checkout.state, State::Ready { .. }),
            "checkout is incomplete (Pending); complete create before publish"
        );
        if let State::Ready {
            blocked: Some(blocked),
            ..
        } = &checkout.state
        {
            // An ingest block must be cleared by the operation that resolves
            // it (a successful ingest); publish's own explicit retry is the
            // operation that resumes a publish block (ADR 011 §7: the
            // whole-checkout rule gates automatic flows, and the retry is the
            // resolution path the block's diagnostic names).
            eyre::ensure!(
                blocked.operation == Operation::Publish,
                "ingest block stands: {}; resolve it first, then retry",
                blocked.failure
            );
        }
        if let Some(claim) = checkout.pending_imports.first().cloned() {
            eyre::bail!(
                "unresolved import claim for {claim} from an interrupted ingest; \
                 resolve with --adopt-import-claim {claim}=<doc-id> or --drop-import-claim {claim} before publishing"
            );
        }

        // §1.1 step 2–3: the settlement gate runs for the whole batch before
        // anything publishes (refusing names the recovery verb per path).
        // Pendingness (step 3's skip rule) is settled-state equality: a
        // record whose branch and upstream heads are both unchanged since it
        // was written needs no work; fresh staged work or an upstream move
        // re-enters the loop (the record skip is the §4.2 resume rule, not a
        // staging barrier).
        let receiver = TokioFs::new(root);
        let tracked = tracked_bindings(marker.checkout())?;
        let mut failures: Vec<String> = Vec::new();
        let mut destinations: Vec<Destination> = Vec::new();
        for track in &tracked {
            let path_key = track.projection.path.clone();
            let receipt = marker
                .checkout()
                .receipts
                .iter()
                .rev()
                .find(|receipt| receipt.path == path_key)
                .cloned();
            let checkout_live_heads =
                match publish_settlement(drawer, &receiver, track, receipt.as_ref()).await {
                    Ok(heads) => heads,
                    Err(error) => {
                        failures.push(format!("{error:#}"));
                        continue;
                    }
                };
            // §4.2 resume rule first: a destination whose latest publication
            // record still matches both sides' live heads is settled and
            // needs no work. Fresh staged work or an upstream move re-enters
            // the loop — the record skip is a resume rule, not a staging
            // barrier.
            let document = track.projection.document.clone();
            let latest_published = marker
                .checkout()
                .publications
                .iter()
                .filter(|publication| {
                    publication.document == document && publication.outcome == Outcome::Published
                })
                .max_by_key(|publication| publication.seq);
            let settled = if let Some(record) = latest_published {
                let live_main_heads = drawer
                    .get_with_heads(&document, BranchPath::new("main"), None)
                    .await?
                    .map(|(_, heads)| am_utils_rs::serialize_commit_heads(&heads))
                    .ok_or_eyre(format!("document {document} is not registered on main"))?;
                record.branch_heads == checkout_live_heads
                    && record.published_heads.as_deref() == Some(live_main_heads.as_slice())
            } else {
                false
            };
            if settled || track.render_heads.as_deref() == Some(checkout_live_heads.as_slice()) {
                // A settled publication record, or no staged work beyond the
                // recorded render basis: nothing to publish (the §7 no-op
                // honesty — publish never ingests, so upstream intake alone
                // is not this verb's business).
                continue;
            }
            destinations.push(Destination {
                track: track.clone(),
            });
        }
        if !failures.is_empty() {
            eyre::bail!("publish refused:\n  {}", failures.join("\n  "));
        }
        if destinations.is_empty() {
            println!("nothing to publish");
            return Ok(());
        }

        let mut not_published: Vec<String> = Vec::new();
        for destination in &destinations {
            let document = destination.track.projection.document.clone();
            match publish_destination(drawer, destination).await? {
                DestinationOutcome::Published {
                    expected_heads,
                    branch_heads,
                    published_heads,
                    attempts,
                } => {
                    {
                        let checkout = marker.checkout_mut();
                        let seq = checkout
                            .publications
                            .iter()
                            .map(|publication| publication.seq)
                            .max()
                            .unwrap_or(0)
                            + 1;
                        checkout.publications.push(Publication {
                            seq,
                            document: document.clone(),
                            expected_heads,
                            branch_heads,
                            published_heads: Some(published_heads.clone()),
                            outcome: Outcome::Published,
                            failure: None,
                            attempts,
                        });
                    }
                    replace_marker(root, marker).await?;
                    // §1.1 step 10: the published state re-renders when the
                    // merged bytes differ from the acknowledged disk state.
                    project_published(&receiver, marker, drawer, destination, published_heads)
                        .await?;
                    let path = destination.track.projection.path.clone();
                    println!("published {path}");
                }
                other => {
                    let (outcome, failure, expected_heads, attempts) = match other {
                        DestinationOutcome::Blocked {
                            expected_heads,
                            failure,
                            attempts,
                        } => (Outcome::Blocked, failure, Some(expected_heads), attempts),
                        DestinationOutcome::Refused {
                            expected_heads,
                            failure,
                        } => (Outcome::Refused, failure, expected_heads, 1),
                        DestinationOutcome::Published { .. } => unreachable!("matched above"),
                    };
                    let diag = format!("{failure:#}; explicit retry resumes this destination");
                    {
                        let checkout = marker.checkout_mut();
                        let seq = checkout
                            .publications
                            .iter()
                            .map(|publication| publication.seq)
                            .max()
                            .unwrap_or(0)
                            + 1;
                        checkout.publications.push(Publication {
                            seq,
                            document: document.clone(),
                            expected_heads: expected_heads.unwrap_or_default(),
                            branch_heads: Vec::new(),
                            published_heads: None,
                            outcome,
                            failure: Some(diag),
                            attempts,
                        });
                        // The checkout-level block (F2/F3/F4/F5) marks the
                        // batch incomplete; staged work is preserved.
                        if let State::Ready { blocked, .. } = &mut checkout.state {
                            *blocked = Some(Blocked {
                                operation: Operation::Publish,
                                failure: format!("{document}: {failure:#}"),
                            });
                        }
                    }
                    replace_marker(root, marker).await?;
                    not_published.push(document);
                }
            }
        }
        if !not_published.is_empty() {
            eyre::bail!(
                "publish incomplete: {} not published; the checkout blocks with its staged work preserved; fix and retry",
                not_published.join(", ")
            );
        }
        // The explicit retry succeeded for every destination: the block the
        // previous run recorded no longer stands.
        let State::Ready { blocked, .. } = &mut marker.checkout_mut().state else {
            unreachable!("the preflight gate requires a Ready checkout");
        };
        if blocked.is_some() {
            // The retry succeeded for every destination: the block the
            // previous run recorded no longer stands.
            *blocked = None;
            replace_marker(root, marker).await?;
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests;
}
