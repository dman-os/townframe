use super::*;

pub(super) const MARKER: &str = ".daybook-checkout";
pub(super) const VERSION: u32 = 3;

/// Retry bound for the publish-side expected-heads CAS against concurrent
/// upstream writers (ADR 011 §6 step 8: a finite bound, not a backoff; on
/// exhaustion the checkout blocks with its staged work preserved and an
/// explicit retry resumes). The upstream environment is what keeps
/// moving; racing it forever cannot be what "publish" means.
pub(super) const PUBLISH_MAX_CAS_ATTEMPTS: u32 = 3;

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
pub(super) static PUBLISH_RACE_CHANNELS: std::sync::OnceLock<
    std::sync::Mutex<HashMap<String, PublishRaceChannel>>,
> = std::sync::OnceLock::new();

/// The loop→racer handshake for one armed destination: the publish loop
/// sends a request at the persist→publish window; the racer commits its
/// upstream movement at main's live heads and signals back.
#[cfg(test)]
pub(super) struct PublishRaceChannel {
    pub(super) requests: tokio::sync::mpsc::UnboundedSender<()>,
    pub(super) signals: tokio::sync::mpsc::UnboundedReceiver<()>,
}

/// Which flow a block was recorded by. Ingest blocking pre-dates
/// publication (v2 markers carried a bare failure) and its vocabulary
/// stays `ingest`.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Operation {
    Ingest,
    Publish,
}

/// Publication outcome vocabulary, per design §4.1's spellings.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Outcome {
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
pub(super) struct Publication {
    /// Per-checkout monotonic, same last-writer-wins counter as receipts.
    pub(super) seq: u64,
    /// Destination document id (later batches: per destination incl.
    /// cross-doc facet targets).
    #[serde(rename = "doc")]
    pub(super) document: String,
    /// The upstream heads this attempt's candidate was validated against.
    #[serde(rename = "expectedHeads")]
    pub(super) expected_heads: Vec<String>,
    /// The checkout branch heads after the validated candidate persisted.
    #[serde(rename = "branchHeads")]
    pub(super) branch_heads: Vec<String>,
    /// Main heads after the CAS merge; `null` when not published.
    #[serde(rename = "publishedHeads")]
    pub(super) published_heads: Option<Vec<String>>,
    pub(super) outcome: Outcome,
    /// `refused`/`blocked` diagnostic; null for published.
    pub(super) failure: Option<String>,
    /// CAS attempts consumed in this run.
    pub(super) attempts: u32,
}

/// Ingest is the single writer for a checkout only while one CLI process
/// runs it; no cross-process lock exists in this slice. Concurrent CLI
/// invocations against one checkout are a user error until the watch lane
/// introduces real locking. Marker v2 documents this contract.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum State {
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
pub(super) struct Blocked {
    pub(super) operation: Operation,
    pub(super) failure: String,
}

/// The landed v2 blocking record: ingest blocks only, no operation tag.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct BlockedV2 {
    pub(super) failure: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum StateV2 {
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
pub(super) struct CheckoutV2 {
    pub(super) version: u32,
    pub(super) id: Uuid,
    pub(super) node_path: String,
    pub(super) node_key: String,
    pub(super) drawer: String,
    pub(super) projection: Projection,
    pub(super) imports: Vec<Import>,
    pub(super) basis: Vec<String>,
    pub(super) branch: String,
    pub(super) branch_id: Option<String>,
    pub(super) render_heads: Option<Vec<String>>,
    pub(super) state: StateV2,
    pub(super) receipts: Vec<Receipt>,
    pub(super) pending_imports: Vec<String>,
}

/// Durable receipt mapping one ingested file's evidence to the
/// checkout-branch heads it was staged at. A strictly local checkout fact:
/// never a vtree row, never an acknowledgement of upstream publication.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Receipt {
    /// RelPath key of the bound output, matching the projection spelling.
    pub(super) path: String,
    /// Evidence of the exact bytes accepted into the branch.
    pub(super) file: FileEvidence,
    /// Serialized commit heads of the owning branch AFTER this staging.
    pub(super) branch_heads: Vec<String>,
    /// Monotonic per-checkout counter; a receipt replaces any earlier
    /// receipt for the same path.
    pub(super) sequence: u64,
}

/// A binding created by an `--allow` import: a document whose first
/// acknowledged disk state is the imported file itself. Its branch was
/// forked from main at the importing add, so it holds no other staged work.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Import {
    pub(super) projection: Projection,
    pub(super) branch: String,
    pub(super) branch_id: String,
    /// Heads the import branch was forked at; the recorded disk evidence
    /// corresponds to this state.
    pub(super) render_heads: Vec<String>,
    /// Evidence of the file at import time; `None` if the file was absent
    /// when the binding was adopted, so there is no known render evidence.
    pub(super) file: Option<FileEvidence>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Checkout {
    pub(super) version: u32,
    pub(super) id: Uuid,
    pub(super) node_path: String,
    pub(super) node_key: String,
    pub(super) drawer: String,
    pub(super) projection: Projection,
    /// Additional bindings created by ingest imports (see `Import`).
    pub(super) imports: Vec<Import>,
    pub(super) basis: Vec<String>,
    pub(super) branch: String,
    pub(super) branch_id: Option<String>,
    pub(super) render_heads: Option<Vec<String>>,
    pub(super) state: State,
    pub(super) receipts: Vec<Receipt>,
    /// Claimed import paths from an interrupted ingest. Presence blocks all
    /// ingestion: the document may or may not exist, and re-importing would
    /// duplicate its identity. Resolution is explicit (see the command flags).
    pub(super) pending_imports: Vec<String>,
    /// Per-destination publication records (design §4.1). v3-only: empty
    /// until this checkout's first publish.
    pub(super) publications: Vec<Publication>,
}

/// A loaded checkout marker, explicit about its on-disk version: the
/// runtime fields are always the current (v3) shape — a v2 marker loads
/// into that shape with an inferred `ingest`-only `operation` and no
/// publications — while writes re-serialize the disk version's exact
/// layout. Only the publish verb upgrades a marker (on write).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Marker {
    pub(super) checkout: Checkout,
    pub(super) disk_version: u32,
}

impl CheckoutV2 {
    /// Loads the landed v2 layout into the runtime (v3) field shape. The
    /// layout cannot record an operation: its blocked record is ingest's
    /// by construction (publish did not exist on v2).
    pub(super) fn runtime_shape(self) -> Checkout {
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
    pub(super) fn checkout(&self) -> &Checkout {
        &self.checkout
    }
    pub(super) fn checkout_mut(&mut self) -> &mut Checkout {
        &mut self.checkout
    }

    /// Publish's upgrade point: the marker's disk layout becomes v3.
    /// A converted v2 record keeps its blocking state with the operation
    /// the layout could not name (ingest, the only v2 blocker).
    pub(super) fn upgrade(&mut self) {
        if self.disk_version == VERSION {
            return;
        }
        self.checkout.version = VERSION;
        self.disk_version = VERSION;
    }

    /// The marker as v2 layout bytes, for write-back preserving a loaded
    /// v2 marker's shape. Blocked drops its operation tag (v2 blocked is
    /// ingest-only by construction), publications have no key.
    pub(super) fn v2_shape(&self) -> CheckoutV2 {
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
    pub(super) fn backend(&self) -> BackendId {
        BackendId(format!("daybook:{}:{}", self.node_key, self.id))
    }
    pub(super) fn node_path(&self) -> Res<PathBuf> {
        let bytes = utils_rs::byte_key::decode(&self.node_path)?;
        eyre::ensure!(!bytes.contains(&0), "NUL in recorded node path");
        let path = PathBuf::from(std::ffi::OsString::from_vec(bytes));
        eyre::ensure!(path.is_absolute(), "recorded node path is not absolute");
        Ok(path)
    }
    pub(super) fn store_path(&self) -> Res<PathBuf> {
        Ok(self
            .node_path()?
            .join("local_state/checkouts")
            .join(self.id.to_string())
            .join("vtree.sqlite"))
    }
    pub(super) fn validate(&self) -> Res<()> {
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
