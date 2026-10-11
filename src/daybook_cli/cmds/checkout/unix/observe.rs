use super::*;

/// One tracked output binding and the recorded render basis it is compared
/// against. The primary projection comes first; imports follow.
#[derive(Clone, Debug)]
pub(super) struct Tracked {
    pub(super) projection: Projection,
    pub(super) branch: String,
    pub(super) branch_id: String,
    /// Evidence of the bytes whose ingestion last produced the recorded
    /// render heads. None when there is no known acknowledged render state.
    pub(super) render_evidence: Option<FileEvidence>,
    pub(super) render_heads: Option<Vec<String>>,
}

pub(super) fn tracked_bindings(checkout: &Checkout) -> Res<Vec<Tracked>> {
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
pub(super) async fn verify_bindings(
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
pub(super) async fn binding_state(
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

pub(super) fn note_evidence(note: &daybook_types::doc::Note) -> FileEvidence {
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
pub(super) enum Disposition {
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

pub(super) fn disposition(
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

pub(super) fn display(disposition: &Disposition, path: &str) -> String {
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
pub(super) async fn scan_untracked(
    root: &Path,
    excluded: &HashSet<String>,
) -> Res<BTreeSet<RelPath>> {
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

pub(super) fn tracked_exclusions(checkout: &Checkout) -> Res<HashSet<String>> {
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

pub(super) async fn status(
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
