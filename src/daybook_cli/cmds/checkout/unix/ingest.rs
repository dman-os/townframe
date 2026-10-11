use super::*;

/// Records that the running verb blocked the checkout. A Pending checkout
/// stays Pending: its create-phase failure already blocks everything.
/// Best-effort marker write: the operation's own failure propagates
/// regardless; a failed write compounds both errors explicitly.
pub(super) async fn record_blocked(
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
pub(super) async fn ingest_checked(
    root: &Path,
    marker: &mut Marker,
    drawer: &daybook_core::drawer::DrawerRepo,
    allow: &[PathBuf],
) -> Res<()> {
    if let Err(error) = ingest(root, marker, drawer, allow).await {
        if let Err(marker_error) = record_blocked(marker, root, &error, Operation::Ingest).await {
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
pub(super) async fn resolve_claims(
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
            .get_doc_bundle_at_branch(&document, BranchPath::new(&import_branch), Some(Vec::new()))
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

pub(super) fn push_receipt(
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
pub(super) fn allowed_key(root: &Path, argument: &Path) -> Res<String> {
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

pub(super) struct EditTarget {
    pub(super) track: Tracked,
    pub(super) live_heads: Vec<String>,
    pub(super) branch_note: daybook_types::doc::Note,
}

/// What the batch must stage: a prepared edit of an existing tracked
/// binding, or a prepared import of an allowed untracked file.
pub(super) enum Target {
    Edit(Box<EditTarget>),
    Import(String),
}

/// Recognizes and prepares the whole checkout's batch before staging
/// anything (ADR 011 §6 steps 1–4, ADR 012 §6). Nothing is written to the
/// filesystem except the marker's own progress records; upstream `main` is
/// never read or merged: staging is local-first and publication is out of
/// this slice.
pub(super) async fn ingest(
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
                    .wrap_err_with(|| {
                        format!(
                            "ingest failed again after refreshing evidence; first failure: {first}"
                        )
                    })?;
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
pub(super) async fn stage_edit(
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
pub(super) async fn stage_import(
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
