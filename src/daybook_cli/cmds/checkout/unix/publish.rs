use super::*;

/// Per-destination publication lines (design §7): published records with
/// their heads, upstream divergence, and pending destinations.
pub(super) async fn publication_lines(
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
pub(super) fn validate_candidate_projection(
    candidate: &daybook_core::drawer::types::MergeCandidate,
    track: &Tracked,
) -> Res<()> {
    let mut dpaths = Vec::new();
    for (key, value) in &candidate.facets {
        let Some(path) = Dpath::parse_facet_key(key) else {
            continue;
        };
        let path = path.map_err(|error| eyre::eyre!("candidate dpath does not parse: {error}"))?;
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
pub(super) async fn publish_settlement(
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
pub(super) async fn publish_race_wait(document: &str) {
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
pub(super) async fn publish_destination(
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
                    .get_doc_bundle_at_branch(document, BranchPath::new("main"), Some(Vec::new()))
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
pub(super) async fn project_published(
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
pub(super) enum DestinationOutcome {
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

pub(super) struct Destination {
    pub(super) track: Tracked,
}

pub(super) async fn publish_pipeline(
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
                project_published(&receiver, marker, drawer, destination, published_heads).await?;
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
