use crate::interlude::*;

use daybook_types::doc::{
    BlobPin, ChangeHashSet, DocId, DocPatch, FacetKey, FacetRaw, WellKnownFacet, WellKnownFacetTag,
};
use tokio_util::sync::CancellationToken;

use crate::blobs::pair_roots::{PairRoots, UnresolvedPair};
use crate::drawer::{DrawerRepo, MaterializationWake};
use crate::index::facet_delta::FacetDelta;
use crate::index::facet_set::{FacetSetRevisionStore, FacetSetSelector};
use crate::repos::RepoStopToken;
use big_sync::DeltaWalkerStateRepo as _;
use big_sync::delta_walker_state::SqliteDeltaWalkerStateRepo;
use big_sync_core::concurrent_delta_walker::{
    ConcurrentDelta, ConcurrentDeltaRead, ConcurrentDeltaWalker,
};
use big_sync_core::revisioned_store::{RevisionRead, RevisionedStore as _};
use big_sync_core::serial_delta_walker::SerialDeltaWalker;
use big_sync_core::tokio_keyed_scheduler::{TokioKeyedScheduler, TokioTaskCompletion};
use daybook_types::doc::BranchId;
use sqlx::{Row, Sqlite};

pub(crate) const BLOB_PIN_STATE_LOCAL_STATE_ID: &str = "@daybook/core/blob-pin-worker";

/// Walker state for the enablement machine (plugs config event rev store).
pub(crate) const BLOB_PIN_PLUG_EVENTS_STATE_ID: &str = "@daybook/core/blob-pin-plug-events";

/// Everything `spawn_blob_pin_worker` reads from the booted repo. Named
/// fields (not a positional tuple) keep the two spawn sites self-describing.
pub(crate) struct BlobPinWorkerArgs {
    pub drawer_repo: Arc<DrawerRepo>,
    pub sql: SqlCtx,
    pub core_inventory_doc_id: DocumentId,
    pub docs_inventory_doc_id: DocumentId,
    pub encryption_inventory_doc_id: DocumentId,
    pub blobs_repo: Arc<crate::blobs::BlobsRepo>,
    pub facet_set_store: Arc<FacetSetRevisionStore>,
    pub plugs_repo: Arc<crate::plugs::PlugsRepo>,
    pub parent_cancel_token: CancellationToken,
}

/// Spawn the blob-pin worker and its two machines:
///
/// - the facet machine: blob facet deltas -> docs inventory (full-branch
///   state per delta) and, from `cipherBlob` facet deltas, the encrypted
///   representations they name -> the encryption inventory;
/// - the plug-events machine: typed `PlugsEvent`s from the plugs config
///   rev store -> core inventory pin maintenance, enablement-driven.
///
/// Both share one inventory lock so their inventory writes cannot
/// interleave. The machines' stop handle rides the returned
/// [`RepoStopToken`]. No public surface: observers read the inventory
/// docs through the drawer.
#[tracing::instrument(
    level = "debug",
    skip_all,
    err(Debug),
    fields(worker = "blob-pin-worker")
)]
pub(crate) async fn spawn_blob_pin_worker(args: BlobPinWorkerArgs) -> Res<RepoStopToken> {
    let BlobPinWorkerArgs {
        drawer_repo,
        sql,
        core_inventory_doc_id,
        docs_inventory_doc_id,
        encryption_inventory_doc_id,
        blobs_repo,
        facet_set_store,
        plugs_repo,
        parent_cancel_token,
    } = args;
    Ctx::ensure_schema(&sql).await?;
    // The pair-root ledger, booted before the machines so the drain below can
    // resolve pairs an earlier run left rooted (ADR 003 §19).
    let pair_roots = PairRoots::boot(sql.clone()).await?;

    let core_doc_id =
        ensure_configured_inventory_branch(&drawer_repo, &core_inventory_doc_id, "core").await?;
    let docs_doc_id =
        ensure_configured_inventory_branch(&drawer_repo, &docs_inventory_doc_id, "docs").await?;
    let encryption_doc_id = ensure_configured_inventory_branch(
        &drawer_repo,
        &encryption_inventory_doc_id,
        "encryption",
    )
    .await?;
    let ctx = Arc::new(Ctx {
        drawer_repo,
        sql,
        core_inventory_doc_id: core_doc_id,
        docs_inventory_doc_id: docs_doc_id,
        encryption_inventory_doc_id: encryption_doc_id,
        store: blobs_repo.iroh_store(),
        inventory_lock: Arc::new(tokio::sync::Mutex::new(())),
        pair_roots,
    });
    let event_store = Arc::new(crate::plugs::PlugsConfigEventStore::new(
        Arc::clone(&facet_set_store),
        Arc::clone(&ctx.drawer_repo),
        &plugs_repo,
    ));
    let cancel_token = parent_cancel_token.child_token();
    // Resolve pairs a crash left rooted but unclaimed BEFORE the machines run:
    // they are what derives pins, and the drain has to read their state as of boot.
    ctx.drain_pair_roots().await?;
    // One supervisor joins both machines; a panic in either takes the
    // task down per the task-panic-handler convention.
    let worker_handle = tokio::spawn({
        let facet_set_store = Arc::clone(&facet_set_store);
        let plugs_repo = Arc::clone(&plugs_repo);
        let cancel_token = cancel_token.clone();
        let mut facet_worker = Worker::new(Arc::clone(&ctx));
        let mut event_worker = Worker::new(ctx);
        async move {
            let facet = facet_worker.run_facet_machine(facet_set_store, cancel_token.clone());
            let events =
                event_worker.run_plug_events_machine(event_store, plugs_repo, cancel_token);
            let (facet, events) = tokio::join!(facet, events);
            facet.expect("blob-pin facet machine error");
            events.expect("blob-pin plug-events machine error");
        }
    });
    Ok(RepoStopToken {
        cancel_token,
        worker_handle: Some(worker_handle),
    })
}

/// The pin worker's boot ensurer for the config-driven inventory specification
/// (ADR 003 §13). The machines reconcile BlobPin facets into the configured
/// inventory documents through drawer branch writes, so a configured inventory
/// document id that no local drawer entry registers — the resolver's own
/// fallback spelling is exactly that silent miss — is a broken config: the
/// spawn fails here, at the boot edge, with the id named. This sits at the
/// boot edge by design rather than mid-run: the violation is the config, and
/// production provisioned its inventories into the drawer before their pin
/// worker ever boots.
async fn ensure_configured_inventory_branch(
    drawer_repo: &DrawerRepo,
    configured_branch_doc_id: &DocumentId,
    what: &str,
) -> Res<DocId> {
    let doc_id = drawer_repo
        .resolve_doc_id_for_branch_doc_id(configured_branch_doc_id.clone())
        .await?;
    let registered = drawer_repo.get_entry(&doc_id).await?.is_some_and(|entry| {
        entry
            .branches
            .values()
            .any(|branch| &branch.branch_doc_id == configured_branch_doc_id)
    });
    eyre::ensure!(
        registered,
        "configured {what} inventory document {configured_branch_doc_id} is not a local \
         drawer branch: the inventory specification is config driven (ADR 003 §13), so a \
         configured inventory the repo has not provisioned locally is a broken config",
    );
    Ok(doc_id)
}

/// Shared state of the blob-pin worker: the facet machine (docs inventory)
/// and the plug-events machine (core inventory) plus their inventory
/// upsert subtasks all hold this Arc. Private — the worker has no public
/// surface; observers read the inventory docs through the drawer.
struct Ctx {
    drawer_repo: Arc<DrawerRepo>,
    sql: SqlCtx,
    core_inventory_doc_id: DocId,
    docs_inventory_doc_id: DocId,
    /// The encrypted-representation inventory: ciphertext pins only. Absent on
    /// a repo created before it existed (ADR 003 §13), in which case ciphertext
    /// pins are not derived at all rather than mixed into a plaintext
    /// inventory.
    encryption_inventory_doc_id: DocId,
    /// The blob store, for the pair tags the release path deletes. Named tags
    /// are the only GC roots, so releasing a pair is a store write, not just an
    /// inventory edit.
    store: iroh_blobs::api::Store,
    /// The durable root record for pairs: rows written before their tags and
    /// retired once a durable facet or a ciphertext pin claims the pair. It is the
    /// only trace of a pair the encryption inventory never recorded, so
    /// [`Ctx::drain_pair_roots`] reads it before the machines start, and every pin
    /// write retires the rows it owns.
    pair_roots: PairRoots,
    /// One lock shared by both machines: plug-pin upserts and facet-driven
    /// inventory diffs must not interleave.
    inventory_lock: Arc<tokio::sync::Mutex<()>>,
}

/// What the boot drain decided for one pair-root row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainAction {
    /// Retire the row: nothing is rooted, or the pin machinery owns the pair.
    ClearRow,
    /// Leave the row: the facet machine is about to derive the pair's pin.
    Keep,
    /// Drop the pair's tags and its row: nothing will ever claim the pair.
    Release,
    /// Leave the row and warn: the facets cannot be checked, so no release.
    KeepUnprovenanced,
}

/// Private machine owner. The context is shared with task futures, while each
/// machine keeps its walker, scheduler, pending work, and wake state on its
/// own stack in the machine method.
struct Worker {
    ctx: Arc<Ctx>,
}

impl Worker {
    fn new(ctx: Arc<Ctx>) -> Self {
        Self { ctx }
    }
}

impl std::ops::Deref for Worker {
    type Target = Ctx;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

struct PreparedDocBranch {
    doc_id: DocId,
    branch_id: BranchId,
    pins: Option<DocPins>,
}

/// One branch's derived pin sets: plaintext digests from its `Blob` facets and
/// ciphertext digests from its `cipherBlob` facets. They are derived together
/// because both come from the branch's one hydrated facet set, and each has its
/// own inventory (`pins.plain` -> docs inventory, `pins.cipher` -> encryption
/// inventory).
#[derive(Debug, Clone, Default)]
struct DocPins {
    plain: HashMap<String, u64>,
    cipher: HashMap<String, u64>,
}
impl Ctx {
    async fn ensure_schema(sql: &SqlCtx) -> Res<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS blob_pin_doc_state (
                doc_id TEXT NOT NULL,
                branch_id TEXT NOT NULL,
                blob_hash TEXT NOT NULL,
                length_octets INTEGER NOT NULL,
                PRIMARY KEY (doc_id, branch_id, blob_hash)
            );
            CREATE INDEX IF NOT EXISTS idx_blob_pin_doc_state_hash ON blob_pin_doc_state(blob_hash);

            CREATE TABLE IF NOT EXISTS blob_pin_plug_state (
                plug_id TEXT NOT NULL,
                blob_hash TEXT NOT NULL,
                length_octets INTEGER NOT NULL,
                PRIMARY KEY (plug_id, blob_hash)
            );
            CREATE INDEX IF NOT EXISTS idx_blob_pin_plug_state_hash ON blob_pin_plug_state(blob_hash);

            CREATE TABLE IF NOT EXISTS blob_pin_cipher_doc_state (
                doc_id TEXT NOT NULL,
                branch_id TEXT NOT NULL,
                cipher_hash TEXT NOT NULL,
                length_octets INTEGER NOT NULL,
                PRIMARY KEY (doc_id, branch_id, cipher_hash)
            );
            CREATE INDEX IF NOT EXISTS idx_blob_pin_cipher_doc_state_hash ON blob_pin_cipher_doc_state(cipher_hash);
            "#,
        )
        .execute(&sql.write_pool)
        .await?;

        let has_branch_id_col: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM pragma_table_info('blob_pin_doc_state') WHERE name = 'branch_id'",
        )
        .fetch_optional(&sql.write_pool)
        .await?;
        if has_branch_id_col.is_none() {
            let mut tx = sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            sqlx::query("ALTER TABLE blob_pin_doc_state RENAME TO blob_pin_doc_state_legacy")
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                r#"CREATE TABLE blob_pin_doc_state (
                    doc_id TEXT NOT NULL
                  , branch_id TEXT NOT NULL
                  , blob_hash TEXT NOT NULL
                  , length_octets INTEGER NOT NULL
                  , PRIMARY KEY (doc_id, branch_id, blob_hash)
                )"#,
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"INSERT INTO blob_pin_doc_state(doc_id, branch_id, blob_hash, length_octets)
                   SELECT doc_id
                        , CASE WHEN branch_path = 'main' THEN doc_id ELSE branch_path END
                        , blob_hash
                        , length_octets
                     FROM blob_pin_doc_state_legacy"#,
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query("DROP TABLE blob_pin_doc_state_legacy")
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_blob_pin_doc_state_hash ON blob_pin_doc_state(blob_hash)")
            .execute(&sql.write_pool)
            .await?;
        Ok(())
    }

    /// Blob pin candidates from one Blob facet value: the facet's digest plus
    /// any `db+blob` component URLs, keyed by representation hash.
    fn blob_pins_from_facet_value(blob: &daybook_types::doc::Blob) -> Vec<(String, u64)> {
        let mut out = Vec::new();
        if let Some(urls) = &blob.urls {
            for url_str in urls {
                if let Ok(url) = url_str.parse::<url::Url>()
                    && (url.scheme() == crate::blobs::BLOB_SCHEME || url.scheme() == "daybook-blob")
                {
                    let hash = url.path().trim_start_matches('/');
                    if hash.parse::<crate::blobs::BlobId>().is_ok() {
                        out.push((hash.to_string(), blob.length_octets));
                    }
                }
            }
        }
        if blob.digest.parse::<crate::blobs::BlobId>().is_ok() {
            out.push((blob.digest.clone(), blob.length_octets));
        }
        out
    }

    /// Derive one branch's pin sets at `heads`: plaintext digests from its
    /// `Blob` facets, ciphertext digests from its `cipherBlob` facets.
    async fn hydrate_pins(
        drawer: &DrawerRepo,
        physical_branch_id: &BranchId,
        document_id: &DocId,
        heads: ChangeHashSet,
    ) -> Res<Option<DocPins>> {
        let physical_id = physical_branch_id.0.parse::<big_repo::DocumentId>()?;
        let Some(facets) = drawer
            .hydrate_physical_doc_at_heads(physical_id, heads)
            .await?
        else {
            return Ok(None);
        };
        let branch = match WellKnownFacet::from_json(
            facets
                .get(&FacetKey::from(WellKnownFacetTag::Branch))
                .cloned()
                .ok_or_else(|| ferr!("missing mandatory Branch facet"))?,
            WellKnownFacetTag::Branch,
        )? {
            WellKnownFacet::Branch(branch) => branch,
            _ => unreachable!("Branch facet decoded to another well-known variant"),
        };
        if branch.branch_id != *physical_branch_id || branch.document_id != *document_id {
            return Err(ferr!("blob facet branch identity mismatch"));
        }
        // Plug manifests are static artifacts whose blob pins follow
        // ENABLEMENT (the core inventory, driven by the plugs config event
        // stream), not doc presence. Exclude them from the docs-inventory
        // path so a replicated manifest does not pin its blobs on every peer.
        if facets.contains_key(&FacetKey::from(WellKnownFacetTag::PlugManifest)) {
            return Ok(Some(DocPins::default()));
        }
        let dmeta = match WellKnownFacet::from_json(
            facets
                .get(&FacetKey::from(WellKnownFacetTag::Dmeta))
                .cloned()
                .ok_or_else(|| ferr!("missing mandatory Dmeta facet"))?,
            WellKnownFacetTag::Dmeta,
        )? {
            WellKnownFacet::Dmeta(dmeta) => dmeta,
            _ => unreachable!("Dmeta facet decoded to another well-known variant"),
        };
        if dmeta.id != *document_id {
            return Err(ferr!("dmeta document id does not match Branch facet"));
        }
        let mut current_pins = DocPins::default();
        for (facet_key, meta) in dmeta.facets {
            if !meta.deleted_at.is_empty() {
                continue;
            }
            let tag = &facet_key.tag;
            if *tag == WellKnownFacetTag::Blob.into() {
                let Some(facet_raw) = facets.get(&facet_key) else {
                    return Err(ferr!("active Blob facet is missing its value"));
                };
                let WellKnownFacet::Blob(blob) =
                    WellKnownFacet::from_json(facet_raw.clone(), WellKnownFacetTag::Blob)?
                else {
                    unreachable!("Blob facet decoded to another well-known variant");
                };
                for (hash, length) in Self::blob_pins_from_facet_value(&blob) {
                    current_pins.plain.insert(hash, length);
                }
            } else if *tag == WellKnownFacetTag::CipherBlob.into() {
                // ADR 003 §13: the encrypted representation's digest is what a
                // relay is asked to hold, so it is a pin - routed to the
                // encryption inventory, never to a plaintext one.
                let Some(facet_raw) = facets.get(&facet_key) else {
                    return Err(ferr!("active CipherBlob facet is missing its value"));
                };
                let WellKnownFacet::CipherBlob(cipher) =
                    WellKnownFacet::from_json(facet_raw.clone(), WellKnownFacetTag::CipherBlob)?
                else {
                    unreachable!("CipherBlob facet decoded to another well-known variant");
                };
                let digest = &cipher.representation.digest;
                // A pin is only useful if it names a blob id this repo's
                // stores can key on. Accept either digest spelling: ADR 003 §3
                // makes the multihash form canonical and it is the *only*
                // carrier a cipherBlob facet has, whereas a `Blob` facet also
                // carries `db+blob:///` URLs in the bare form.
                if crate::blobs::digest_str_to_blob_id_lenient(digest).is_some() {
                    current_pins
                        .cipher
                        .insert(digest.clone(), cipher.representation.length_octets);
                }
            }
        }
        Ok(Some(current_pins))
    }

    /// Hydrate the manifest doc's Blob facets at an enabled ref's heads.
    ///
    /// Returns `None` when the manifest is not locally readable at the ref
    /// (ADR 007 §6: pending). Unpinned refs resolve the branch's current
    /// heads. Pure read — the pin set is computed, never written back.
    async fn manifest_blob_pins(&self, ref_url: &url::Url) -> Res<Option<HashMap<String, u64>>> {
        let parsed = crate::plugs::PlugsRepo::parse_enabled_ref(ref_url)?;
        let branch_path =
            daybook_types::doc::BranchPath::new(parsed.branch.as_deref().unwrap_or("main"));
        let heads = if let Some(at) = &parsed.at {
            ChangeHashSet(am_utils_rs::parse_commit_heads(at)?)
        } else {
            let Some(heads) = self
                .drawer_repo
                .get_branch_heads_for_path(&parsed.doc_id, branch_path)
                .await?
            else {
                return Ok(None);
            };
            heads
        };
        // All facets: the manifest doc is small, and the Blob facet keys carry
        // per-blob ids, so a tag filter would need the full key list anyway.
        let Some(doc) = self
            .drawer_repo
            .get_doc_with_facets_at_branch_heads(&parsed.doc_id, branch_path, &heads, None)
            .await?
        else {
            return Ok(None);
        };
        let mut pins = HashMap::new();
        for (facet_key, raw) in &doc.facets {
            if facet_key.tag != WellKnownFacetTag::Blob.into() {
                continue;
            }
            let WellKnownFacet::Blob(blob) =
                WellKnownFacet::from_json(raw.clone(), WellKnownFacetTag::Blob)?
            else {
                unreachable!("Blob facet decoded to another well-known variant");
            };
            for (hash, length) in Self::blob_pins_from_facet_value(&blob) {
                pins.insert(hash, length);
            }
        }
        Ok(Some(pins))
    }

    async fn desired_pins(&self) -> Res<HashMap<String, BlobPin>> {
        let rows = sqlx::query(
            "SELECT blob_hash, MAX(length_octets) AS length_octets
               FROM blob_pin_doc_state
              GROUP BY blob_hash",
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        let mut pins = HashMap::new();
        for row in rows {
            let hash: String = row.try_get("blob_hash")?;
            let length_octets = u64::try_from(row.try_get::<i64, _>("length_octets")?)?;
            pins.insert(hash, BlobPin { length_octets });
        }
        Ok(pins)
    }

    /// The encrypted representations the facets currently name, globally:
    /// the encryption inventory's desired set.
    async fn desired_cipher_pins(&self) -> Res<HashMap<String, BlobPin>> {
        let rows = sqlx::query(
            "SELECT cipher_hash, MAX(length_octets) AS length_octets
               FROM blob_pin_cipher_doc_state
              GROUP BY cipher_hash",
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        let mut pins = HashMap::new();
        for row in rows {
            let hash: String = row.try_get("cipher_hash")?;
            let length_octets = u64::try_from(row.try_get::<i64, _>("length_octets")?)?;
            pins.insert(hash, BlobPin { length_octets });
        }
        Ok(pins)
    }

    /// Reconcile one inventory doc against a desired set, returning the hashes
    /// that were removed from it.
    async fn apply_inventory_diff(
        &self,
        inventory_doc_id: &DocId,
        desired: &HashMap<String, BlobPin>,
    ) -> Res<Vec<String>> {
        let current = self.list_pins_from_doc_id(inventory_doc_id).await?;
        let mut facets_set = HashMap::new();
        for (hash, pin) in desired {
            if current
                .get(hash)
                .is_none_or(|existing| existing.length_octets != pin.length_octets)
            {
                facets_set.insert(
                    FacetKey {
                        tag: WellKnownFacetTag::BlobPin.into(),
                        id: hash.clone(),
                    },
                    FacetRaw::from(WellKnownFacet::BlobPin(pin.clone())),
                );
            }
        }
        let removed = current
            .keys()
            .filter(|hash| !desired.contains_key(*hash))
            .cloned()
            .collect::<Vec<_>>();
        let mut facets_remove = removed
            .iter()
            .map(|hash| FacetKey {
                tag: WellKnownFacetTag::BlobPin.into(),
                id: hash.clone(),
            })
            .collect::<Vec<_>>();
        facets_remove.sort_by(|left, right| left.id.cmp(&right.id));
        if facets_set.is_empty() && facets_remove.is_empty() {
            return Ok(Vec::new());
        }
        // Ciphertext-inventory removals release their pairs BEFORE the removal
        // write. This inventory is the only record a pair exists (see
        // [`release_pairs`]), so a removed ciphertext pair will never be
        // claimed again, and the order decides which crash window is harmless:
        // released-but-still-declared (tags gone, pin row still written) just
        // serves a not-found, while written-but-unreleased leaves a GC root
        // that nothing will ever claim again (ADR 003 §19). Only the
        // ciphertext inventory drives releases - a plaintext pin leaving the
        // docs inventory means nothing for pairs (`pt:<C>` is keyed by
        // ciphertext).
        if *inventory_doc_id == self.encryption_inventory_doc_id {
            self.release_pairs(&removed).await?;
        }
        self.drawer_repo
            .update_at_heads(
                DocPatch {
                    id: inventory_doc_id.clone(),
                    user_path: None,
                    facets_set,
                    facets_remove,
                },
                daybook_types::doc::BranchPath::new("main"),
                None,
            )
            .await?;
        Ok(removed)
    }

    /// Release the pairs whose ciphertext pins just left the encrypted-
    /// representation inventory.
    ///
    /// That inventory is the only record a pair exists, so a hash leaving it
    /// means the facets that named it are gone: a retired representation, a
    /// removed domain, or a document that left the encryption-eligibility
    /// group. The `ct:`/`pt:` tags are the store-level GC roots for the
    /// ciphertext's outboard and for the plaintext that serves it, so this is
    /// where both become collectable again (ADR 003 §13/§19). Driven by the same
    /// diff that removed the pin - reconciled, never incidental, and never a
    /// separate sweep - and driven from `apply_inventory_diff` BEFORE its
    /// removal write, so a crash between the two writes releases rather than
    /// orphans.
    async fn release_pairs(&self, removed: &[String]) -> Res<()> {
        for hash in removed {
            // The hash is a digest string a facet supplied, so it may use
            // either spelling (the same rule hydration applied). A panic here
            // would take the process down over facet-authored data, so a bad
            // value is a loud error instead.
            let Some(blob_id) = crate::blobs::digest_str_to_blob_id_lenient(hash) else {
                eyre::bail!("cannot release a pair for {hash}: not a blob digest");
            };
            crate::blobs::encrypt::drop_pair_tags(
                &self.store,
                crate::blobs::blob_id_to_iroh_hash(blob_id),
            )
            .await?;
        }
        Ok(())
    }

    /// What the boot drain decides for one ledger row.
    ///
    /// A pure function because the decision, not the plumbing, is what has to be
    /// right: releasing a pair a document still names unroots a live
    /// representation, while leaving a pair nothing names strands its ciphertext
    /// and its plaintext forever.
    fn drain_action(rooted: bool, pin_recorded: bool, facet_names: Option<bool>) -> DrainAction {
        if !rooted {
            // Nothing is rooted, so there is nothing to release and nothing to
            // keep alive: only the row is stale.
            return DrainAction::ClearRow;
        }
        if pin_recorded {
            // The pin machinery owns the pair now, and it is what releases a pair
            // whose facets go away. The row has done its job.
            return DrainAction::ClearRow;
        }
        match facet_names {
            // A durable facet still names the pair, so the facet machine will
            // derive its pin: the pair is live and has to stay rooted.
            Some(true) => DrainAction::Keep,
            // Nothing names it and nothing pinned it, so nothing ever will: this
            // is exactly the window the ledger exists for.
            Some(false) => DrainAction::Release,
            // Without provenance the facets cannot be checked, and releasing would
            // be a guess. Leave the row: a row is visible (and logged), whereas a
            // dropped tag is a silent serve failure.
            None => DrainAction::KeepUnprovenanced,
        }
    }

    /// Resolve pairs a crash left rooted but unclaimed.
    ///
    /// The ciphertext inventory is the only record a pair exists, and it is built
    /// from facets, so a pair whose facet write never landed is invisible to the
    /// release path and its tags become permanent (ADR 003 §19). Every row here was
    /// written before those tags, which makes this the one place that can see such
    /// a pair; it runs at boot, before the machines start writing pins.
    ///
    /// The cost is one query on a clean boot: rows only exist between a pair's
    /// tags and the pin that claims it.
    async fn drain_pair_roots(&self) -> Res<()> {
        let unresolved: Vec<UnresolvedPair> = self.pair_roots.unresolved().await?;
        if unresolved.is_empty() {
            return Ok(());
        }
        // Spelling is not stable between the two planes: pin rows are keyed by
        // whatever digest the facet supplied, the ledger by canonical hex. Compare
        // by blob id, leaning on the pin worker's own check that only parseable
        // digests reach the table.
        let pinned: HashSet<crate::blobs::BlobId> = self
            .desired_cipher_pins()
            .await?
            .keys()
            .filter_map(|digest| crate::blobs::digest_str_to_blob_id_lenient(digest))
            .collect();
        for pair in unresolved {
            let rooted =
                crate::blobs::encrypt::has_pair_tags(&self.store, pair.cipher_hash).await?;
            let blob_id = crate::blobs::BlobId::new(*pair.cipher_hash.as_bytes());
            let pin_recorded = pinned.contains(&blob_id);
            let facet_names = match &pair.claimed_by {
                Some((doc_id, branch_path)) => Some(
                    Self::branch_facets_name(
                        &self.drawer_repo,
                        doc_id,
                        daybook_types::doc::BranchPath::new(branch_path),
                        &blob_id,
                    )
                    .await?,
                ),
                None => None,
            };
            match Self::drain_action(rooted, pin_recorded, facet_names) {
                DrainAction::ClearRow => {
                    self.pair_roots.clear(pair.cipher_hash).await?;
                }
                DrainAction::Keep => {}
                DrainAction::Release => {
                    tracing::warn!(
                        cipher = %pair.cipher_hash,
                        "releasing a pair an earlier run rooted but no facet or pin ever \
                         claimed: its tags were the only roots (ADR 003 §19)"
                    );
                    crate::blobs::encrypt::drop_pair_tags(&self.store, pair.cipher_hash).await?;
                    self.pair_roots.clear(pair.cipher_hash).await?;
                }
                DrainAction::KeepUnprovenanced => {
                    tracing::warn!(
                        cipher = %pair.cipher_hash,
                        "a rooted pair has no recorded provenance, so its facets cannot be \
                         checked: leaving its tags rooted rather than risking a live pair"
                    );
                }
            }
        }
        Ok(())
    }

    /// Whether any facet of `doc_id`'s `branch_path` names `blob_id` as its
    /// ciphertext representation.
    ///
    /// A `cipherBlob` facet's key id is `{domain}/{facet}`, so the representation
    /// digest lives in the facet *value* and no tag+id index can answer this: the
    /// branch's facets are read and their digests compared. `None` heads means the
    /// branch's current heads, which is what the facet machine derives pins from.
    async fn branch_facets_name(
        drawer: &DrawerRepo,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        blob_id: &crate::blobs::BlobId,
    ) -> Res<bool> {
        let Some(doc) = drawer
            .get_doc_with_facets_at_branch(doc_id, branch_path, None)
            .await?
        else {
            // No such branch (or no such document): it names nothing, which is a
            // real answer rather than an unknown.
            return Ok(false);
        };
        for (key, raw) in &doc.facets {
            if key.tag != WellKnownFacetTag::CipherBlob.into() {
                continue;
            }
            let WellKnownFacet::CipherBlob(cipher) =
                WellKnownFacet::from_json(raw.clone(), WellKnownFacetTag::CipherBlob)?
            else {
                unreachable!("cipherBlob facet decoded to another well-known variant");
            };
            if crate::blobs::digest_str_to_blob_id_lenient(&cipher.representation.digest).as_ref()
                == Some(blob_id)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Apply one branch's derived pin sets: replace its rows, then reconcile
    /// both inventories from the global desired sets.
    ///
    /// A plaintext pin leaving the docs inventory is not a release: the
    /// `pt:<C>` tag is keyed by ciphertext, and a `P` that stops being pinned by
    /// one document may still serve a live `C` there. Only the ciphertext
    /// inventory drives releases.
    async fn apply_branch_pins(&self, branch: PreparedDocBranch) -> Res<()> {
        let _guard = self.inventory_lock.lock().await;
        self.replace_doc_branch_state(std::slice::from_ref(&branch))
            .await?;
        let docs = self.desired_pins().await?;
        self.apply_inventory_diff(&self.docs_inventory_doc_id, &docs)
            .await?;
        {
            let cipher = self.desired_cipher_pins().await?;
            // The release for this diff happens inside `apply_inventory_diff`,
            // before the removal write.
            self.apply_inventory_diff(&self.encryption_inventory_doc_id, &cipher)
                .await?;
        }
        Ok(())
    }

    async fn replace_doc_branch_state(&self, branches: &[PreparedDocBranch]) -> Res<()> {
        if branches.is_empty() {
            return Ok(());
        }
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let mut newly_pinned = Vec::new();
        for branch in branches {
            sqlx::query("DELETE FROM blob_pin_doc_state WHERE doc_id = ? AND branch_id = ?")
                .bind(&branch.doc_id)
                .bind(&branch.branch_id.0)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM blob_pin_cipher_doc_state WHERE doc_id = ? AND branch_id = ?")
                .bind(&branch.doc_id)
                .bind(&branch.branch_id.0)
                .execute(&mut *tx)
                .await?;
            let Some(pins) = &branch.pins else {
                continue;
            };
            for (table, hash_column, pins) in [
                ("blob_pin_doc_state", "blob_hash", &pins.plain),
                ("blob_pin_cipher_doc_state", "cipher_hash", &pins.cipher),
            ] {
                if pins.is_empty() {
                    continue;
                }
                let mut query = sqlx::QueryBuilder::<Sqlite>::new(format!(
                    "INSERT INTO {table}(doc_id, branch_id, {hash_column}, length_octets) "
                ));
                let rows = pins
                    .iter()
                    .map(|(hash, length)| Ok((hash.as_str(), i64::try_from(*length)?)))
                    .collect::<Res<Vec<_>>>()?;
                query.push_values(rows.iter(), |mut row, (hash, length)| {
                    row.push_bind(&branch.doc_id)
                        .push_bind(&branch.branch_id.0)
                        .push_bind(hash)
                        .push_bind(*length);
                });
                query.build().execute(&mut *tx).await?;
                if hash_column == "cipher_hash" {
                    // Freshly written ciphertext pins retire their ledger rows: the
                    // pin state is now the durable record that keeps this pair (and
                    // its diff-driven release) visible.
                    for (hash, _) in &rows {
                        let hash: &str = hash;
                        let Some(blob_id) = crate::blobs::digest_str_to_blob_id_lenient(hash)
                        else {
                            eyre::bail!("pinned ciphertext {hash} is not a blob digest");
                        };
                        newly_pinned.push(crate::blobs::blob_id_to_iroh_hash(blob_id));
                    }
                }
            }
        }
        tx.commit().await?;
        // Retiring rows is a plain delete on the ledger, outside the pin write's
        // transaction. It runs after the commit, so a crash in between leaves the
        // row to the boot drain, which resolves the same pair from the same facts.
        for c_hash in newly_pinned {
            self.pair_roots.clear(c_hash).await?;
        }
        Ok(())
    }

    /// Upsert one enabled plug's blob pins into the core inventory.
    ///
    /// Kept core-inventory machinery (blobPin upsert + orphan eviction keyed
    /// by plug): only the pin-source changes with the plugs event rework —
    /// the caller computes pins from the manifest doc's Blob facets at the
    /// enabled heads instead of parsing manifest internals.
    async fn apply_plug_pins(&self, plug_id: &str, current_pins: HashMap<String, u64>) -> Res<()> {
        let prev_hashes: HashSet<String> =
            sqlx::query_scalar("SELECT blob_hash FROM blob_pin_plug_state WHERE plug_id = ?1")
                .bind(plug_id)
                .fetch_all(&self.sql.write_pool)
                .await?
                .into_iter()
                .collect();

        let mut pins_to_set = Vec::new();
        for (hash, length_octets) in &current_pins {
            pins_to_set.push((
                FacetKey {
                    tag: WellKnownFacetTag::BlobPin.into(),
                    id: hash.clone(),
                },
                FacetRaw::from(WellKnownFacet::BlobPin(BlobPin {
                    length_octets: *length_octets,
                })),
            ));
        }

        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM blob_pin_plug_state WHERE plug_id = ?1")
            .bind(plug_id)
            .execute(&mut *tx)
            .await?;

        for (hash, length_octets) in &current_pins {
            sqlx::query(
                r#"
                INSERT INTO blob_pin_plug_state (plug_id, blob_hash, length_octets)
                VALUES (?1, ?2, ?3)
                "#,
            )
            .bind(plug_id)
            .bind(hash)
            .bind(*length_octets as i64)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;

        let mut pins_to_remove = Vec::new();
        for prev_hash in &prev_hashes {
            if !current_pins.contains_key(prev_hash) {
                let remaining_count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM blob_pin_plug_state WHERE blob_hash = ?1",
                )
                .bind(prev_hash)
                .fetch_one(&self.sql.write_pool)
                .await?;

                if remaining_count == 0 {
                    pins_to_remove.push(FacetKey {
                        tag: WellKnownFacetTag::BlobPin.into(),
                        id: prev_hash.clone(),
                    });
                }
            }
        }

        if !pins_to_set.is_empty() || !pins_to_remove.is_empty() {
            let mut facets_set = HashMap::new();
            for (key, value) in pins_to_set {
                facets_set.insert(key, FacetRaw::from(serde_json::to_value(value)?));
            }
            let patch = DocPatch {
                id: self.core_inventory_doc_id.clone(),
                user_path: None,
                facets_set,
                facets_remove: pins_to_remove,
            };
            self.drawer_repo
                .update_at_heads(patch, daybook_types::doc::BranchPath::new("main"), None)
                .await?;
        }

        Ok(())
    }

    /// Drop one plug's blob pins from the core inventory, evicting pins no
    /// other plug still references. Driven by `PlugsEvent::PlugDisabled`.
    async fn drop_plug_pins(&self, plug_id: &str) -> Res<()> {
        let prev_hashes: Vec<String> =
            sqlx::query_scalar("SELECT blob_hash FROM blob_pin_plug_state WHERE plug_id = ?1")
                .bind(plug_id)
                .fetch_all(&self.sql.write_pool)
                .await?;

        if prev_hashes.is_empty() {
            return Ok(());
        }

        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM blob_pin_plug_state WHERE plug_id = ?1")
            .bind(plug_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        let mut pins_to_remove = Vec::new();
        for hash in &prev_hashes {
            let remaining_count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM blob_pin_plug_state WHERE blob_hash = ?1")
                    .bind(hash)
                    .fetch_one(&self.sql.write_pool)
                    .await?;

            if remaining_count == 0 {
                pins_to_remove.push(FacetKey {
                    tag: WellKnownFacetTag::BlobPin.into(),
                    id: hash.clone(),
                });
            }
        }

        if !pins_to_remove.is_empty() {
            let patch = DocPatch {
                id: self.core_inventory_doc_id.clone(),
                user_path: None,
                facets_set: HashMap::new(),
                facets_remove: pins_to_remove,
            };
            self.drawer_repo
                .update_at_heads(patch, daybook_types::doc::BranchPath::new("main"), None)
                .await?;
        }

        Ok(())
    }

    async fn list_pins_from_doc_id(&self, doc_id: &DocId) -> Res<HashMap<String, BlobPin>> {
        let Some(doc) = self
            .drawer_repo
            .get_doc_with_facets_at_branch(
                doc_id,
                &daybook_types::doc::BranchPathBuf::from("main"),
                None,
            )
            .await?
        else {
            return Ok(HashMap::new());
        };
        let mut pins = HashMap::new();
        for (key, raw) in &doc.facets {
            if key.tag == WellKnownFacetTag::BlobPin.into()
                && let Ok(WellKnownFacet::BlobPin(pin)) =
                    WellKnownFacet::from_json(raw.clone(), WellKnownFacetTag::BlobPin)
            {
                pins.insert(key.id.clone(), pin);
            }
        }
        Ok(pins)
    }
}

/// Keyed execution budget for the blob-pin machine; mirrors the frontier
/// worker's concurrent budget.
const BLOB_PIN_TASK_BUDGET: usize = 64;

/// Scheduling key: one physical branch (hash collisions only over-serialize
/// a key, never break correctness).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct BlobPinKey(u64);

fn blob_pin_facet_key(branch_id: &BranchId) -> BlobPinKey {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    branch_id.0.hash(&mut hasher);
    BlobPinKey(hasher.finish())
}

/// The keyed command for one branch: the newest delta with the source cursor
/// it must cover.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BlobPinTask {
    key: BlobPinKey,
    cursor: u64,
    /// One branch's blob facet delta from the facet-set source. Hydration is
    /// full-branch state at the delta's heads, so a newer cursor supersedes
    /// any older or sibling delta for the branch.
    delta: FacetDelta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlobPinTaskOutput {
    Applied,
}

async fn run_blob_pin_task(task: BlobPinTask, ctx: Arc<Ctx>) -> Res<BlobPinTaskOutput> {
    let delta = task.delta;
    let tag = delta.key.facet_key.tag;
    // Both facet kinds feed one per-branch recompute: `hydrate_pins` derives the
    // branch's whole pin state (plaintext and ciphertext) at the delta's heads,
    // so any other facet tag is not a pin source.
    if tag != WellKnownFacetTag::Blob.into() && tag != WellKnownFacetTag::CipherBlob.into() {
        return Ok(BlobPinTaskOutput::Applied);
    }
    let pins = match &delta.current_branch_heads {
        Some(heads) => {
            match Ctx::hydrate_pins(
                &ctx.drawer_repo,
                &delta.key.branch_id,
                &delta.key.document_id,
                heads.clone(),
            )
            .await?
            {
                Some(pins) => pins,
                None => eyre::bail!(
                    "blob-pin source heads are not materialized for branch {}",
                    delta.key.branch_id.0
                ),
            }
        }
        None => {
            // Tombstone: the branch was removed; its pin rows go with it, which
            // also releases any ciphertext pair the branch was holding.
            ctx.apply_branch_pins(PreparedDocBranch {
                doc_id: delta.key.document_id,
                branch_id: delta.key.branch_id,
                pins: None,
            })
            .await?;
            return Ok(BlobPinTaskOutput::Applied);
        }
    };
    // All hydration completes before the inventory section opens; the state
    // write and the inventory diffs are then serialized so their global
    // recomputes cannot interleave.
    ctx.apply_branch_pins(PreparedDocBranch {
        doc_id: delta.key.document_id,
        branch_id: delta.key.branch_id,
        pins: Some(pins),
    })
    .await?;
    Ok(BlobPinTaskOutput::Applied)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PlugPinKey(u64);

fn plug_pin_key(plug_id: &str) -> PlugPinKey {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    plug_id.hash(&mut hasher);
    PlugPinKey(hasher.finish())
}

#[derive(Debug, Clone)]
struct PlugPinTask {
    key: PlugPinKey,
    event: crate::plugs::PlugsEvent,
    ref_url: Option<url::Url>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlugPinTaskOutput {
    Applied,
    Deferred,
}

impl Worker {
    /// The blob-pin facet machine: a `ConcurrentDeltaWalker` over the facet-set
    /// source (the `Blob` and `CipherBlob` tags), keyed by branch, with
    /// per-branch inventory tasks.
    /// Mutable machine state lives as stack locals here.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        err(Debug),
        fields(worker = "blob-pin-facet-machine", doc_id = %self.docs_inventory_doc_id),
    )]
    async fn run_facet_machine(
        &mut self,
        facet_set_store: Arc<FacetSetRevisionStore>,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        let facet_state = SqliteDeltaWalkerStateRepo::new(
            self.sql.read_pool.clone(),
            self.sql.write_pool.clone(),
            BLOB_PIN_STATE_LOCAL_STATE_ID,
            "facets",
        )
        .await
        .map_err(|error| ferr!("initializing blob-pin FacetSet walker state: {error}"))?;
        let durable = facet_state.progress().await?.upstream_revision;
        let reader = facet_set_store
            .open(
                FacetSetSelector::Tags(vec![
                    WellKnownFacetTag::Blob,
                    WellKnownFacetTag::CipherBlob,
                ]),
                durable,
            )
            .await
            .map_err(|error| ferr!("opening blob-pin FacetSet reader: {error}"))?;
        let mut facet_walker =
            ConcurrentDeltaWalker::open(reader, facet_state, |entry: &FacetDelta| {
                blob_pin_facet_key(&entry.key.branch_id)
            })
            .await
            .map_err(|error| ferr!("opening blob-pin FacetSet walker: {error}"))?;
        let mut tasks = TokioKeyedScheduler::new(BLOB_PIN_TASK_BUDGET);
        // The newest unacked delta per key.
        let mut pending: HashMap<BlobPinKey, BlobPinTask> = HashMap::new();
        loop {
            let available = BLOB_PIN_TASK_BUDGET.saturating_sub(tasks.active_count());
            let next_deadline = tasks.next_deadline();
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => return Ok(()),
                completion = tasks.next_completion() => {
                    self.on_task_completion(
                        &mut facet_walker,
                        &mut pending,
                        completion?,
                    )
                    .await?;
                }
                facet = async {
                    if available == 0 {
                        std::future::pending().await
                    } else {
                        facet_walker
                            .next(std::num::NonZeroUsize::new(available).expect("available is non-zero"))
                            .await
                    }
                } => match facet? {
                    ConcurrentDeltaRead::ReplayComplete { .. } => {}
                    ConcurrentDeltaRead::Entries { entries, .. } => {
                        for delta in entries {
                            self.on_delta(&mut tasks, &mut pending, delta)?;
                        }
                    }
                },
                _ = async {
                    if let Some(deadline) = next_deadline {
                        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    tasks.tick(std::time::Instant::now())?
                }
            }
        }
    }

    async fn on_task_completion(
        &mut self,
        facet_walker: &mut ConcurrentDeltaWalker<
            '_,
            FacetSetRevisionStore,
            SqliteDeltaWalkerStateRepo,
            BlobPinKey,
        >,
        pending: &mut HashMap<BlobPinKey, BlobPinTask>,
        completion: TokioTaskCompletion<BlobPinTask, BlobPinTaskOutput>,
    ) -> Res<()> {
        let task = completion.command;
        match completion.result {
            Ok(BlobPinTaskOutput::Applied) => {
                // The command's effect is durable; only now may the walker
                // cursor advance past it.
                facet_walker.ack(task.key, task.cursor).await?;
                if pending
                    .get(&task.key)
                    .is_some_and(|existing| existing.cursor == task.cursor)
                {
                    pending.remove(&task.key);
                }
            }
            Err(error) => panic!("blob-pin task failed: {error:?}"),
        }
        Ok(())
    }

    fn on_delta(
        &mut self,
        tasks: &mut TokioKeyedScheduler<BlobPinKey, BlobPinTask, BlobPinTaskOutput>,
        pending: &mut HashMap<BlobPinKey, BlobPinTask>,
        delta: ConcurrentDelta<BlobPinKey, FacetDelta>,
    ) -> Res<()> {
        let task = BlobPinTask {
            key: delta.key,
            cursor: delta.cursor,
            delta: delta.entry,
        };
        match pending.get(&task.key) {
            // Newest-wins is correct here: `hydrate_blob_pins` recomputes the
            // branch's full blob-pin state at the delta's heads, so a newer
            // cursor (or an equal-cursor sibling of the same branch) is fully
            // covered by the newest delta.
            Some(existing) if existing.cursor >= task.cursor => return Ok(()),
            _ => {}
        }
        pending.insert(task.key, task.clone());
        self.start_task(tasks, task)
    }

    fn start_task(
        &mut self,
        tasks: &mut TokioKeyedScheduler<BlobPinKey, BlobPinTask, BlobPinTaskOutput>,
        task: BlobPinTask,
    ) -> Res<()> {
        let future = run_blob_pin_task(task.clone(), Arc::clone(&self.ctx));
        tasks.replace(task.key, task.clone(), future)?;
        Ok(())
    }
}

async fn run_plug_pin_task(
    task: PlugPinTask,
    ctx: Arc<Ctx>,
    plugs_repo: Arc<crate::plugs::PlugsRepo>,
) -> Res<PlugPinTaskOutput> {
    tracing::debug!(event = ?task.event, ref_url = ?task.ref_url, "blob-pin plug task starting");
    match &task.event {
        crate::plugs::PlugsEvent::PlugEnabled { plug_id, .. }
        | crate::plugs::PlugsEvent::PlugUpdated { plug_id, .. } => {
            let live_ref;
            let ref_url = match task.ref_url.as_ref() {
                Some(ref_url) => ref_url,
                None => {
                    let Some(ref_url) = plugs_repo.enabled_ref(plug_id).await? else {
                        return Ok(PlugPinTaskOutput::Applied);
                    };
                    live_ref = ref_url;
                    &live_ref
                }
            };
            tracing::debug!(%plug_id, %ref_url, "blob-pin plug task resolved manifest ref");
            let Some(pins) = ctx.manifest_blob_pins(ref_url).await? else {
                tracing::debug!(%plug_id, %ref_url, "blob-pin plug task deferred: manifest is not readable");
                return Ok(PlugPinTaskOutput::Deferred);
            };
            let _guard = ctx.inventory_lock.lock().await;
            ctx.apply_plug_pins(plug_id, pins).await?;
        }
        crate::plugs::PlugsEvent::PlugDisabled { plug_id } => {
            let _guard = ctx.inventory_lock.lock().await;
            ctx.drop_plug_pins(plug_id).await?;
        }
        crate::plugs::PlugsEvent::PlugsConfigChanged { .. } => {}
    }
    Ok(PlugPinTaskOutput::Applied)
}

impl Worker {
    /// The enablement machine: a serial walker over the plugs config event rev
    /// store maintaining the core inventory's plug pins.
    ///
    /// Serial, not keyed: config revisions are rare, each event's effect is one
    /// inventory transaction, and config events must apply in order (an enable
    /// and its disable cannot reorder). Replay folds from the facet-set cursor —
    /// the rev store's diff is a pure function of the config history, so the
    /// final state converges even though intermediate replays use the revision's
    /// own config snapshot.
    ///
    /// The durable stream alone cannot resolve pending→active transitions (ADR
    /// 007 §6: a manifest arriving produces no config revision), so the machine
    /// also consumes the plugs event broadcast, which carries those transitions.
    async fn process_plug_pin_task(
        &self,
        drawer: &DrawerRepo,
        plugs_repo: Arc<crate::plugs::PlugsRepo>,
        tasks: &mut TokioKeyedScheduler<PlugPinKey, PlugPinTask, PlugPinTaskOutput>,
        subscriptions: &mut HashMap<PlugPinKey, MaterializationWake>,
        task: PlugPinTask,
    ) -> Res<()> {
        let key = task.key;
        tracing::debug!(?key, event = ?task.event, "blob-pin plug machine scheduling task");
        tasks.replace(
            key,
            task.clone(),
            run_plug_pin_task(task.clone(), Arc::clone(&self.ctx), Arc::clone(&plugs_repo)),
        )?;
        let mut attempted_after_subscription = false;
        loop {
            let completion = tasks.next_completion().await?;
            tracing::debug!(?key, result = ?completion.result, "blob-pin plug machine task completed");
            match completion.result {
                Ok(PlugPinTaskOutput::Applied) => {
                    subscriptions.remove(&key);
                    return Ok(());
                }
                Ok(PlugPinTaskOutput::Deferred) => {
                    tracing::debug!(?key, "blob-pin plug machine task deferred");
                    let ref_url = match task.ref_url.as_ref() {
                        Some(ref_url) => ref_url.clone(),
                        None => plugs_repo
                            .enabled_ref(match &task.event {
                                crate::plugs::PlugsEvent::PlugEnabled { plug_id, .. }
                                | crate::plugs::PlugsEvent::PlugUpdated { plug_id, .. }
                                | crate::plugs::PlugsEvent::PlugDisabled { plug_id } => plug_id,
                                crate::plugs::PlugsEvent::PlugsConfigChanged { .. } => {
                                    unreachable!("config-only event cannot defer")
                                }
                            })
                            .await?
                            .ok_or_else(|| ferr!("enabled plug disappeared while materializing"))?,
                    };
                    if let std::collections::hash_map::Entry::Vacant(entry) =
                        subscriptions.entry(key)
                    {
                        let parsed = crate::plugs::PlugsRepo::parse_enabled_ref(&ref_url)?;
                        let branch_id = BranchId(parsed.doc_id.to_string());
                        entry.insert(
                            drawer
                                .subscribe_document_materialization(&branch_id)
                                .await?,
                        );
                        tracing::debug!(
                            ?key,
                            ?branch_id,
                            "blob-pin plug machine subscribed to manifest materialization"
                        );
                    }
                    if !attempted_after_subscription {
                        attempted_after_subscription = true;
                        tracing::debug!(
                            ?key,
                            "blob-pin plug machine retrying once after subscription"
                        );
                        tasks.replace(
                            key,
                            task.clone(),
                            run_plug_pin_task(
                                task.clone(),
                                Arc::clone(&self.ctx),
                                Arc::clone(&plugs_repo),
                            ),
                        )?;
                        continue;
                    }
                    tracing::debug!(?key, "blob-pin plug machine parking until materialization");
                    tasks.park(key, task.clone());
                    loop {
                        let change = subscriptions
                            .get_mut(&key)
                            .expect("parked plug task has a materialization subscription")
                            .ready_changed()
                            .await?;
                        tracing::debug!(
                            ?key,
                            ?change,
                            "blob-pin plug machine woke for materialization"
                        );
                        tasks.wake(
                            key,
                            run_plug_pin_task(
                                task.clone(),
                                Arc::clone(&self.ctx),
                                Arc::clone(&plugs_repo),
                            ),
                        )?;
                        let completion = tasks.next_completion().await?;
                        match completion.result {
                            Ok(PlugPinTaskOutput::Applied) => {
                                subscriptions.remove(&key);
                                return Ok(());
                            }
                            Ok(PlugPinTaskOutput::Deferred) => {
                                tasks.park(key, task.clone());
                            }
                            Err(error) => panic!("blob-pin plug task failed: {error:?}"),
                        }
                    }
                }
                Err(error) => panic!("blob-pin plug task failed: {error:?}"),
            }
        }
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        err(Debug),
        fields(worker = "blob-pin-plug-events-machine", doc_id = %self.core_inventory_doc_id),
    )]
    async fn run_plug_events_machine(
        &mut self,
        event_store: Arc<crate::plugs::PlugsConfigEventStore>,
        plugs_repo: Arc<crate::plugs::PlugsRepo>,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        let state = SqliteDeltaWalkerStateRepo::new(
            self.sql.read_pool.clone(),
            self.sql.write_pool.clone(),
            BLOB_PIN_PLUG_EVENTS_STATE_ID,
            "plug-events",
        )
        .await
        .map_err(|error| ferr!("initializing blob-pin plug-events walker state: {error}"))?;
        let durable = state.progress().await?.upstream_revision;
        let reader = event_store
            .open((), durable)
            .await
            .map_err(|error| ferr!("opening plugs event reader: {error}"))?;
        let mut walker: SerialDeltaWalker<
            '_,
            crate::plugs::PlugsConfigEventStore,
            SqliteDeltaWalkerStateRepo,
        > = SerialDeltaWalker::open(reader, &state)
            .await
            .map_err(|error| ferr!("opening plugs event walker: {error}"))?;
        let mut events_rx = plugs_repo.subscribe_events();
        let mut tasks = TokioKeyedScheduler::new(1);
        let mut subscriptions = HashMap::new();
        loop {
            let read: big_sync_core::revisioned_store::RevisionRead<
                u64,
                crate::plugs::PlugsConfigRevision,
            > = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => return Ok(()),
                event = events_rx.recv() => {
                    match event {
                        Ok(event) => {
                            let key = match &event {
                                crate::plugs::PlugsEvent::PlugEnabled { plug_id, .. }
                                | crate::plugs::PlugsEvent::PlugUpdated { plug_id, .. }
                                | crate::plugs::PlugsEvent::PlugDisabled { plug_id } => plug_pin_key(plug_id),
                                crate::plugs::PlugsEvent::PlugsConfigChanged { .. } => continue,
                            };
                            let task = PlugPinTask { key, event, ref_url: None };
                            self.process_plug_pin_task(
                                &self.drawer_repo,
                                Arc::clone(&plugs_repo),
                                &mut tasks,
                                &mut subscriptions,
                                task,
                            )
                            .await?;
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                            tracing::warn!(missed, "plugs event broadcast lagged");
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            return Err(ferr!("plugs event broadcast closed"));
                        }
                    }
                }
                read = walker.next() => read?,
            };
            match read {
                RevisionRead::ReplayComplete { .. } => {}
                RevisionRead::Entries { revision, entries } => {
                    for entry in entries {
                        for event in &entry.events {
                            let key = match event {
                                crate::plugs::PlugsEvent::PlugEnabled { plug_id, .. }
                                | crate::plugs::PlugsEvent::PlugUpdated { plug_id, .. }
                                | crate::plugs::PlugsEvent::PlugDisabled { plug_id } => {
                                    plug_pin_key(plug_id)
                                }
                                crate::plugs::PlugsEvent::PlugsConfigChanged { .. } => continue,
                            };
                            let ref_url = match event {
                                crate::plugs::PlugsEvent::PlugEnabled { plug_id, .. }
                                | crate::plugs::PlugsEvent::PlugUpdated { plug_id, .. } => {
                                    entry.config.enabled.get(plug_id).cloned()
                                }
                                _ => None,
                            };
                            self.process_plug_pin_task(
                                &self.drawer_repo,
                                Arc::clone(&plugs_repo),
                                &mut tasks,
                                &mut subscriptions,
                                PlugPinTask {
                                    key,
                                    event: event.clone(),
                                    ref_url,
                                },
                            )
                            .await?;
                        }
                    }
                    walker
                        .settle(revision)
                        .await
                        .map_err(|error| ferr!("settling plugs event walker: {error}"))?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_cx;
    use big_sync::DeltaWalkerStateRepo;
    use daybook_types::doc::{AddDocArgs, Blob, BranchPath, DocPatch, FacetRaw, WellKnownFacet};
    use daybook_types::manifest::{PlugManifest, WflowBundleManifest};

    /// Pins recorded as BlobPin facets on an inventory doc (the actual
    /// observable: the machines apply their state into these drawer docs).
    async fn inventory_blob_pins(
        drawer: &DrawerRepo,
        inventory_doc_id: &DocId,
    ) -> Res<HashMap<String, BlobPin>> {
        let Some(doc) = drawer
            .get_doc_with_facets_at_branch(
                inventory_doc_id,
                &daybook_types::doc::BranchPathBuf::from("main"),
                None,
            )
            .await?
        else {
            return Ok(HashMap::new());
        };
        let mut pins = HashMap::new();
        for (key, raw) in &doc.facets {
            if key.tag == WellKnownFacetTag::BlobPin.into()
                && let Ok(WellKnownFacet::BlobPin(pin)) =
                    WellKnownFacet::from_json(raw.clone(), WellKnownFacetTag::BlobPin)
            {
                pins.insert(key.id.clone(), pin);
            }
        }
        Ok(pins)
    }

    /// Boot the production pin worker for the duration of a test.
    ///
    /// The harness no longer spawns blob workers, and these lifecycle tests
    /// observe the pins the worker derives instead of driving it: the pin
    /// pipeline *is* the subject. Booting it here keeps the writer and the
    /// assertions in the same test.
    async fn spawn_pin_worker_for_test(
        test_context: &crate::test_support::DaybookTestContext,
    ) -> Res<crate::repos::RepoStopToken> {
        crate::blobs::spawn_blob_pin_worker(crate::blobs::BlobPinWorkerArgs {
            drawer_repo: Arc::clone(&test_context.rt.drawer),
            sql: test_context.rt.rcx.sql.clone(),
            core_inventory_doc_id: test_context.rt.rcx.core_inventory_doc_id.clone(),
            docs_inventory_doc_id: test_context.rt.rcx.docs_inventory_doc_id.clone(),
            encryption_inventory_doc_id: test_context.rt.rcx.encryption_inventory_doc_id.clone(),
            blobs_repo: Arc::clone(&test_context.rt.blobs_repo),
            facet_set_store: test_context.rt.doc_facet_set_index_repo.revision_store(),
            plugs_repo: Arc::clone(&test_context.rt.plugs_repo),
            parent_cancel_token: tokio_util::sync::CancellationToken::new(),
        })
        .await
    }

    async fn wait_for_pin_presence(
        drawer: &DrawerRepo,
        inventory_doc_id: &DocId,
        hash: &str,
        should_exist: bool,
    ) -> Res<()> {
        loop {
            let pins = inventory_blob_pins(drawer, inventory_doc_id).await?;
            if pins.contains_key(hash) == should_exist {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    async fn facet_walker_progress(sql: &SqlCtx) -> Res<u64> {
        let state = SqliteDeltaWalkerStateRepo::new(
            sql.read_pool.clone(),
            sql.write_pool.clone(),
            BLOB_PIN_STATE_LOCAL_STATE_ID,
            "facets",
        )
        .await?;
        Ok(state.progress().await?.upstream_revision)
    }

    /// Positive control for the boot ensurer: every production-shaped repo's
    /// configured inventories are its own init-minted, drawer-registered
    /// branches, so the spawn succeeds (the lifecycle tests below are the
    /// small proof this stays true against a real machine).
    #[tokio::test(flavor = "multi_thread")]
    async fn pin_worker_boot_ensurer_accepts_the_configured_inventory_branches() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let _pin_worker = spawn_pin_worker_for_test(&test_context).await?;
        Ok(())
    }

    /// Negative control: a configured inventory document that no local drawer
    /// entry registers is a broken config (inventory specification is config
    /// driven, ADR 003 §13), so the spawn fails at the boot edge naming that
    /// id, instead of the resolver's silent fallback spelling a foreign id
    /// through to a mid-run "headless patch" crash on the first nonempty
    /// inventory diff.
    #[tokio::test(flavor = "multi_thread")]
    async fn pin_worker_boot_ensurer_fails_a_spawn_whose_configured_inventory_has_no_drawer_branch()
    -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        for (what, configured) in [
            ("docs", DocumentId::random()),
            ("encryption", DocumentId::random()),
        ] {
            let err = crate::blobs::spawn_blob_pin_worker(crate::blobs::BlobPinWorkerArgs {
                drawer_repo: Arc::clone(&test_context.rt.drawer),
                sql: test_context.rt.rcx.sql.clone(),
                core_inventory_doc_id: test_context.rt.rcx.core_inventory_doc_id.clone(),
                // Only the slot under test points at the branchless document;
                // the other configured inventory stays the repo's own, so the
                // failure is attributable to the slot the loop names.
                docs_inventory_doc_id: if what == "docs" {
                    configured.clone()
                } else {
                    test_context.rt.rcx.docs_inventory_doc_id.clone()
                },
                encryption_inventory_doc_id: if what == "encryption" {
                    configured.clone()
                } else {
                    test_context.rt.rcx.encryption_inventory_doc_id.clone()
                },
                blobs_repo: Arc::clone(&test_context.rt.blobs_repo),
                facet_set_store: test_context.rt.doc_facet_set_index_repo.revision_store(),
                plugs_repo: Arc::clone(&test_context.rt.plugs_repo),
                parent_cancel_token: tokio_util::sync::CancellationToken::new(),
            })
            .await;
            let msg = match err {
                Ok(_) => panic!(
                    "the boot ensurer must refuse a configured {what} inventory \
                     with no drawer branch",
                ),
                Err(err) => format!("{err:#}"),
            };
            assert!(
                msg.contains("not a local drawer branch")
                    && msg.contains(configured.to_string().as_str()),
                "the failure must name the broken config ({what}): {msg}",
            );
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_blob_pin_worker_doc_lifecycle() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let _pin_worker = spawn_pin_worker_for_test(&test_context).await?;
        let drawer = &test_context.drawer_repo;
        let docs_inventory_doc_id = test_context.rt.rcx.docs_inventory_doc_id.to_string();
        let sql = test_context.rt.rcx.sql.clone();

        let blob_id_1 = test_context
            .rt
            .blobs_repo
            .put(b"test doc blob 1 content")
            .await?;
        let blob_id_2 = test_context
            .rt
            .blobs_repo
            .put(b"test doc blob 2 content")
            .await?;
        let hash_1 = blob_id_1.to_string();
        let hash_2 = blob_id_2.to_string();

        // 1. Add doc with Blob facet having hash_1 and hash_2
        let doc_id = test_context
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [
                    (
                        FacetKey::from(WellKnownFacetTag::Blob),
                        FacetRaw::from(WellKnownFacet::Blob(Blob {
                            mime: "application/octet-stream".to_string(),
                            length_octets: 1234,
                            digest: "bafakedigest".to_string(),
                            inline: None,
                            urls: Some(vec![
                                format!("{}:///{hash_1}", crate::blobs::BLOB_SCHEME),
                                format!("{}:///{hash_2}", crate::blobs::BLOB_SCHEME),
                            ]),
                        })),
                    ),
                    (
                        FacetKey::from(WellKnownFacetTag::Note),
                        FacetRaw::from(WellKnownFacet::Note("test note".into())),
                    ),
                ]
                .into(),
                user_path: None,
            })
            .await?;

        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_1, true).await?;
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_2, true).await?;
        let initial_progress = facet_walker_progress(&sql).await?;
        assert!(initial_progress > 0);

        let pins = inventory_blob_pins(drawer, &docs_inventory_doc_id).await?;
        assert_eq!(pins.get(&hash_1).unwrap().length_octets, 1234);
        assert_eq!(pins.get(&hash_2).unwrap().length_octets, 1234);

        // 2. Update doc to only retain hash_1
        test_context
            .drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: [(
                        FacetKey::from(WellKnownFacetTag::Blob),
                        FacetRaw::from(WellKnownFacet::Blob(Blob {
                            mime: "application/octet-stream".to_string(),
                            length_octets: 1234,
                            digest: "bafakedigest".to_string(),
                            inline: None,
                            urls: Some(vec![format!("{}:///{hash_1}", crate::blobs::BLOB_SCHEME)]),
                        })),
                    )]
                    .into(),
                    facets_remove: vec![],
                    user_path: None,
                },
                BranchPath::new("main"),
                None,
            )
            .await?;

        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_2, false).await?;
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_1, true).await?;
        let update_progress = facet_walker_progress(&sql).await?;
        assert!(update_progress > initial_progress);

        // Repeating the same logical value is a new source revision but must
        // leave the projected inventory unchanged.
        test_context
            .drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: [(
                        FacetKey::from(WellKnownFacetTag::Blob),
                        FacetRaw::from(WellKnownFacet::Blob(Blob {
                            mime: "application/octet-stream".to_string(),
                            length_octets: 1234,
                            digest: "bafakedigest".to_string(),
                            inline: None,
                            urls: Some(vec![format!("{}:///{hash_1}", crate::blobs::BLOB_SCHEME)]),
                        })),
                    )]
                    .into(),
                    facets_remove: vec![],
                    user_path: None,
                },
                BranchPath::new("main"),
                None,
            )
            .await?;
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_2, false).await?;
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_1, true).await?;

        // A second branch owns the same pin independently. Removing it from
        // main must not unpin it until the branch is removed as well.
        let main_heads = test_context
            .drawer_repo
            .get_branch_heads_for_path(&doc_id, BranchPath::new("main"))
            .await?
            .ok_or_eyre("missing main branch heads")?;
        let branch_path = BranchPathBuf::from("/test/blob-pin-branch");
        test_context
            .drawer_repo
            .create_branch_at_heads_from_branch(
                &doc_id,
                &branch_path,
                BranchPath::new("main"),
                &main_heads,
                None,
            )
            .await?;
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_1, true).await?;

        test_context
            .drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: default(),
                    facets_remove: vec![FacetKey::from(WellKnownFacetTag::Blob)],
                    user_path: None,
                },
                BranchPath::new("main"),
                None,
            )
            .await?;
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_1, true).await?;

        let branch_heads = test_context
            .drawer_repo
            .get_branch_heads_for_path(&doc_id, &branch_path)
            .await?
            .ok_or_eyre("missing blob-pin branch heads")?;
        test_context
            .drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: default(),
                    facets_remove: vec![FacetKey::from(WellKnownFacetTag::Blob)],
                    user_path: None,
                },
                BranchPath::new(branch_path.as_str()),
                Some(branch_heads),
            )
            .await?;
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_1, false).await?;

        // 3. Delete doc
        test_context.drawer_repo.del(&doc_id).await?;
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_1, false).await?;

        test_context.stop().await?;
        Ok(())
    }

    /// The enablement-driven plug lifecycle: authoring bakes the manifest's
    /// blob references as Blob facets on the manifest doc; enabling the plug
    /// (a config facet revision) drives the core inventory pins; disabling
    /// drops them. Manifest blob facets never flow into the docs inventory.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_blob_pin_worker_plug_lifecycle() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let _pin_worker = spawn_pin_worker_for_test(&test_context).await?;
        let drawer = &test_context.drawer_repo;
        let core_inventory_doc_id = test_context.rt.rcx.core_inventory_doc_id.to_string();
        let docs_inventory_doc_id = test_context.rt.rcx.docs_inventory_doc_id.to_string();
        let plugs = &test_context.rt.plugs_repo;

        let blob_id = test_context
            .rt
            .blobs_repo
            .put(b"test wasm bundle content")
            .await?;
        let hash = blob_id.to_string();

        // 1. Author the plug: `add` bakes the manifest's blob references as
        //    Blob facets on the manifest doc (the static artifact carries its
        //    own blob declarations).
        let manifest = PlugManifest {
            namespace: "test".into(),
            name: "sample-plug".into(),
            version: "0.1.0".parse().unwrap(),
            title: "Sample Plug".into(),
            desc: "A test plug".into(),
            facets: default(),
            local_states: default(),
            dependencies: default(),
            routines: default(),
            wflow_bundles: [(
                "bundle1".into(),
                Arc::new(WflowBundleManifest {
                    keys: vec!["wflow1".into()],
                    component_urls: vec![
                        format!("{}:///{hash}", crate::blobs::BLOB_SCHEME)
                            .parse()
                            .unwrap(),
                    ],
                }),
            )]
            .into(),
            views: default(),
            commands: default(),
            inits: default(),
            processors: default(),
        };
        let doc_id = plugs.add(manifest).await?;

        // 2. Enable: the config revision's PlugEnabled drives the core
        //    inventory pins.
        let ref_url: url::Url =
            format!("db+facet:///{doc_id}/org.example.daybook.plugManifest/main?branch=main")
                .parse()?;
        plugs.enable_plug(&ref_url).await?;
        wait_for_pin_presence(drawer, &core_inventory_doc_id, &hash, true).await?;

        // 3. Manifest blobs follow enablement only: the docs inventory must
        //    not pin them (manifest-doc exclusion in the facet machine).
        let docs_pins = inventory_blob_pins(drawer, &docs_inventory_doc_id).await?;
        assert!(
            !docs_pins.contains_key(&hash),
            "manifest blob facets must not flow into the docs inventory"
        );

        // 4. Disable: the PlugDisabled event drops the plug's pins.
        plugs.disable_plug("@test/sample-plug").await?;
        wait_for_pin_presence(drawer, &core_inventory_doc_id, &hash, false).await?;

        test_context.stop().await?;
        Ok(())
    }

    /// A peer authors a `Blob` facet as it likes, and the facet's digest is what
    /// the pin becomes: a reserved `/…` spelling names no blob and must not
    /// become a pin (from which a path would later be built).
    #[tokio::test(flavor = "multi_thread")]
    async fn reserved_facet_blob_digest_never_becomes_a_pin() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let drawer = &test_context.drawer_repo;
        let docs_inventory_doc_id = test_context.rt.rcx.docs_inventory_doc_id.to_string();
        // The worker these assertions observe: main's harness booted it ambiently;
        // ADR 003's fork regime spawns it explicitly. The pin worker's spawn owns
        // both machines the test reads (the facet machine and the plug-events
        // machine), so one spawn covers the pin derivations below.
        let _pin_worker = spawn_pin_worker_for_test(&test_context).await?;
        let reserved = "/etc/daybook-escape".to_string();

        let control_blob_id = test_context
            .rt
            .blobs_repo
            .put(b"reserved-spelling-control-blob")
            .await?;
        let control_hash = control_blob_id.to_string();

        test_context
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::Blob),
                    FacetRaw::from(WellKnownFacet::Blob(Blob {
                        mime: "application/octet-stream".to_string(),
                        length_octets: 1234,
                        digest: reserved.clone(),
                        inline: None,
                        urls: Some(vec![format!(
                            "{}:///{control_hash}",
                            crate::blobs::BLOB_SCHEME
                        )]),
                    })),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        // The control pin proves this doc revision's pins were applied; the
        // reserved digest is in the same revision and must not be among them.
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &control_hash, true).await?;
        let pins = inventory_blob_pins(drawer, &docs_inventory_doc_id).await?;
        assert!(
            pins.contains_key(&control_hash),
            "control pin missing: {:?}",
            pins.keys().collect::<Vec<_>>()
        );
        assert!(
            !pins.contains_key(&reserved),
            "a reserved digest spelling became a pin: {reserved:?}"
        );

        test_context.stop().await?;
        Ok(())
    }

    /// How many plugs still reference a blob, read from the plug-pin rows the
    /// teardown deletes and the `remaining_count` gate counts.
    async fn plug_pin_rows(sql: &SqlCtx, plug_id: &str) -> Res<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM blob_pin_plug_state WHERE plug_id = ?1",
        )
        .bind(plug_id)
        .fetch_one(&sql.write_pool)
        .await?)
    }

    /// Every `(namespace, consumer_id)` pair that owns a delta-walker progress
    /// row in the daybook database.
    async fn walker_progress_identities(sql: &SqlCtx) -> Res<Vec<(String, String)>> {
        Ok(sqlx::query_as::<_, (String, String)>(
            "SELECT namespace, consumer_id FROM delta_walker_progress",
        )
        .fetch_all(&sql.read_pool)
        .await?)
    }

    /// The plug-events walker's own durable progress row, read straight from the
    /// table so the reader cannot create the row it is looking for.
    async fn plug_events_progress_row(sql: &SqlCtx) -> Res<Option<u64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT upstream_revision FROM delta_walker_progress \
              WHERE namespace = ?1 AND consumer_id = 'plug-events'",
        )
        .bind(BLOB_PIN_PLUG_EVENTS_STATE_ID)
        .fetch_optional(&sql.read_pool)
        .await?
        .map(|progress| progress as u64))
    }

    /// The plug teardown is refcounted: dropping one plug's rows must not evict a
    /// blob pin that another plug still references, and the eviction must happen
    /// once the last reference is gone.
    ///
    /// `test_blob_pin_worker_plug_lifecycle` covers the single-plug case end to
    /// end; the two-plug refcount is where a teardown that ignores
    /// `remaining_count` silently unpins live data — the blob becomes evictable
    /// while a plug still needs it. Both halves are asserted here: after the
    /// first drop the row deletion is committed and the facet survives, after the
    /// last drop the facet goes.
    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_a_plug_pin_keeps_a_blob_another_plug_still_pins() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let drawer_repo = Arc::clone(&test_context.drawer_repo);
        // The same resolution the worker does at spawn, so this context writes
        // the facet to the document the running machine writes to.
        let core_inventory_doc_id = drawer_repo
            .resolve_doc_id_for_branch_doc_id(test_context.rt.rcx.core_inventory_doc_id.clone())
            .await?;
        let docs_inventory_doc_id = drawer_repo
            .resolve_doc_id_for_branch_doc_id(test_context.rt.rcx.docs_inventory_doc_id.clone())
            .await?;
        let encryption_inventory_doc_id = drawer_repo
            .resolve_doc_id_for_branch_doc_id(
                test_context.rt.rcx.encryption_inventory_doc_id.clone(),
            )
            .await?;
        // The hand-built context bypasses the spawn path, so it owes the schema
        // the spawn owns (Ctx::ensure_schema at :57) itself.
        Ctx::ensure_schema(&test_context.rt.rcx.sql).await?;
        let ctx = Ctx {
            drawer_repo: Arc::clone(&drawer_repo),
            sql: test_context.rt.rcx.sql.clone(),
            core_inventory_doc_id: core_inventory_doc_id.clone(),
            docs_inventory_doc_id: docs_inventory_doc_id.clone(),
            encryption_inventory_doc_id,
            store: test_context.rt.blobs_repo.iroh_store(),
            pair_roots: crate::blobs::pair_roots::PairRoots::boot(test_context.rt.rcx.sql.clone())
                .await?,
            inventory_lock: Arc::new(tokio::sync::Mutex::new(())),
        };
        let watched_doc_id = core_inventory_doc_id.clone();
        let hash = "refcounted-plug-blob".to_string();
        let pins = HashMap::from([(hash.clone(), 64_u64)]);

        // Two plugs referencing one blob, each through the production upsert.
        ctx.apply_plug_pins("@test/refcount-a", pins.clone())
            .await?;
        ctx.apply_plug_pins("@test/refcount-b", pins.clone())
            .await?;
        assert!(
            inventory_blob_pins(&drawer_repo, &watched_doc_id)
                .await?
                .contains_key(&hash),
            "two plugs pinning one blob leave one pin facet"
        );

        ctx.drop_plug_pins("@test/refcount-a").await?;
        {
            let pins = inventory_blob_pins(&drawer_repo, &watched_doc_id).await?;
            assert!(
                pins.contains_key(&hash),
                "a blob another plug still pins must survive the first plug's teardown: {:?}",
                pins.keys().collect::<Vec<_>>()
            );
        }
        assert_eq!(
            plug_pin_rows(&ctx.sql, "@test/refcount-a").await?,
            0,
            "the dropped plug's rows are deleted and committed before the facet decision"
        );
        assert_eq!(
            plug_pin_rows(&ctx.sql, "@test/refcount-b").await?,
            1,
            "the surviving plug's row is untouched"
        );

        ctx.drop_plug_pins("@test/refcount-b").await?;
        assert!(
            !inventory_blob_pins(&drawer_repo, &watched_doc_id)
                .await?
                .contains_key(&hash),
            "the last plug's teardown evicts the pin"
        );
        assert_eq!(plug_pin_rows(&ctx.sql, "@test/refcount-b").await?, 0);

        test_context.stop().await?;
        Ok(())
    }

    /// The plug-events walker keeps its cursor in its own `(namespace,
    /// consumer_id)` row and nothing else writes it: one durable writer per
    /// consumer, so a second watermark cannot re-enter through a new identity.
    ///
    /// The facet machine's identity is a separate row — the two machines are
    /// separate consumers — and a fresh construction of the plug-events identity
    /// reads back exactly the row that is in the table, which is what a restart
    /// resuming "at its own progress" means. The writer is stopped first, so the
    /// row is read at a fixed point rather than racing the machine's own
    /// settlement of the revisions it is still consuming.
    ///
    /// Deliberately not asserted: *how far* the machine has settled by the time it
    /// is stopped. The walker advances on its own task's schedule, so the exact
    /// revision it reached is not a property of the contract; that a restart reads
    /// back whatever it reached, and never moves backwards, is.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_plug_events_walker_resumes_at_the_pin_workers_own_progress_row() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let sql = test_context.rt.rcx.sql.clone();
        let core_inventory_doc_id = test_context.rt.rcx.core_inventory_doc_id.to_string();
        let drawer = &test_context.drawer_repo;
        // The plug-events machine lives inside the pin worker's spawn (both
        // machines join in spawn_blob_pin_worker), so the test owes it like the
        // other pin-derivation tests do.
        let _pin_worker = spawn_pin_worker_for_test(&test_context).await?;

        let blob_id = test_context
            .rt
            .blobs_repo
            .put(b"plug-events-walker-progress")
            .await?;
        let hash = blob_id.to_string();
        let manifest = PlugManifest {
            namespace: "test".into(),
            name: "progress-plug".into(),
            version: "0.1.0".parse().unwrap(),
            title: "Progress Plug".into(),
            desc: "drives the plug-events walker".into(),
            facets: default(),
            local_states: default(),
            dependencies: default(),
            routines: default(),
            wflow_bundles: [(
                "bundle1".into(),
                Arc::new(WflowBundleManifest {
                    keys: vec!["wflow1".into()],
                    component_urls: vec![
                        format!("{}:///{hash}", crate::blobs::BLOB_SCHEME)
                            .parse()
                            .unwrap(),
                    ],
                }),
            )]
            .into(),
            views: default(),
            commands: default(),
            inits: default(),
            processors: default(),
        };
        let doc_id = test_context.rt.plugs_repo.add(manifest).await?;
        let ref_url: url::Url =
            format!("db+facet:///{doc_id}/org.example.daybook.plugManifest/main?branch=main")
                .parse()?;
        let consumed_before = plug_events_progress_row(&sql).await?.unwrap_or(0);
        test_context.rt.plugs_repo.enable_plug(&ref_url).await?;

        // The pin is written by the plug-events machine's own task, which creates
        // its progress row before it processes anything, so the pin's appearance
        // is what puts the row there to read. (`wait_for_pin_presence` is this
        // module's existing convergence helper, already used by its other tests.)
        wait_for_pin_presence(drawer, &core_inventory_doc_id, &hash, true).await?;

        // Rows are `(namespace, consumer_id)`, and the machine builds its state repo as
        // `SqliteDeltaWalkerStateRepo::new(.., BLOB_PIN_PLUG_EVENTS_STATE_ID, "plug-events")`:
        // the state id is the NAMESPACE and `plug-events` is the consumer id. So this
        // collects the namespaces that own the plug-events consumer id — exactly one
        // namespace may, or two machines are writing the same consumer's progress.
        let identities = walker_progress_identities(&sql).await?;
        let plug_events_namespaces: Vec<String> = identities
            .iter()
            .filter(|identity| identity.1 == "plug-events")
            .map(|identity| identity.0.clone())
            .collect();
        assert_eq!(
            plug_events_namespaces,
            vec![BLOB_PIN_PLUG_EVENTS_STATE_ID.to_string()],
            "one namespace owns the plug-events consumer id: {identities:?}"
        );
        assert!(
            identities
                .iter()
                .any(|identity| identity.1 == "facets"
                    && identity.0 == BLOB_PIN_STATE_LOCAL_STATE_ID),
            "the facet machine keeps its own namespace, not this one: {identities:?}"
        );

        // Stop joins the blob-pin supervisor and both machines before comparing
        // the durable row with a reopened reader. Otherwise the live writer can
        // advance between those reads. The cloned SQL handle retains the DB.
        test_context.stop().await?;

        // A restart reads the exact durable revision after its writer has stopped.
        let durable_row = plug_events_progress_row(&sql)
            .await?
            .expect("the machine's task created its progress row before it processed anything");
        assert!(
            durable_row >= consumed_before,
            "a durable revision never moves backwards: was {consumed_before}, now {durable_row}"
        );
        let reopened = SqliteDeltaWalkerStateRepo::new(
            sql.read_pool.clone(),
            sql.write_pool.clone(),
            BLOB_PIN_PLUG_EVENTS_STATE_ID,
            "plug-events",
        )
        .await?;
        assert_eq!(
            reopened.retention_reader_id(),
            "@daybook/core/blob-pin-plug-events/plug-events"
        );
        assert_eq!(
            reopened.progress().await?.upstream_revision,
            durable_row,
            "a restart of the walker resumes at its own progress row"
        );
        Ok(())
    }

    /// The drain's decision table, without a database or a blob store.
    ///
    /// The decision is what has to be right: releasing a pair a document still
    /// names unroots a live representation, while leaving a pair nothing names
    /// strands its ciphertext and the plaintext serving it (ADR 003 §19).
    #[test]
    fn test_pair_root_drain_decision_table() {
        use DrainAction::*;
        // Nothing rooted: only the row is stale.
        assert_eq!(Ctx::drain_action(false, false, None), ClearRow);
        assert_eq!(Ctx::drain_action(false, true, Some(true)), ClearRow);
        // Rooted and pinned: the pin machinery owns the pair and its release.
        assert_eq!(Ctx::drain_action(true, true, None), ClearRow);
        assert_eq!(Ctx::drain_action(true, true, Some(false)), ClearRow);
        // Rooted, unpinned, and a durable facet names it: the machine will pin it.
        assert_eq!(Ctx::drain_action(true, false, Some(true)), Keep);
        // Rooted, unpinned, and nothing names it: the crash window.
        assert_eq!(Ctx::drain_action(true, false, Some(false)), Release);
        // Rooted, unpinned, and uncheckable: never release.
        assert_eq!(Ctx::drain_action(true, false, None), KeepUnprovenanced);
    }

    /// The boot drain resolves exactly the pairs a crash left rooted and unclaimed.
    ///
    /// Both pairs here have the shape a crash between the tags and the facet write
    /// leaves: rooted, and invisible to the release path, which is built from
    /// facets. One carries provenance to a document that names no representation,
    /// so nothing will ever claim it and the tags must go; one carries no
    /// provenance at all, so the drain cannot check it and must leave the pair
    /// rooted (ADR 003 §19).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_pair_root_drain_releases_only_unclaimed_pairs() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let sql = test_context.rt.rcx.sql.clone();
        let store = test_context.rt.blobs_repo.iroh_store();
        let roots = crate::blobs::pair_roots::PairRoots::boot(sql.clone()).await?;

        let plaintext = test_context
            .rt
            .blobs_repo
            .put(b"unclaimed plaintext")
            .await?;
        let unprovenanced = test_context
            .rt
            .blobs_repo
            .put(b"pair with no provenance")
            .await?;
        let checked = test_context.rt.blobs_repo.put(b"pair to check").await?;
        root_pair_tags(&store, unprovenanced.clone(), plaintext.clone()).await?;
        root_pair_tags(&store, checked.clone(), plaintext).await?;
        roots
            .record_before_root(crate::blobs::blob_id_to_iroh_hash(unprovenanced.clone()))
            .await?;
        roots
            .record_before_root(crate::blobs::blob_id_to_iroh_hash(checked.clone()))
            .await?;

        // A document that exists and names no representation. Provenance lets the
        // drain read it, and reading it is what answers "nothing claims this pair".
        let doc_id = test_context
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::Note),
                    FacetRaw::from(WellKnownFacet::Note("names no representation".into())),
                )]
                .into(),
                user_path: None,
            })
            .await?;
        roots
            .attach_provenance(
                crate::blobs::blob_id_to_iroh_hash(checked.clone()),
                &doc_id,
                "main",
            )
            .await?;

        let _pin_worker = spawn_pin_worker_for_test(&test_context).await?;

        wait_for_pair_tags_absent(&store, &checked).await?;
        assert!(
            pair_tags_present(&store, &unprovenanced).await?,
            "a pair the drain cannot check must stay rooted rather than be released blind"
        );
        let unresolved = roots.unresolved().await?;
        assert_eq!(
            unresolved.len(),
            1,
            "the drain retires the row it resolves and leaves the one it cannot"
        );
        assert_eq!(
            unresolved[0].cipher_hash,
            crate::blobs::blob_id_to_iroh_hash(unprovenanced)
        );

        test_context.stop().await?;
        Ok(())
    }

    /// A pair a durable facet still names is live, even with no pin derived yet.
    ///
    /// This is the window the ledger must *not* release in: the facet exists, so the
    /// facet machine is about to derive the pin, and dropping the tags now would
    /// unroot a representation a document serves. The pin write then retires the
    /// row without touching the tags (ADR 003 §19).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_pair_root_drain_keeps_a_pair_a_facet_still_names() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let drawer = Arc::clone(&test_context.drawer_repo);
        let sql = test_context.rt.rcx.sql.clone();
        let store = test_context.rt.blobs_repo.iroh_store();
        let roots = crate::blobs::pair_roots::PairRoots::boot(sql.clone()).await?;
        let encryption_inventory_doc_id = drawer
            .resolve_doc_id_for_branch_doc_id(
                test_context.rt.rcx.encryption_inventory_doc_id.clone(),
            )
            .await?;

        let plaintext = test_context
            .rt
            .blobs_repo
            .put(b"live pair plaintext")
            .await?;
        let cipher = test_context
            .rt
            .blobs_repo
            .put(b"live pair representation")
            .await?;
        root_pair_tags(&store, cipher.clone(), plaintext).await?;
        let c_hash = crate::blobs::blob_id_to_iroh_hash(cipher.clone());
        roots.record_before_root(c_hash).await?;

        // The facet the crash did not get to write, authored the way the encryption
        // worker writes it: a cipherBlob facet carries the digest in the multihash
        // spelling, and it is the facet's value - not its key - that names `C`.
        let (key_doc_id, key_heads) = crate::test_support::stage_key_doc(
            &drawer,
            &crate::blobs::encrypt::MasterKey::random(),
        )
        .await?;
        let key_ref = format!("db+facet:///{key_doc_id}/org.example.daybook.jwk/relay");
        let cipher_digest = crate::blobs::blob_id_to_digest_str(cipher.clone());
        // A plain document first: a cipherBlob facet is system-managed, so it takes
        // the scope the encryption worker writes it with rather than a user write.
        let doc_id = drawer
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::Note),
                    FacetRaw::from(WellKnownFacet::Note("names a representation".into())),
                )]
                .into(),
                user_path: None,
            })
            .await?;
        drawer
            .update_at_heads_with_scope(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: [(
                        FacetKey::from(WellKnownFacetTag::CipherBlob),
                        cipher_blob_facet(&cipher_digest, 4096, &key_ref, key_heads)?,
                    )]
                    .into(),
                    facets_remove: vec![],
                    user_path: None,
                },
                BranchPath::new("main"),
                None,
                crate::drawer::FacetWriteScope::System,
            )
            .await?;
        roots.attach_provenance(c_hash, &doc_id, "main").await?;

        let _pin_worker = spawn_pin_worker_for_test(&test_context).await?;

        // The machine claims the pair: the pin lands in the encryption inventory and
        // the tags the drain left alone are still there.
        wait_for_pin_presence(&drawer, &encryption_inventory_doc_id, &cipher_digest, true).await?;
        assert!(
            pair_tags_present(&store, &cipher).await?,
            "a pair a durable facet names is live, so its tags must survive the drain"
        );
        assert!(
            roots.unresolved().await?.is_empty(),
            "the pin write retires the ledger row it now owns"
        );

        test_context.stop().await?;
        Ok(())
    }

    /// The tag name `encrypt.rs` writes for a pair: named from the ciphertext
    /// `Hash`, whose `Display` is hex - not from the daybook `BlobId`, which is
    /// bs58. Both readings of "the pair tag" must use this, or a test asserts
    /// against names nothing ever writes.
    fn pair_tag_name(prefix: &str, cipher: &crate::blobs::BlobId) -> String {
        format!(
            "{prefix}{}",
            iroh_blobs::Hash::from_bytes(cipher.to_bytes32())
        )
    }

    /// Stage a pair the way `CipherBlobProvider::register_pair` leaves it: both
    /// durable tags present, so the pair is rooted.
    async fn root_pair_tags(
        store: &iroh_blobs::api::Store,
        cipher: crate::blobs::BlobId,
        plaintext: crate::blobs::BlobId,
    ) -> Res<()> {
        // Production (`set_pair_tags`, encrypt.rs) names BOTH tags from the
        // ciphertext hash; the pt tag's *value* names the plaintext.
        for (prefix, named, valued) in [
            (
                crate::blobs::encrypt::TAG_CT_PREFIX,
                cipher.clone(),
                cipher.clone(),
            ),
            (
                crate::blobs::encrypt::TAG_PT_PREFIX,
                cipher.clone(),
                plaintext.clone(),
            ),
        ] {
            store
                .tags()
                .set(
                    pair_tag_name(prefix, &named),
                    iroh_blobs::HashAndFormat {
                        hash: crate::blobs::blob_id_to_iroh_hash(valued),
                        format: iroh_blobs::BlobFormat::Raw,
                    },
                )
                .await?;
        }
        Ok(())
    }

    async fn pair_tags_present(
        store: &iroh_blobs::api::Store,
        cipher: &crate::blobs::BlobId,
    ) -> Res<bool> {
        let ct = store
            .tags()
            .get(pair_tag_name(crate::blobs::encrypt::TAG_CT_PREFIX, cipher))
            .await?;
        let pt = store
            .tags()
            .get(pair_tag_name(crate::blobs::encrypt::TAG_PT_PREFIX, cipher))
            .await?;
        Ok(ct.is_some() && pt.is_some())
    }

    async fn wait_for_pair_tags_absent(
        store: &iroh_blobs::api::Store,
        cipher: &crate::blobs::BlobId,
    ) -> Res<()> {
        loop {
            if !pair_tags_present(store, cipher).await? {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    fn cipher_blob_facet(
        digest: &str,
        length_octets: u64,
        key_ref: &str,
        key_ref_heads: ChangeHashSet,
    ) -> Res<FacetRaw> {
        Ok(FacetRaw::from(WellKnownFacet::CipherBlob(
            daybook_types::doc::CipherBlob {
                representation: daybook_types::doc::Representation {
                    digest: digest.to_string(),
                    length_octets,
                },
                content_encoding: "aes128gcm".to_string(),
                key_ref: key_ref.parse()?,
                key_ref_heads,
                encoding_parameters: serde_json::json!({
                    "recordSize": 65_536,
                    "padding": "record",
                }),
            },
        )))
    }

    /// A `cipherBlob` facet is a pin source, but only for the encrypted-
    /// representation inventory: the digest is what a relay is asked to hold and
    /// what the pair tags are named from. Removing the facet releases the pair -
    /// the pin leaves the inventory and both `ct:`/`pt:` tags go with it, which
    /// is the only thing that makes the ciphertext's outboard and the plaintext
    /// serving it collectable again (ADR 003 §13/§19).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_blob_pin_worker_cipher_inventory_lifecycle() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let _pin_worker = spawn_pin_worker_for_test(&test_context).await?;
        let drawer = &test_context.drawer_repo;
        let docs_inventory_doc_id = drawer
            .resolve_doc_id_for_branch_doc_id(test_context.rt.rcx.docs_inventory_doc_id.clone())
            .await?;
        let encryption_inventory_doc_id = drawer
            .resolve_doc_id_for_branch_doc_id(
                test_context.rt.rcx.encryption_inventory_doc_id.clone(),
            )
            .await?;
        let store = test_context.rt.blobs_repo.iroh_store();

        let plaintext = test_context
            .rt
            .blobs_repo
            .put(b"pin worker cipher source")
            .await?;
        let cipher = test_context
            .rt
            .blobs_repo
            .put(b"pin worker cipher representation")
            .await?;
        let plaintext_hash = plaintext.to_string();
        // ADR 003 §3 spells a representation digest as a multihash, and that is
        // the only carrier a cipherBlob facet has, so the fixture uses it.
        let cipher_hash = crate::blobs::blob_id_to_digest_str(cipher.clone());
        root_pair_tags(&store, cipher.clone(), plaintext).await?;

        let doc_id = drawer
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::Blob),
                    FacetRaw::from(WellKnownFacet::Blob(Blob {
                        mime: "application/octet-stream".to_string(),
                        length_octets: 1234,
                        digest: "bafakedigest".to_string(),
                        inline: None,
                        urls: Some(vec![format!(
                            "{}:///{plaintext_hash}",
                            crate::blobs::BLOB_SCHEME
                        )]),
                    })),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        // The facet is system-managed, so it takes the scope the encryption
        // worker will write it with rather than an ordinary user write.
        let (key_doc_id, key_heads) =
            crate::test_support::stage_key_doc(drawer, &crate::blobs::encrypt::MasterKey::random())
                .await?;
        let key_ref = format!("db+facet:///{key_doc_id}/org.example.daybook.jwk/relay");
        let cipher_facet_key = FacetKey::from(WellKnownFacetTag::CipherBlob);
        drawer
            .update_at_heads_with_scope(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: [(
                        cipher_facet_key.clone(),
                        cipher_blob_facet(&cipher_hash, 4096, &key_ref, key_heads)?,
                    )]
                    .into(),
                    facets_remove: vec![],
                    user_path: None,
                },
                BranchPath::new("main"),
                None,
                crate::drawer::FacetWriteScope::System,
            )
            .await?;

        wait_for_pin_presence(drawer, &encryption_inventory_doc_id, &cipher_hash, true).await?;
        let cipher_pins = inventory_blob_pins(drawer, &encryption_inventory_doc_id).await?;
        assert_eq!(cipher_pins.get(&cipher_hash).unwrap().length_octets, 4096);
        assert!(
            pair_tags_present(&store, &cipher).await?,
            "the pair is still rooted while the facet names it"
        );

        // The plaintext pin still lands in the docs inventory, and the
        // ciphertext digest must not be there. A plaintext inventory is read by
        // peers that only hold plaintext.
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &plaintext_hash, true).await?;
        assert!(
            !inventory_blob_pins(drawer, &docs_inventory_doc_id)
                .await?
                .contains_key(&cipher_hash),
            "a ciphertext digest must never enter a plaintext inventory"
        );

        // Removing the facet releases the pair.
        drawer
            .update_at_heads_with_scope(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: default(),
                    facets_remove: vec![cipher_facet_key],
                    user_path: None,
                },
                BranchPath::new("main"),
                None,
                crate::drawer::FacetWriteScope::System,
            )
            .await?;

        wait_for_pin_presence(drawer, &encryption_inventory_doc_id, &cipher_hash, false).await?;
        wait_for_pair_tags_absent(&store, &cipher).await?;
        assert!(
            inventory_blob_pins(drawer, &docs_inventory_doc_id)
                .await?
                .contains_key(&plaintext_hash),
            "releasing the ciphertext must not disturb the plaintext pin"
        );

        test_context.stop().await?;
        Ok(())
    }
}
