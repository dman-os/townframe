//! Micros that cartridges contribute (Clay on the node section 5, ADR 049 sections 3 and 6).
//!
//! A cartridge in the node's catalog may carry a micro beside its nodule: one envelope, one
//! install datum, each component enabled on its own. [`NodeComposition::sync_cartridge_micros`]
//! reconciles the micros of every install whose `micro` member is enabled:
//!
//! 1. the cartridges service resolves the install's pin to the envelope and its payloads
//!    (`resolveCartridgePin`, the same read a nodule's install takes);
//! 2. the typed [`big_cartridges_core::MicroContribution`] names the component by key, so the wasm
//!    bytes are read out of the blob store by the digest the envelope pins, and its Clay manifest
//!    is the contribution's `description` plus the identity the envelope owns (`id`,
//!    `package_version`, `authority`), which is the sidecar a host loads a package from;
//! 3. the micro's handles are minted from its `uses` intersected with the cartridge's grants. The
//!    grants are resolved as the nodule's are ([`ResolvedGrants`]) and lowered by
//!    [`big_nodule::RoutineScopes::lower`], the function that lowers a routine's `uses`, so a
//!    `uses` entry confers the same thing in either runtime. The handles are bound to the micro's
//!    own actor and held by the host: the guest names only operations.
//!
//! A micro with no granted `uses` is minted nothing and can call nothing. Each micro then has a
//! follower: a task that long-polls the cartridge's grants (`awaitCartridgeGrants`) and, when they
//! move, retracts the handles whose scope changed and mints them again, so the micro's next call
//! sees the new grant. The follower is owned by the micro's entry in the composition and stops when
//! the micro is dropped or the composition stops.

use crate::Installed;
use crate::Minted;
use crate::MintedState;
use crate::Minting;
use crate::NodeComposition;
use crate::mint_handles;
use crate::retract;
use big_blobs_core::BlobId;
use big_cartridges_core::CLAY_MICRO_MANIFEST_SCHEMA;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Component;
use big_cartridges_core::ResolvedCartridge;
use big_cartridges_core::protocol::AWAIT_MAX_HOLD_MS;
use big_cartridges_core::protocol::AwaitCartridgeGrants;
use big_cartridges_core::protocol::CartridgeGrantsAt;
use big_cartridges_core::protocol::CartridgeInstallRecord;
use big_cartridges_core::protocol::CartridgesError;
use big_cartridges_core::protocol::ListCartridgeInstalls;
use big_cartridges_core::protocol::ResolveCartridgePin;
use serde_json::Value;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use service_utils_rs::Spawner;
use service_utils_rs::StopToken;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::task::JoinHandle;
use utils_rs::prelude::Res;
use utils_rs::prelude::WrapErr;
use utils_rs::prelude::ferr;

/// The largest micro component the loader reads. The whole component is held in memory while it
/// compiles, as a nodule's handler is (`big_nodule_wasm`), so a hostile or mistaken envelope is
/// bounded by the same 64 MiB.
const MAX_COMPONENT_OCTETS: u64 = 64 * 1024 * 1024;

/// One micro a cartridge contributes, as a host loads a package from it: `manifest_json` and
/// `component` are the sidecar JSON and the wasm bytes of a standalone package, and `micro` is the
/// id the host installs it as, which is the cartridge's id (Clay on the node section 5).
#[derive(Debug, Clone)]
pub struct CartridgeMicro {
    /// The cartridge id text, `<did>/<name>`: the key of [`NodeComposition::egress`].
    pub micro: String,
    pub manifest_json: Vec<u8>,
    pub component: Vec<u8>,
}

impl NodeComposition {
    /// Reconciles the micros of the installed cartridges with what this composition holds, and
    /// answers the micros that are now installed, each as the host loads it. An entry that is
    /// `Err` could not be loaded and is the host's to report beside its other installation errors;
    /// it never stops the others. Its message says whether it is retried: a pin whose envelope or
    /// payloads have not reached this node yet is skipped now and resolved by the next sync, and a
    /// pin the registry refuses (a bad signature, a mismatched revision, an invalid payload) is
    /// refused for good. A micro that was installed from a cartridge and is no longer enabled or
    /// installed is dropped: its follower stops and its handles are retracted.
    ///
    /// It is idempotent: a micro whose pinned revision is unchanged keeps its follower and its
    /// handles. Call it again when installs change (`observeCartridgeInstalls`); grants are
    /// followed by each micro's own follower.
    pub async fn sync_cartridge_micros(&mut self) -> Res<Vec<Res<CartridgeMicro>>> {
        let installs = self
            .minting
            .nodules
            .call_with(
                RequestContext::root(),
                self.minting.operator().metadata(),
                ListCartridgeInstalls {},
            )
            .await
            .wrap_err("listing the installed cartridges")?
            .installs;
        let wanted: Vec<CartridgeInstallRecord> = installs
            .into_iter()
            .filter(|record| {
                record
                    .install
                    .micro
                    .as_ref()
                    .is_some_and(|micro| micro.enabled)
            })
            .collect();
        let wanted_ids: BTreeSet<String> = wanted
            .iter()
            .map(|record| record.cartridge_id.to_string())
            .collect();
        let dropped: Vec<String> = self
            .installed
            .iter()
            .filter(|(micro, installed)| {
                installed.follower.is_some() && !wanted_ids.contains(*micro)
            })
            .map(|(micro, _)| micro.clone())
            .collect();
        for micro in dropped {
            let installed = self
                .installed
                .remove(&micro)
                .expect("the micro was listed from the table");
            installed.release(&self.minting).await?;
        }
        let mut loaded = Vec::with_capacity(wanted.len());
        for record in wanted {
            let micro = record.cartridge_id.to_string();
            loaded.push(
                self.sync_one(record)
                    .await
                    .wrap_err_with(|| format!("loading the micro of cartridge {micro}")),
            );
        }
        Ok(loaded)
    }

    async fn sync_one(&mut self, record: CartridgeInstallRecord) -> Res<CartridgeMicro> {
        let minting = self.minting.clone();
        let CartridgeInstallRecord {
            cartridge_id,
            install,
        } = record;
        let resolved = minting
            .cartridges
            .call_with(
                RequestContext::root(),
                minting.operator().metadata(),
                ResolveCartridgePin {
                    pin: install.cartridge.clone(),
                },
            )
            .await
            .map_err(pin_error)?;
        if resolved.cartridge.id != cartridge_id {
            return Err(ferr!(
                "the install is of `{cartridge_id}` but its pin names the envelope of `{}`",
                resolved.cartridge.id
            ));
        }
        let cartridge = &resolved.cartridge;
        let contribution = cartridge
            .micro()
            .expect("an install has a micro member exactly when its envelope contributes one");
        if contribution.schema != CLAY_MICRO_MANIFEST_SCHEMA {
            return Err(ferr!(
                "the micro's manifest is `{}`, and this host loads only `{CLAY_MICRO_MANIFEST_SCHEMA}`",
                contribution.schema
            ));
        }
        let component = resolved
            .components
            .get(&contribution.component)
            .expect("the registry resolves every component the envelope names");
        let Component::Wasm(wasm) = component else {
            return Err(ferr!("the micro's component is not a wasm component"));
        };
        let component = match self
            .booted
            .node()
            .blobs
            .service
            .read_all(&BlobId::new(wasm.digest.0), MAX_COMPONENT_OCTETS)
            .await?
        {
            Some(bytes) => bytes,
            None => {
                return Err(ferr!(
                    "the micro's component is not in this node's blob store yet; the next sync retries"
                ));
            }
        };
        assert_eq!(
            blake3::hash(&component).as_bytes(),
            &wasm.digest.0,
            "the blob store returned bytes that do not hash to their id"
        );
        let manifest_json = sidecar_json(cartridge, contribution)?;

        let micro = cartridge_id.to_string();
        let revision = cartridge.revision();
        let held = self.installed.remove(&micro);
        let installed = match held {
            Some(held) if held.revision == Some(revision) => held,
            held => {
                // The follower holds the envelope it lowers `uses` from, so a new revision
                // restarts it. The egress the micro's incarnation holds stays, and so does the
                // state its handles are diffed against.
                let (egress, state, mut minted) = match held {
                    Some(Installed {
                        egress,
                        state,
                        follower,
                        ..
                    }) => {
                        let minted = match follower {
                            Some(follower) => follower.stop().await,
                            None => None,
                        };
                        (egress, state, minted)
                    }
                    None => (
                        Arc::new(
                            crate::MicroEgress::install(
                                minting.dispatch.clone(),
                                minting.operator(),
                                &crate::open_micro_actor(&minting, &micro).await?,
                                Default::default(),
                            )
                            .await
                            .wrap_err_with(|| format!("minting the capability of micro {micro}"))?,
                        ),
                        Arc::new(MintedState::new(None)),
                        None,
                    ),
                };
                let first = await_grants(&minting, &cartridge_id, None, 0).await?;
                let following = Following {
                    micro: micro.clone(),
                    cartridge_id: cartridge_id.clone(),
                    resolved: Arc::new(resolved),
                    config_doc: install.config_doc.clone(),
                    egress: Arc::clone(&egress),
                    state: Arc::clone(&state),
                };
                apply_grants(&minting, &following, &mut minted, &first).await?;
                let (stop, spawner) = StopToken::new();
                // The follower's tasks run on the node's runtime, not on the caller's.
                let _runtime = self.runtime.enter();
                let task = spawner
                    .spawn(follow_grants(
                        minting.clone(),
                        spawner.clone(),
                        following,
                        minted,
                        first.revision,
                    ))
                    .expect("a fresh spawner is open");
                Installed {
                    egress,
                    config: None,
                    state,
                    follower: Some(Follower { stop, task }),
                    revision: Some(revision),
                }
            }
        };
        self.installed.insert(micro.clone(), installed);
        Ok(CartridgeMicro {
            micro,
            manifest_json,
            component,
        })
    }
}

/// What a micro's follower lowers grants against and re-mints into.
struct Following {
    micro: String,
    cartridge_id: CartridgeId,
    resolved: Arc<ResolvedCartridge>,
    config_doc: big_cabinet_core::dmeta::DocId,
    egress: Arc<crate::MicroEgress>,
    state: Arc<MintedState>,
}

/// A micro's grants follower: the task, and the token that stops it. It owns the micro's
/// [`Minted`] handles for as long as it runs and hands them back when it stops, so exactly one
/// task re-mints them at a time.
pub(crate) struct Follower {
    stop: StopToken,
    task: JoinHandle<Option<Minted>>,
}

impl Follower {
    /// Cancels the follower, waits for it, and answers what it held.
    pub(crate) async fn stop(self) -> Option<Minted> {
        self.stop.stop().await;
        self.task.await.expect("a stopped follower task joins")
    }
}

/// Long-polls the micro's cartridge's grants and applies each new revision, until the micro is
/// dropped (the spawner is cancelled) or its revision is replaced, and then answers the handles it
/// holds. A failure here is a broken invariant of the node the composition holds (the node stops
/// only after every follower), so it panics, and the stop that joins this task panics with it.
async fn follow_grants(
    minting: Minting,
    spawner: Spawner,
    following: Following,
    mut minted: Option<Minted>,
    mut revision: String,
) -> Option<Minted> {
    loop {
        let answer = tokio::select! {
            () = spawner.cancelled() => return minted,
            answer = await_grants(
                &minting,
                &following.cartridge_id,
                Some(revision.clone()),
                AWAIT_MAX_HOLD_MS,
            ) => answer.expect("following a cartridge's grants"),
        };
        if answer.revision == revision {
            continue;
        }
        apply_grants(&minting, &following, &mut minted, &answer)
            .await
            .expect("re-minting a micro's handles for its moved grants");
        revision = answer.revision;
    }
}

async fn await_grants(
    minting: &Minting,
    cartridge_id: &CartridgeId,
    since: Option<String>,
    hold_ms: u32,
) -> Res<CartridgeGrantsAt> {
    minting
        .nodules
        .call_with(
            RequestContext::root(),
            minting.operator().metadata(),
            AwaitCartridgeGrants {
                cartridge_id: cartridge_id.clone(),
                since,
                hold_ms,
            },
        )
        .await
        .wrap_err("awaiting the cartridge's grants")
}

/// Lowers the micro's `uses` against `grants` and brings its handles to the result.
async fn apply_grants(
    minting: &Minting,
    following: &Following,
    minted: &mut Option<Minted>,
    grants: &CartridgeGrantsAt,
) -> Res<()> {
    use big_nodule_core::NoduleAdmission;
    let resolved = &following.resolved;
    let contribution = resolved
        .cartridge
        .micro()
        .expect("a micro's follower is started for an envelope that contributes one");
    let grants = big_nodule_core::config::ResolvedGrants::from_grants(
        grants
            .grants
            .iter()
            .map(|record| (&record.grant, &record.value)),
    )
    .map_err(|reason| ferr!("the cartridge's grants do not resolve: {reason}"))?;
    // The grants that cover a method permbind of the cartridge, as the nodule binds them: an
    // authority-prefix grant never reaches beyond the operations the manifest names.
    let (effective, _unrequested) = resolved.bind_operations(&grants.operations);
    let scopes = big_nodule::RoutineScopes::lower(
        resolved,
        &following.config_doc,
        &contribution.uses,
        &grants.drawers,
        &effective,
    );
    mint_handles(
        minting,
        &following.micro,
        &following.egress,
        &following.state,
        minted,
        scopes,
    )
    .await
}

/// The error of a pin that did not resolve, saying whether the next sync retries it, as the
/// nodule's own install read does (`big_nodule::cartridge_docs`).
fn pin_error(error: ClientError<CartridgesError>) -> utils_rs::prelude::eyre::Report {
    match error {
        ClientError::Method(
            error @ (CartridgesError::UnknownCartridge(_)
            | CartridgesError::UnknownVersion { .. }
            | CartridgesError::PinnedEnvelopeUnreadable { .. }
            | CartridgesError::PinnedDatumUnreadable { .. }),
        ) => {
            ferr!("the pinned version is not available yet, and the next sync retries it: {error}")
        }
        ClientError::Method(
            error @ (CartridgesError::Rejected(_)
            | CartridgesError::Signature(_)
            | CartridgesError::PinnedDatumInvalid { .. }
            | CartridgesError::PinnedDocumentMismatch { .. }
            | CartridgesError::PinnedRevisionMismatch { .. }),
        ) => ferr!("the registry refuses the pinned version: {error}"),
        error => ferr!("resolving the pinned version failed: {error}"),
    }
}

/// The standalone `<name>.manifest.json` sidecar of a package: Clay's manifest, which the
/// contribution carries as its `description`, and the identity the envelope owns, which the
/// importer split off (`big_cartridges::import`): `id` is the cartridge's name, `package_version`
/// its major version and `authority` the envelope's.
fn sidecar_json(
    cartridge: &big_cartridges_core::Cartridge,
    contribution: &big_cartridges_core::MicroContribution,
) -> Res<Vec<u8>> {
    let mut sidecar: serde_json::Map<String, Value> = contribution
        .description
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let package_version = u32::try_from(cartridge.version.0.major)
        .map_err(|_| ferr!("the version {} has no u32 major", cartridge.version))?;
    sidecar.insert("id".to_owned(), Value::String(cartridge.id.name.clone()));
    sidecar.insert("package_version".to_owned(), package_version.into());
    if let Some(authority) = &cartridge.authority {
        sidecar.insert("authority".to_owned(), Value::String(authority.clone()));
    }
    Ok(serde_json::to_vec(&Value::Object(sidecar)).expect("a JSON object serializes"))
}

impl Installed {
    /// Stops this micro's follower, then retracts every handle minted for it.
    pub(crate) async fn release(self, minting: &Minting) -> Res<()> {
        let Some(follower) = self.follower else {
            return Ok(());
        };
        if let Some(minted) = follower.stop().await {
            let handles = &minted.handles;
            for handle in [
                handles.dispatch,
                handles.docs,
                handles.drawers,
                handles.blobs,
            ]
            .into_iter()
            .flatten()
            {
                retract(minting, handle).await?;
            }
        }
        Ok(())
    }
}
