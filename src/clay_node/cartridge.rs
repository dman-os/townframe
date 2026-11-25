//! Micros that cartridges contribute (Clay on the node section 5, ADR 049 sections 3 and 6, ADR 052).
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
//! 3. the micro's handles are the node's: the `micros` nodule of the core cartridge opens the
//!    micro's actor and mints dispatch, docs, drawers and blobs handles from its `uses`
//!    intersected with the cartridge's grants, and `micros.handles` answers them. The host mints
//!    nothing. It presents the dispatch handle and signs with the actor capability on the micro's
//!    egress, and the guest names only operations.
//!
//! Mounting never waits on a poll: `micros.handles` answers at once, and the host mounts the micro
//! with what it returned. One task per composition, [`HandlesFollower`], long-polls
//! `micros.awaitHandles` for every cartridge the host has mounted and binds each re-minted
//! answer to the micro's egress, so the micro's next call presents the new handle. A micro that
//! loses a handle sees a retryable not-ready answer between the node's retraction and re-mint
//! (ADR 049 section 6.4), never a stale handle. A micro with no granted `uses` holds a dispatch
//! handle that reaches nothing and can call nothing.

use crate::Installed;
use crate::MicroEgress;
use crate::MintedState;
use crate::Minting;
use crate::NodeComposition;
use big_blobs_core::BlobId;
use big_cartridges_core::CLAY_MICRO_MANIFEST_SCHEMA;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Component;
use big_cartridges_core::protocol::AWAIT_MAX_HOLD_MS;
use big_cartridges_core::protocol::CartridgeInstallRecord;
use big_cartridges_core::protocol::CartridgesError;
use big_cartridges_core::protocol::ListCartridgeInstalls;
use big_cartridges_core::protocol::ResolveCartridgePin;
use big_dispatch_core::Invoke;
use method_core::Method;
use micros_nodule::AwaitHandles;
use micros_nodule::CartridgeHandles;
use micros_nodule::Handles;
use micros_nodule::MicroHandles;
use micros_nodule::Moved;
use serde_json::Value;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use service_utils_rs::Spawner;
use service_utils_rs::StopToken;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::watch;
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
    /// installed is dropped: it stops being followed and its egress goes, so its guest holds no way
    /// out. The node retracts its handles when it next re-mints for the cartridge.
    ///
    /// It is idempotent: a micro whose pinned revision is unchanged keeps its egress and its
    /// handles. Call it again when installs change (`observeCartridgeInstalls`); grants are
    /// followed by the composition's [`HandlesFollower`].
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
                installed.revision.is_some() && !wanted_ids.contains(*micro)
            })
            .map(|(micro, _)| micro.clone())
            .collect();
        for micro in dropped {
            self.installed
                .remove(&micro)
                .expect("the micro was listed from the table");
            self.follower.unwatch(&micro);
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
        let installed = match self.installed.remove(&micro) {
            Some(held) if held.revision == Some(revision) => held,
            held => {
                // Asked for at once, never held: the micro is mounted with these and the
                // follower brings them up to date.
                let answer = handles(&minting, &cartridge_id).await?;
                let handles = only_micro(&answer.micros, &cartridge_id);
                let (egress, state) = match held {
                    // The egress the micro's incarnation holds stays, and so does the state its
                    // handles are announced on; the new handles are bound to them.
                    Some(Installed { egress, state, .. }) => (egress, state),
                    None => (
                        Arc::new(MicroEgress::attached(
                            minting.dispatch.clone(),
                            handles.actor,
                            Some(handles.handles.dispatch),
                        )),
                        Arc::new(MintedState::new(handles.handles.docs)),
                    ),
                };
                let watched = Watched {
                    revision: answer.revision.clone(),
                    egress: Arc::clone(&egress),
                    state: Arc::clone(&state),
                };
                watched.bind(Some(handles), true);
                self.follower.watch(micro.clone(), watched);
                Installed {
                    egress,
                    config: None,
                    state,
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

/// The one micro a cartridge contributes. The envelope carries one unnamed micro (ADR 050
/// section 1's several micros per cartridge are not built), and the nodule answers a micro for
/// every enabled cartridge that ships one, which `sync_one` has just checked this one does.
fn only_micro<'a>(micros: &'a [MicroHandles], cartridge: &CartridgeId) -> &'a MicroHandles {
    let mut micros = micros.iter();
    let micro = micros
        .next()
        .unwrap_or_else(|| panic!("the micros nodule answered no micro for `{cartridge}`"));
    assert!(
        micros.next().is_none(),
        "the micros nodule answered several micros for `{cartridge}`, which ships one"
    );
    micro
}

/// A micro the host has mounted, as the follower binds new handles to it.
#[derive(Clone)]
struct Watched {
    /// The handles revision the egress was last bound at, as `micros.handles` names it.
    revision: String,
    egress: Arc<MicroEgress>,
    state: Arc<MintedState>,
}

impl Watched {
    /// Binds the micro's egress and document capability to `micro`'s handles, or takes them away
    /// when the node answers none (the cartridge's micro was disabled). With `announce`, the
    /// handles are reported changed to whoever follows [`NodeComposition::handles_changed`].
    fn bind(&self, micro: Option<&MicroHandles>, announce: bool) {
        match micro {
            Some(micro) => {
                self.egress
                    .rebind(micro.actor, Some(micro.handles.dispatch));
                *self
                    .state
                    .doc_capability
                    .lock()
                    .expect("doc capability lock") = micro.handles.docs;
            }
            None => {
                self.egress.unbind();
                *self
                    .state
                    .doc_capability
                    .lock()
                    .expect("doc capability lock") = None;
            }
        }
        if announce {
            self.state
                .generation
                .send_modify(|generation| *generation += 1);
        }
    }
}

/// The composition's one follower of the `micros` nodule: a task that long-polls
/// `micros.awaitHandles` for every micro the host has mounted and binds what it answers (ADR 052
/// section 3). A host with several cartridges polls once.
pub(crate) struct HandlesFollower {
    watched: watch::Sender<BTreeMap<String, Watched>>,
    stop: StopToken,
}

impl HandlesFollower {
    /// Starts the task on the current runtime, the node's.
    pub(crate) fn start(minting: Minting) -> Self {
        let (stop, spawner) = StopToken::new();
        let (watched, changes) = watch::channel(BTreeMap::new());
        spawner
            .spawn(follow_handles(
                minting,
                spawner.clone(),
                watched.clone(),
                changes,
            ))
            .expect("a fresh spawner is open");
        Self { watched, stop }
    }

    /// Follows `watched` from now on, replacing an earlier one of the same micro.
    fn watch(&self, micro: String, watched: Watched) {
        self.watched.send_modify(|all| {
            all.insert(micro, watched);
        });
    }

    fn unwatch(&self, micro: &str) {
        self.watched.send_modify(|all| {
            all.remove(micro);
        });
    }

    /// Cancels the task and waits for it. The composition stops it before the node, because it
    /// calls the node's services.
    pub(crate) async fn stop(self) {
        self.stop.stop().await;
    }
}

/// Long-polls the handles of the watched micros and binds each answer, until cancelled. The poll
/// restarts whenever the watched set changes, so a micro mounted meanwhile is in the next one. A
/// failure here is a broken invariant of the node the composition holds (the node stops only
/// after the follower), so it panics, and the stop that joins this task panics with it.
async fn follow_handles(
    minting: Minting,
    spawner: Spawner,
    watched: watch::Sender<BTreeMap<String, Watched>>,
    mut changes: watch::Receiver<BTreeMap<String, Watched>>,
) {
    const SENDER_LIVES: &str = "the follower's handle holds the sender until it stops the task";
    loop {
        let since: BTreeMap<String, String> = changes
            .borrow_and_update()
            .iter()
            .map(|(micro, watched)| (micro.clone(), watched.revision.clone()))
            .collect();
        if since.is_empty() {
            tokio::select! {
                () = spawner.cancelled() => return,
                changed = changes.changed() => changed.expect(SENDER_LIVES),
            }
            continue;
        }
        let moved = tokio::select! {
            () = spawner.cancelled() => return,
            changed = changes.changed() => {
                changed.expect(SENDER_LIVES);
                continue;
            }
            moved = await_handles(&minting, since) => {
                moved.expect("following the handles of the cartridges' micros")
            }
        };
        watched.send_modify(|all| {
            for (cartridge, handles) in moved.cartridges {
                // A micro dropped while the poll was out is no longer followed.
                let Some(watched) = all.get_mut(&cartridge) else {
                    continue;
                };
                watched.revision = handles.revision;
                watched.bind(handles.micros.first(), true);
            }
        });
    }
}

/// Calls a method of the `micros` nodule through dispatch as the node's operator, which is who it
/// answers (ADR 052 section 3): dispatch routes its NSID to the core cartridge's nodule like any
/// nodule method.
async fn call_micros<M: Method>(minting: &Minting, request: &M) -> Res<Value> {
    let nsid = M::XRPC.nsid;
    let output = minting
        .dispatch
        .call_with(
            RequestContext::root(),
            minting.operator().metadata(),
            Invoke::json(
                nsid,
                serde_json::to_value(request).expect("a method's request is JSON"),
                Default::default(),
            ),
        )
        .await
        .wrap_err_with(|| format!("calling {nsid}"))?;
    Ok(output.body.into_json())
}

/// `micros.handles` of `cartridge`: its micros' live handles, answered at once.
async fn handles(minting: &Minting, cartridge: &CartridgeId) -> Res<CartridgeHandles> {
    let answer = call_micros(
        minting,
        &Handles {
            cartridge_id: cartridge.to_string(),
            micro: None,
        },
    )
    .await?;
    Ok(serde_json::from_value(answer).expect("the micros nodule answers the shape it declares"))
}

/// `micros.awaitHandles` since `since` (the revision each watched cartridge's handles were last
/// bound at): the cartridges whose handles moved, held up to the nodule's longest hold.
async fn await_handles(minting: &Minting, since: BTreeMap<String, String>) -> Res<Moved> {
    let answer = call_micros(
        minting,
        &AwaitHandles {
            since,
            hold_ms: AWAIT_MAX_HOLD_MS,
        },
    )
    .await?;
    Ok(serde_json::from_value(answer).expect("the micros nodule answers the shape it declares"))
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
