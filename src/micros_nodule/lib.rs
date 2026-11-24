//! The micros nodule of the core cartridge (ADR 052): the node's management of micros, written as
//! a nodule and not as native node code. It acts as the `micros` subsystem actor and, for each
//! enabled micro of each installed cartridge,
//!
//! 1. opens the micro's actor, `micros/<cartridge>/<micro>` (one path segment per open, each id
//!    segmented as [`big_nodule_core::actor_segment`] segments a cartridge id, which is how the
//!    node derives the `nodules` actors' paths);
//! 2. lowers the micro's `uses` against the cartridge's grants with
//!    [`big_nodule_core::inventory::RoutineScopes::lower`], the code the nodule service lowers a
//!    routine's `uses` with, so a `uses` entry confers the same thing in both runtimes; and
//! 3. mints the micro's handles with that actor as subject, each from a handle this nodule holds
//!    (ADR 049 section 6.1, "a runtime grants itself first, then its children"):
//!    dispatch and blobs from the handles the node delivers at the `micros` delegators' scope
//!    (`x-nodule-capability` and `x-nodule-blobs-capability`), docs and drawers from this
//!    nodule's own actor identity capability (`x-nodule-actor-capability`), whose standing on
//!    every drawer granted to a cartridge that ships a micro the node maintains. Each parent is
//!    named in the mint's body (`capability`), because what dispatch forwards as the credential
//!    is the nodule's own dispatch handle, which no service's domain reads.
//!
//! The nodule is a request handler: a fresh instance serves each call, so the state that spans
//! calls is the nodule's own settings datum in its config document
//! ([`MINTED_TAG`], one per cartridge, key [`cartridge_slot`]). It holds the handles minted for
//! the cartridge and the revision they were minted at. Handles live for the node's boot (the
//! authority's ledger starts empty at every boot), so the datum also names the boot it was
//! written in, by this nodule's actor capability, and one from another boot is not read.
//!
//! The handlers have no wasm-only code, so the crate builds for both targets: `micros_nodule_guest`
//! exports [`Micros`] as the component and the native tests serve it too.

use big_blobs_core::MintBlobsCapability;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::OpenedActor;
use big_cabinet_core::authority::ResolveAuthority;
use big_cabinet_core::authority::RetractCapability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::protocol::AWAIT_MAX_HOLD_MS;
use big_cartridges_core::protocol::AwaitCartridgeGrants;
use big_cartridges_core::protocol::CartridgeGrantsAt;
use big_cartridges_core::protocol::CartridgeInstallRecord;
use big_cartridges_core::protocol::ListCartridgeInstalls;
use big_cartridges_core::protocol::ResolveCartridgePin;
use big_dispatch_core::MintDispatchCapability;
use big_ndk::Cx;
use big_ndk::Handler;
use big_nodule_core::NoduleAdmission as _;
use big_nodule_core::actor_segment;
use big_nodule_core::config::ResolvedGrants;
use big_nodule_core::inventory::RoutineScopes;
use method_core::ContextError;
use method_core::HandleNeed;
use method_core::Method as _;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use service_core_rs::ClientError;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// The settings datum kind this nodule keeps its minted handles in, one datum per cartridge.
pub const MINTED_TAG: &str = "org.example.daybook.micros.minted";

/// The name of a cartridge's one micro. The envelope contributes one unnamed micro whose identity
/// is the cartridge's (ADR 050 section 1's several micros per cartridge are not built), so it is
/// named here, in one place: its id is `<cartridge id>/micro/main`.
pub const MICRO_NAME: &str = "main";

/// The id of `cartridge`'s micro, ADR 050 section 1's `<cartridge id>/micro/<name>`.
pub fn micro_id(cartridge: &CartridgeId) -> String {
    format!("{cartridge}/micro/{MICRO_NAME}")
}

/// The slot of a cartridge's [`MINTED_TAG`] datum: its actor segment, which is safe in a datum
/// key where a cartridge id (`/`, `:`) is not.
pub fn cartridge_slot(cartridge: &CartridgeId) -> String {
    actor_segment(&cartridge.name, &cartridge.to_string())
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MicrosError {
    Context(ContextError),
    /// The caller is not the operator.
    Denied,
    /// No such cartridge is installed.
    UnknownCartridge(String),
    /// A call this nodule makes to the node failed.
    Upstream(String),
}

impl std::fmt::Display for MicrosError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Context(error) => error.fmt(f),
            Self::Denied => f.write_str("only the operator may manage micros"),
            Self::UnknownCartridge(id) => write!(f, "cartridge `{id}` is not installed"),
            Self::Upstream(message) => write!(f, "the node refused a call: {message}"),
        }
    }
}

impl From<ContextError> for MicrosError {
    fn from(error: ContextError) -> Self {
        Self::Context(error)
    }
}

/// What a micro is minted: the handles its host mounts it with, `dispatch` always (a micro that
/// is granted nothing holds one that reaches nothing).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MicroHandles {
    /// The micro's id, `<cartridge id>/micro/<name>`.
    pub micro: String,
    /// The micro's actor identity capability, with `USE` only: what the host signs as.
    pub actor: CapId,
    pub handles: MintedHandles,
    /// The record the host puts in the micro's configuration slot: what the node knows of it
    /// (today, the document capability under `docCap`).
    pub config: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintedHandles {
    pub dispatch: CapId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docs: Option<CapId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blobs: Option<CapId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drawers: Option<CapId>,
}

/// A cartridge's micros as minted, and the revision they were minted at: opaque, and naming the
/// boot it was taken in.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeHandles {
    pub micros: Vec<MicroHandles>,
    pub revision: String,
}

/// `{ cartridgeId, micro? }`: the live handles of the cartridge's micros (or the one named),
/// minted the first time and never again while they are live.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Handles {
    pub cartridge_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub micro: Option<String>,
}

/// `{ since: { <cartridgeId>: revision }, holdMs }`: holds up to `holdMs` (at most
/// [`AWAIT_MAX_HOLD_MS`]) for a watched cartridge's grants to move, re-mints the micros of the
/// ones that did, and answers those cartridges' handles. A `since` that is not current answers
/// at once.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AwaitHandles {
    pub since: BTreeMap<String, String>,
    pub hold_ms: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Moved {
    pub cartridges: BTreeMap<String, CartridgeHandles>,
}

method_core::methods! {
    error = MicrosError, nsid_prefix = "org.example.daybook.micros.";
    Handles => CartridgeHandles, procedure, "handles";
    AwaitHandles => Moved, procedure, "awaitHandles";
}

/// The nodule: a fresh value serves each request.
#[derive(Default, Clone)]
pub struct Micros;

/// What a minted cartridge is kept as in [`MINTED_TAG`]'s datum.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Minted {
    /// The grants revision the handles were minted at, as `awaitCartridgeGrants` names it.
    grants: String,
    /// The scopes the handles were minted for, per micro id, so a grant change that moves no
    /// scope keeps the handles.
    scopes: BTreeMap<String, Value>,
    micros: Vec<MicroHandles>,
}

impl Minted {
    fn answer(&self, boot: &str, only: Option<&str>) -> CartridgeHandles {
        CartridgeHandles {
            micros: self
                .micros
                .iter()
                .filter(|micro| only.is_none_or(|only| micro.micro == only))
                .cloned()
                .collect(),
            revision: format!("{boot}.{}", self.grants),
        }
    }
}

fn upstream<E: std::fmt::Debug>(what: &'static str) -> impl FnOnce(E) -> MicrosError {
    move |error| MicrosError::Upstream(format!("{what}: {error:?}"))
}

/// The service handle a built-in operation needs beyond dispatch's admission. The host catalogue
/// (`method_lexicon`) is native-only, so the two blobs operations that need one are named here.
fn handle_need(nsid: &str) -> Option<HandleNeed> {
    [
        big_blobs_core::PutBlob::XRPC,
        big_blobs_core::ImportFile::XRPC,
    ]
    .into_iter()
    .find(|method| method.nsid == nsid)
    .and_then(|method| method.handle)
}

fn cap_id(text: &str) -> Result<CapId, MicrosError> {
    serde_json::from_value(Value::String(text.to_owned())).map_err(upstream("a capability id"))
}

/// The capabilities this nodule mints from, as the node delivered them.
struct Parents {
    /// Its own actor identity capability.
    actor: CapId,
    /// Its dispatch handle, at the `micros` delegator's scope.
    dispatch: CapId,
    /// Its blobs handle, at the `micros` delegator's scope.
    blobs: Option<CapId>,
}

impl Parents {
    fn of(cx: &Cx) -> Result<Self, MicrosError> {
        let missing = || MicrosError::Context(ContextError::Unauthenticated);
        Ok(Self {
            actor: cap_id(cx.actor_capability().ok_or_else(missing)?)?,
            dispatch: cap_id(cx.nodule_capability().ok_or_else(missing)?)?,
            blobs: cx.blobs_capability().map(cap_id).transpose()?,
        })
    }
}

impl Micros {
    /// Refuses a caller that is not the operator: the nodule recognizes it by the resolved root
    /// capability and by nothing it was configured with.
    async fn require_operator(cx: &Cx) -> Result<(), MicrosError> {
        let caller = cx
            .caller_capability()
            .ok_or(MicrosError::Context(ContextError::Unauthenticated))?;
        let resolved = cx
            .call(ResolveAuthority {
                capability: cap_id(caller)?,
            })
            .await
            .map_err(upstream("resolving the caller"))?;
        if resolved.is_operator_root() {
            Ok(())
        } else {
            Err(MicrosError::Denied)
        }
    }

    async fn installed(cx: &Cx, id: &str) -> Result<CartridgeInstallRecord, MicrosError> {
        cx.call(ListCartridgeInstalls {})
            .await
            .map_err(upstream("listing installs"))?
            .installs
            .into_iter()
            .find(|record| record.cartridge_id.to_string() == id)
            .ok_or_else(|| MicrosError::UnknownCartridge(id.to_owned()))
    }

    fn key(cartridge: &CartridgeId) -> String {
        format!("{MINTED_TAG}/{}", cartridge_slot(cartridge))
    }

    /// The state kept for `cartridge` in this boot, if any.
    async fn stored(
        cx: &Cx,
        boot: &str,
        cartridge: &CartridgeId,
    ) -> Result<Option<Minted>, MicrosError> {
        let read = cx
            .read_config(None)
            .await
            .map_err(upstream("reading the minted state"))?;
        let datum = &read["datums"][Self::key(cartridge)];
        let Some(value) = datum["value"].as_object() else {
            return Ok(None);
        };
        if value.get("boot").and_then(Value::as_str) != Some(boot) {
            return Ok(None);
        }
        serde_json::from_value(value["body"].clone())
            .map(Some)
            .map_err(upstream("decoding the minted state"))
    }

    async fn store(
        cx: &Cx,
        boot: &str,
        cartridge: &CartridgeId,
        minted: &Minted,
    ) -> Result<(), MicrosError> {
        cx.write_config(
            BTreeMap::from([(
                Self::key(cartridge),
                json!({
                    "$type": MINTED_TAG,
                    "boot": boot,
                    "body": serde_json::to_value(minted).expect("minted state serializes"),
                }),
            )]),
            BTreeSet::new(),
        )
        .await
        .map_err(upstream("writing the minted state"))?;
        Ok(())
    }

    async fn retract(cx: &Cx, via: CapId, capability: CapId) -> Result<(), MicrosError> {
        match cx.call(RetractCapability { via, capability }).await {
            Ok(()) => Ok(()),
            // A handle that is already dead is retracted.
            Err(ClientError::Method(AuthorityServiceError::InvalidCapability)) => Ok(()),
            Err(error) => Err(upstream("retracting a handle")(error)),
        }
    }

    /// Brings `record`'s micro to the handles its `uses` and `grants` give, in ADR 049 section
    /// 6.4's order: the old handles are retracted, then new ones minted. `held` is what an
    /// earlier mint left. A micro whose scopes did not move keeps its handles.
    async fn mint(
        cx: &Cx,
        parents: &Parents,
        record: &CartridgeInstallRecord,
        grants: &CartridgeGrantsAt,
        held: Option<Minted>,
    ) -> Result<Minted, MicrosError> {
        let install = &record.install;
        let envelope = cx
            .call(ResolveCartridgePin {
                pin: install.cartridge.clone(),
            })
            .await
            .map_err(upstream("resolving the install's envelope"))?;
        let mut minted = Minted {
            grants: grants.revision.clone(),
            scopes: BTreeMap::new(),
            micros: Vec::new(),
        };
        let micro = envelope
            .cartridge
            .micro()
            .filter(|_| install.micro.as_ref().is_some_and(|micro| micro.enabled));
        let Some(micro) = micro else {
            Self::retract_all(cx, parents, held).await?;
            return Ok(minted);
        };
        let granted = ResolvedGrants::from_grants(
            grants
                .grants
                .iter()
                .map(|record| (&record.grant, &record.value)),
        )
        .map_err(|reason| MicrosError::Upstream(format!("the cartridge's grants: {reason}")))?;
        // The grants that cover a method permbind of the cartridge, as the nodule binds them: an
        // authority-prefix grant never reaches beyond the operations the manifest names.
        let (effective, _unrequested) = envelope.bind_operations(&granted.operations);
        let scopes = RoutineScopes::lower(
            &envelope,
            &install.config_doc,
            &micro.uses,
            &granted.drawers,
            &effective,
            &handle_need,
        );
        let id = micro_id(&record.cartridge_id);
        let scopes_json = serde_json::to_value(&scopes_wire(&scopes)).expect("scopes serialize");
        if let Some(held) = &held
            && held.scopes.get(&id) == Some(&scopes_json)
            && let Some(kept) = held.micros.iter().find(|held| held.micro == id)
        {
            minted.scopes.insert(id, scopes_json);
            minted.micros.push(kept.clone());
            return Ok(minted);
        }
        let actor = Self::open_micro_actor(cx, parents, &record.cartridge_id).await?;
        Self::retract_all(cx, parents, held).await?;
        let subject = Some(actor.actor.actor_id);
        let dispatch = cx
            .call(MintDispatchCapability {
                capability: Some(parents.dispatch),
                scope: scopes.dispatch.clone().unwrap_or_default(),
                rights: Rights::USE,
                subject,
            })
            .await
            .map_err(upstream("minting the micro's dispatch handle"))?
            .capability_id;
        let docs = match scopes.docs.clone() {
            Some(scope) => Some(
                cx.call(MintDocCapability {
                    capability: Some(parents.actor),
                    scope,
                    rights: Rights::USE,
                    subject,
                })
                .await
                .map_err(upstream("minting the micro's docs handle"))?
                .capability_id,
            ),
            None => None,
        };
        let drawers = match scopes.drawers.clone() {
            Some(scope) => Some(
                cx.call(MintDrawerCapability {
                    capability: Some(parents.actor),
                    scope,
                    rights: Rights::USE,
                    subject,
                })
                .await
                .map_err(upstream("minting the micro's drawers handle"))?
                .capability_id,
            ),
            None => None,
        };
        let blobs = match (scopes.blobs.clone(), parents.blobs) {
            (Some(scope), Some(parent)) => Some(
                cx.call(MintBlobsCapability {
                    capability: Some(parent),
                    scope,
                    rights: Rights::USE,
                    subject,
                })
                .await
                .map_err(upstream("minting the micro's blobs handle"))?
                .capability_id,
            ),
            _ => None,
        };
        let config = match docs {
            Some(docs) => json!({ "docCap": docs }),
            None => json!({}),
        };
        minted.scopes.insert(id.clone(), scopes_json);
        minted.micros.push(MicroHandles {
            micro: id,
            actor: actor.capability,
            handles: MintedHandles {
                dispatch,
                docs,
                blobs,
                drawers,
            },
            config,
        });
        Ok(minted)
    }

    /// Opens `micros/<cartridge>` then `micros/<cartridge>/<micro>`, one segment per open (an
    /// existing path opens to the same actor), and answers the micro's actor.
    async fn open_micro_actor(
        cx: &Cx,
        parents: &Parents,
        cartridge: &CartridgeId,
    ) -> Result<OpenedActor, MicrosError> {
        let segment = cartridge_slot(cartridge);
        let of_cartridge = cx
            .call(OpenActor {
                parent: parents.actor,
                path: format!("micros/{segment}"),
                rights: Rights::USE | Rights::MINT_CHILD,
            })
            .await
            .map_err(upstream("opening the cartridge's actor"))?;
        let micro = actor_segment(MICRO_NAME, &micro_id(cartridge));
        cx.call(OpenActor {
            parent: of_cartridge.capability,
            path: format!("micros/{segment}/{micro}"),
            rights: Rights::USE,
        })
        .await
        .map_err(upstream("opening the micro's actor"))
    }

    /// Retracts what `held` minted: each handle from the parent it was minted from.
    async fn retract_all(
        cx: &Cx,
        parents: &Parents,
        held: Option<Minted>,
    ) -> Result<(), MicrosError> {
        for micro in held.into_iter().flat_map(|held| held.micros) {
            let MintedHandles {
                dispatch,
                docs,
                blobs,
                drawers,
            } = micro.handles;
            Self::retract(cx, parents.dispatch, dispatch).await?;
            for handle in [docs, drawers].into_iter().flatten() {
                Self::retract(cx, parents.actor, handle).await?;
            }
            if let (Some(handle), Some(parent)) = (blobs, parents.blobs) {
                Self::retract(cx, parent, handle).await?;
            }
        }
        Ok(())
    }

    async fn grants_at(
        cx: &Cx,
        cartridge: &CartridgeId,
        since: Option<String>,
        hold_ms: u32,
    ) -> Result<CartridgeGrantsAt, MicrosError> {
        cx.call(AwaitCartridgeGrants {
            cartridge_id: cartridge.clone(),
            since,
            hold_ms,
        })
        .await
        .map_err(upstream("awaiting the cartridge's grants"))
    }

    /// The cartridge's handles at its current grants: the stored ones if this boot minted them
    /// and the grants have not moved, else minted now.
    async fn current(
        cx: &Cx,
        parents: &Parents,
        boot: &str,
        record: &CartridgeInstallRecord,
    ) -> Result<(Minted, bool), MicrosError> {
        let held = Self::stored(cx, boot, &record.cartridge_id).await?;
        if let Some(held) = held {
            return Ok((held, false));
        }
        let grants = Self::grants_at(cx, &record.cartridge_id, None, 0).await?;
        let minted = Self::mint(cx, parents, record, &grants, None).await?;
        Self::store(cx, boot, &record.cartridge_id, &minted).await?;
        Ok((minted, true))
    }

    /// Re-mints `record`'s micros for `grants`, which moved.
    async fn remint(
        cx: &Cx,
        parents: &Parents,
        boot: &str,
        record: &CartridgeInstallRecord,
        grants: &CartridgeGrantsAt,
    ) -> Result<Minted, MicrosError> {
        let held = Self::stored(cx, boot, &record.cartridge_id).await?;
        let minted = Self::mint(cx, parents, record, grants, held).await?;
        Self::store(cx, boot, &record.cartridge_id, &minted).await?;
        Ok(minted)
    }
}

/// The scopes a micro's handles were minted for, in a form that compares (the scopes themselves
/// are compared by what they serialize to).
fn scopes_wire(scopes: &RoutineScopes) -> Value {
    json!({
        "dispatch": scopes.dispatch,
        "docs": scopes.docs,
        "drawers": scopes.drawers,
        "blobs": scopes.blobs,
    })
}

impl Handler<Handles> for Micros {
    async fn call(&self, cx: &Cx, input: Handles) -> Result<CartridgeHandles, MicrosError> {
        Self::require_operator(cx).await?;
        let parents = Parents::of(cx)?;
        let boot = Authorization::id_text(parents.actor);
        let record = Self::installed(cx, &input.cartridge_id).await?;
        let (minted, _) = Self::current(cx, &parents, &boot, &record).await?;
        Ok(minted.answer(&boot, input.micro.as_deref()))
    }
}

impl Handler<AwaitHandles> for Micros {
    async fn call(&self, cx: &Cx, input: AwaitHandles) -> Result<Moved, MicrosError> {
        Self::require_operator(cx).await?;
        let parents = Parents::of(cx)?;
        let boot = Authorization::id_text(parents.actor);
        let hold_ms = input.hold_ms.min(AWAIT_MAX_HOLD_MS);
        let installs = cx
            .call(ListCartridgeInstalls {})
            .await
            .map_err(upstream("listing installs"))?
            .installs;
        let mut watched = Vec::new();
        for (id, since) in &input.since {
            let record = installs
                .iter()
                .find(|record| record.cartridge_id.to_string() == *id)
                .ok_or_else(|| MicrosError::UnknownCartridge(id.clone()))?;
            watched.push((record, since));
        }
        let mut moved = BTreeMap::new();
        // A `since` that is not current answers at once, with the handles it is behind.
        let mut current = Vec::new();
        for (record, since) in watched {
            let (minted, _) = Self::current(cx, &parents, &boot, record).await?;
            let answer = minted.answer(&boot, None);
            if answer.revision != *since {
                moved.insert(record.cartridge_id.to_string(), answer);
            } else {
                current.push((record, minted));
            }
        }
        if !moved.is_empty() || current.is_empty() || hold_ms == 0 {
            return Ok(Moved { cartridges: moved });
        }
        // Otherwise hold on the grant feed of every watched cartridge at once, and re-mint the
        // ones that moved. The first answer ends the hold: the host polls again for the rest.
        let waits = current.iter().map(|(record, minted)| {
            Box::pin(async move {
                let grants = Self::grants_at(
                    cx,
                    &record.cartridge_id,
                    Some(minted.grants.clone()),
                    hold_ms,
                )
                .await?;
                Ok::<_, MicrosError>((*record, minted, grants))
            })
        });
        let (answer, _, _) = futures::future::select_all(waits).await;
        let (record, minted, grants) = answer?;
        if grants.revision != minted.grants {
            let remade = Self::remint(cx, &parents, &boot, record, &grants).await?;
            moved.insert(record.cartridge_id.to_string(), remade.answer(&boot, None));
        }
        Ok(Moved { cartridges: moved })
    }
}

impl big_ndk::Nodule for Micros {
    async fn route(&self, nsid: &str, cx: &Cx, body: big_ndk::XrpcBody) -> Option<big_ndk::Reply> {
        match nsid.split('?').next().unwrap_or(nsid) {
            nsid if nsid == Handles::XRPC.nsid => {
                Some(big_ndk::invoke::<Self, Handles>(self, cx, body).await)
            }
            nsid if nsid == AwaitHandles::XRPC.nsid => {
                Some(big_ndk::invoke::<Self, AwaitHandles>(self, cx, body).await)
            }
            _ => None,
        }
    }
}
