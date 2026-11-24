//! Nodule admission over [`big_cartridges_core::Cartridge`] envelopes (ADR 027 §2–4, §6–8).
//!
//! [`admit_resolved_cartridge`] validates the carrying [`big_cartridges_core::Cartridge`] envelope, extracts its
//! [`Nodule`] contribution, checks the contribution schema and nodule admission rules, and
//! verifies that every component a routine names is resolved. [`InstalledNodule`] is the admitted result that
//! [`crate::config::NoduleConfig`] resolves, with its enablement, artifact availability, and
//! readiness.

use crate::Admitted;
use crate::Nodule;
use crate::NoduleAdmission;
use crate::NoduleError;

use big_cabinet_core::DatumTag;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::doc::DatumKinds;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::DrawerReach;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cartridges_core::CartridgeError;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Component as ManifestComponent;
use big_cartridges_core::Permbind;
use big_cartridges_core::ResolvedCartridge;
use big_cartridges_core::Revision;
use big_cartridges_core::RoutineUse;
use big_cartridges_core::Version;
use big_cartridges_core::config::Component;
use big_cartridges_core::protocol::AwaitCartridgeGrantRequest;
use big_cartridges_core::protocol::RequestCartridgeGrant;
use big_dispatch_core::DispatchScope;
use method_core::HandleNeed;
use method_core::Method;
use std::collections::BTreeSet;

use big_dispatch_core::Readiness;
use serde::Deserialize;
use serde::Serialize;

use std::collections::BTreeMap;

/// Availability of an artifact declared in the carrying [`big_cartridges_core::Cartridge`] envelope.
///
/// Admission records every artifact the handler references as [`Self::Unavailable`] from the
/// envelope metadata alone. Only a node's executor, which loads the bytes, can establish
/// [`Self::Available`].
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ArtifactState {
    Unavailable,
    Available,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Artifact {
    pub digest: big_cartridges_core::Digest,
    pub size: Option<u64>,
    pub media_type: Option<String>,
}

/// The cartridge whose code fills a slot under a dev takeover, and the version of it that does.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FilledBy {
    pub cartridge_id: CartridgeId,
    pub version: Version,
}

/// An admitted nodule installed in a standalone [`Inventory`].
#[derive(Debug, Clone, PartialEq)]
pub struct InstalledNodule {
    /// The cartridge config document containing this nodule's settings and grants.
    pub cartridge_id: CartridgeId,
    pub config_doc: DocId,
    pub version: Version,
    pub revision: Revision,
    pub resolved: ResolvedCartridge,
    pub nodule: Nodule,
    pub admitted: Admitted,
    pub artifacts: BTreeMap<String, (Artifact, ArtifactState)>,
    pub enabled: bool,
    /// Set when a dev takeover fills this slot: `cartridge_id` is the slot, while `version`,
    /// `revision`, `nodule`, `admitted` and `artifacts` are the filler's.
    pub filled_by: Option<FilledBy>,
}

impl InstalledNodule {
    #[expect(
        clippy::too_many_arguments,
        reason = "constructor maps the persisted install record fields one-for-one"
    )]
    pub(crate) fn new(
        cartridge_id: CartridgeId,
        config_doc: DocId,
        version: Version,
        revision: Revision,
        resolved: ResolvedCartridge,
        nodule: Nodule,
        admitted: Admitted,
        artifacts: BTreeMap<String, (Artifact, ArtifactState)>,
        enabled: bool,
    ) -> Self {
        Self {
            cartridge_id,
            config_doc,
            version,
            revision,
            resolved,
            nodule,
            admitted,
            artifacts,
            enabled,
            filled_by: None,
        }
    }

    /// Each declared artifact's availability, by key.
    pub fn artifact_states(&self) -> BTreeMap<String, ArtifactState> {
        self.artifacts
            .iter()
            .map(|(key, (_, state))| (key.clone(), *state))
            .collect()
    }

    /// Readiness as the inventory alone can tell it, which is what a node without an executor
    /// reports. An enabled nodule with a handler never yields [`Readiness::Ready`] here, since
    /// only an executor can serve a handler; one without a handler hosts nothing (admission
    /// refuses `implements` without a handler), so there is nothing for it to wait on.
    pub fn readiness(&self) -> Readiness {
        if !self.enabled {
            Readiness::Disabled
        } else if self.artifacts.is_empty() {
            Readiness::Ready
        } else if self
            .artifacts
            .values()
            .any(|(_, state)| *state == ArtifactState::Unavailable)
        {
            Readiness::ArtifactsUnavailable
        } else {
            Readiness::ExecutorUnavailable
        }
    }

    /// Each routine's desired scopes: what its `uses` list, intersected with what the cartridge was
    /// granted (`effective_operations` for methods, `desired_grants` for drawers; ADR 049 §4.1) and
    /// with what the permbind declares. A grant is the cartridge's, so one grant of a drawer
    /// satisfies every routine whose `uses` take a `#drawers` permbind that needs it, and a routine
    /// whose `uses` do not take one reaches no drawer however many are granted.
    ///
    /// `builtin_handle_need` maps a built-in method's NSID to the service handle it needs beyond
    /// dispatch's admission (`method_core::XrpcMethod::handle`); the caller supplies it because
    /// the catalogue (`method_lexicon`) depends on this crate and cannot be called from here.
    pub fn routine_scopes(
        &self,
        desired_grants: &BTreeMap<DrawerId, DrawerAccess>,
        effective_operations: &DispatchScope,
        builtin_handle_need: impl Fn(&str) -> Option<HandleNeed>,
    ) -> BTreeMap<String, RoutineScopes> {
        self.nodule
            .routines
            .iter()
            .map(|(routine_key, routine)| {
                (
                    routine_key.clone(),
                    RoutineScopes::lower(
                        &self.resolved,
                        &self.config_doc,
                        &routine.uses,
                        desired_grants,
                        effective_operations,
                        &builtin_handle_need,
                    ),
                )
            })
            .collect()
    }
}

impl RoutineScopes {
    /// The scopes of one `uses` map (a routine's, or a micro's) over `resolved`'s permbinds, which
    /// is the whole lowering of [`InstalledNodule::routine_scopes`] for one routine, so the nodule
    /// service and the micros nodule (which lowers a micro's `uses` with the same code, in a
    /// guest) cannot disagree about what a `uses` entry confers.
    pub fn lower(
        resolved: &ResolvedCartridge,
        config_doc: &DocId,
        uses: &BTreeMap<String, RoutineUse>,
        desired_grants: &BTreeMap<DrawerId, DrawerAccess>,
        effective_operations: &DispatchScope,
        builtin_handle_need: &impl Fn(&str) -> Option<HandleNeed>,
    ) -> Self {
        let settings_kinds = resolved.settings_kinds();
        let mut operations = BTreeSet::new();
        let mut read = BTreeSet::new();
        let mut write = BTreeSet::new();
        // Every `#datum` kind the routine's `uses` take, settings or not: the kinds its docs
        // handle reaches inside the drawers it was granted.
        let mut datum_read = BTreeSet::new();
        let mut datum_write = BTreeSet::new();
        let mut drawers = BTreeMap::new();
        let mut create_drawer = false;
        // The operation-level flags of the blobs handle the routine's operations need, stated
        // beside each operation's definition (ADR 049 §6.2).
        let mut blobs_flags = serde_json::Map::new();
        for (binding, usage) in uses {
            let permbind = resolved
                .cartridge
                .permbinds
                .get(binding)
                .expect("admission refuses a routine use without its permbind");
            match permbind {
                // Listing a method permbind is the use: method access has no
                // read/write/create/delete to inspect.
                Permbind::Method(method) => {
                    if effective_operations
                        .operations
                        .contains(method.nsid.as_str())
                    {
                        operations.insert(method.nsid.to_string());
                        if let Some(need) = builtin_handle_need(method.nsid.as_str()) {
                            assert_eq!(
                                need.mint, BLOBS_MINT,
                                "a built-in operation's handle need names a domain the nodule runtime mints"
                            );
                            blobs_flags.extend(
                                need.flags.iter().map(|flag| {
                                    ((*flag).to_owned(), serde_json::Value::Bool(true))
                                }),
                            );
                        }
                    }
                }
                Permbind::Datum(datum) => {
                    let tag = DatumTag::new(datum.nsid.to_string())
                        .expect("an admitted datum binding has a valid tag");
                    if usage.read {
                        datum_read.insert(tag.clone());
                    }
                    if usage.write || usage.create || usage.delete {
                        datum_write.insert(tag.clone());
                    }
                    // What the cartridge-wide capabilities this replaces reached, and so
                    // all a datum permbind reaches: the cartridge config document, whole
                    // for reads and the declared settings kinds for writes. No code ever
                    // turned a non-settings `#datum` permbind into a document scope, and
                    // no document set is named for one yet, so such a permbind confers
                    // nothing here rather than a document this change invented.
                    if !settings_kinds
                        .covers(&DatumKinds::Only(std::iter::once(tag.clone()).collect()))
                    {
                        continue;
                    }
                    if usage.read {
                        read.insert(tag.clone());
                    }
                    if usage.write || usage.create || usage.delete {
                        write.insert(tag);
                    }
                }
                Permbind::Drawers(drawer) => {
                    // What the routine uses on existing drawers, as far as the permbind
                    // declares it and a drawer was granted that far. `create` is not
                    // among them: it sets only the scope's create flag below, so a
                    // create-only routine can make drawers without any access to the
                    // ones that exist (grants never widen silently, ADR 028). Deleting
                    // has no access level of its own, so it needs Write.
                    let access = [
                        (usage.read && drawer.read.is_some(), DrawerAccess::Read),
                        (usage.write && drawer.write.is_some(), DrawerAccess::Write),
                        (usage.delete && drawer.delete.is_some(), DrawerAccess::Write),
                    ]
                    .into_iter()
                    .filter_map(|(used, access)| used.then_some(access))
                    .max();
                    if let Some(access) = access {
                        drawers.extend(
                            desired_grants
                                .iter()
                                .filter(|(_, granted)| **granted >= access)
                                .map(|(id, _)| (id.clone(), access)),
                        );
                    }
                    create_drawer |= usage.create && drawer.create.is_some();
                }
                // Network access is checked where the request leaves (`egress::authorize_net`),
                // not by a handle: there is no scope to mint.
                Permbind::Net(_) | Permbind::Unknown(_) => {}
            }
        }
        // A routine with something in its `uses` may always ask for it and wait for the
        // answer: filing confers nothing, and the grant that would admit the call is the very
        // thing the request is for, so it cannot be a precondition of it (ADR 049 §4.2).
        if !uses.is_empty() {
            operations.extend(
                [
                    RequestCartridgeGrant::XRPC.nsid,
                    AwaitCartridgeGrantRequest::XRPC.nsid,
                ]
                .map(str::to_owned),
            );
        }
        let docs = routine_docs_scope(
            config_doc,
            &drawers,
            [read, write],
            [datum_read, datum_write],
            &mut operations,
        );
        let blobs = (!blobs_flags.is_empty()).then(|| {
            serde_json::from_value(serde_json::Value::Object(blobs_flags))
                .expect("a handle need's flags are fields of the blobs scope")
        });
        let drawers = (!drawers.is_empty() || create_drawer).then_some(DrawerScope {
            drawers,
            create: create_drawer,
        });
        let dispatch = (!operations.is_empty()).then(|| DispatchScope {
            operations,
            authorities: effective_operations.authorities.clone(),
            privileged: effective_operations.privileged,
        });
        RoutineScopes {
            dispatch,
            docs,
            drawers,
            blobs,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InventoryError {
    #[error(transparent)]
    Cartridge(#[from] CartridgeError),
    #[error(transparent)]
    Nodule(#[from] NoduleError),
    #[error("cartridge `{0}` carries no nodule contribution")]
    MissingContribution(CartridgeId),
    #[error("cartridge `{id}` nodule contribution has unsupported schema `{schema}`")]
    UnsupportedSchema { id: CartridgeId, schema: String },
    #[error("cartridge `{id}` nodule contribution is not a valid nodule datum: {reason}")]
    InvalidContribution { id: CartridgeId, reason: String },
    #[error(
        "cartridge `{id}` handler requires artifact `{artifact}` not declared in cartridge envelope"
    )]
    MissingArtifact { id: CartridgeId, artifact: String },
    #[error(
        "cartridge `{id}` handler artifact `{artifact}` declares no size, which a node needs to \
         reference and fetch its bytes"
    )]
    ArtifactSizeRequired { id: CartridgeId, artifact: String },
    #[error(
        "cartridge `{id}` holds authority `{authority}`, which overlaps `{existing_authority}` held by `{existing_cartridge}`"
    )]
    AuthorityConflict {
        id: CartridgeId,
        authority: String,
        existing_cartridge: Box<CartridgeId>,
        existing_authority: String,
    },
    #[error(
        "cartridge `{id}` carries Lexicon `{nsid}` in a copy that conflicts with the one carried by \
         `{other}`: copies must be identical unless their declared revisions differ, and a \
         vendored copy never out-revises the authority's"
    )]
    LexiconConflict {
        id: CartridgeId,
        nsid: String,
        other: Box<CartridgeId>,
    },
    #[error("cannot take over `{id}`: {reason}")]
    InvalidDevTakeover { id: CartridgeId, reason: String },
    #[error("invalid method route `{pattern}`: {reason}")]
    InvalidRoute { pattern: String, reason: String },
    #[error("nodule for cartridge `{0}` is not installed")]
    UnknownCartridge(CartridgeId),
    #[error(
        "cartridge `{0}` contributes neither a nodule nor a micro, so there is nothing to install"
    )]
    NothingToInstall(CartridgeId),
    #[error("invalid grant `{grant}`: {reason}")]
    InvalidGrant { grant: String, reason: String },
    #[error("cartridge `{id}` has no `{component}` component to {action}")]
    NoComponent {
        id: CartridgeId,
        component: Component,
        action: &'static str,
    },
}

type Result<T> = std::result::Result<T, InventoryError>;

pub(crate) type AdmittedContribution = (
    ResolvedCartridge,
    Nodule,
    Admitted,
    BTreeMap<String, (Artifact, ArtifactState)>,
);

pub(crate) fn admit_resolved_cartridge(
    resolved: &ResolvedCartridge,
) -> Result<AdmittedContribution> {
    let cartridge = &resolved.cartridge;
    cartridge.validate()?;
    let nodule = cartridge
        .nodule
        .clone()
        .ok_or_else(|| InventoryError::MissingContribution(cartridge.id.clone()))?;
    let admitted = resolved.admit(cartridge.authority.as_deref())?;
    let mut artifacts = BTreeMap::new();
    for key in &admitted.artifacts {
        let Some(component) = resolved.components.get(key) else {
            return Err(InventoryError::MissingArtifact {
                id: cartridge.id.clone(),
                artifact: key.clone(),
            });
        };
        let ManifestComponent::Wasm(wasm) = component else {
            return Err(InventoryError::MissingArtifact {
                id: cartridge.id.clone(),
                artifact: key.clone(),
            });
        };
        artifacts.insert(
            key.clone(),
            (
                Artifact {
                    digest: wasm.digest,
                    size: Some(wasm.size),
                    media_type: Some(wasm.media_type.clone()),
                },
                ArtifactState::Unavailable,
            ),
        );
    }
    Ok((resolved.clone(), nodule, admitted, artifacts))
}

/// The blobs service's mint operation: the one capability domain beyond dispatch, docs and
/// drawers whose handle a routine's operations can need (ADR 049 §6.2).
const BLOBS_MINT: &str = "org.example.daybook.blobs.mintCapability";

/// The scope of each service handle a routine should hold; a service the routine has no access
/// to is `None`, and no handle is minted for it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RoutineScopes {
    pub dispatch: Option<DispatchScope>,
    /// The docs handle: the cartridge config document and, narrowed to the routine's `#datum`
    /// kinds, each drawer it was granted (ADR 049 §2).
    pub docs: Option<DocScope>,
    pub drawers: Option<DrawerScope>,
    pub blobs: Option<big_blobs_core::protocol::BlobsScope>,
}

/// The docs handle's scope for one routine: the cartridge config document's settings kinds
/// (`config`, as `[read, write]`) and each drawer the routine's `#drawers` use reaches
/// (`drawers`), at the routine's `#datum` kinds (`datum`, as `[read, write]`). Adds the docs
/// operations the handle is useless without to `operations`.
fn routine_docs_scope(
    config_doc: &DocId,
    drawers: &BTreeMap<DrawerId, DrawerAccess>,
    [read, write]: [BTreeSet<DatumTag>; 2],
    [datum_read, datum_write]: [BTreeSet<DatumTag>; 2],
    operations: &mut BTreeSet<String>,
) -> Option<DocScope> {
    // Reading or writing datum kinds in a granted drawer is the default shape of drawer
    // access (ADR 049 §2): each drawer the routine's `#drawers` use reaches is reached for
    // the routine's `#datum` kinds only, the kinds it reads for reading and the kinds it
    // writes (`write`, `create`, `delete`) for writing, so a routine that reads one kind and
    // writes another cannot write the first. A write kind needs the drawers use to reach
    // the drawer for `Write`; without it the kind contributes nothing. A routine with no
    // `#datum` kinds reaches no document in any drawer; broad access is a separate,
    // explicit permbind.
    let drawer_reach: BTreeMap<_, _> = drawers
        .iter()
        .filter_map(|(drawer, access)| {
            let write = if *access >= DrawerAccess::Write {
                datum_write.clone()
            } else {
                BTreeSet::new()
            };
            if datum_read.is_empty() && write.is_empty() {
                return None;
            }
            Some((
                drawer.clone(),
                DrawerReach {
                    access: if write.is_empty() {
                        DrawerAccess::Read
                    } else {
                        DrawerAccess::Write
                    },
                    read: DatumKinds::Only(datum_read.clone()),
                    write: DatumKinds::Only(write),
                },
            ))
        })
        .collect();
    (!read.is_empty() || !write.is_empty() || !drawer_reach.is_empty()).then(|| {
        let by_doc = |kinds: BTreeSet<DatumTag>| {
            if kinds.is_empty() {
                BTreeMap::new()
            } else {
                BTreeMap::from([(config_doc.clone(), DatumKinds::Only(kinds))])
            }
        };
        // The nodule reaches the docs service through dispatch, so the handle that
        // names the document kinds is useless without the operations that carry it.
        if !read.is_empty() || !drawer_reach.is_empty() {
            operations.insert("org.example.daybook.docs.readDatums".to_owned());
        }
        if !write.is_empty()
            || drawer_reach
                .values()
                .any(|reach| reach.access >= DrawerAccess::Write)
        {
            operations.insert("org.example.daybook.docs.writeDatums".to_owned());
        }
        DocScope {
            read: by_doc(read),
            write: by_doc(write),
            drawers: drawer_reach,
            ..Default::default()
        }
    })
}
