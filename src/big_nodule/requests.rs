//! The runtime grant requests of this boot (ADR 049 §4.2) and the grants that last as long.
//!
//! Everything here is in memory by design: "only this time" and a decline last until the app and
//! node next start, so a routine refiles after a restart and nothing here is ever written to a
//! config document. The state actor owns one [`GrantRequests`]; the service reads the queue
//! through the [`watch`] receiver [`GrantRequests::new`] hands out, for its list and its long
//! polls.

use big_cabinet_core::authority::CapId;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Permbind;
use big_cartridges_core::PermbindNet;
use big_cartridges_core::config::GrantBody;
use big_cartridges_core::config::GrantKey;
use big_cartridges_core::config::NetTarget;
use big_cartridges_core::protocol::CartridgeGrantRecord;
use big_cartridges_core::protocol::FiledGrantRequest;
use big_cartridges_core::protocol::GrantRequestRecord;
use big_cartridges_core::protocol::GrantRequestState;
use big_cartridges_core::protocol::RequestedAccess;
use big_dispatch_core::DispatchError;
use big_nodule_core::config::CartridgeConfig;
use big_nodule_core::config::DatumSlot;
use big_nodule_core::protocol::NoduleRecord;
use big_nodule_core::protocol::NodulesError;
use method_core::ContextError;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::watch;

type Res<T> = Result<T, NodulesError>;

/// One request and where it stands. Answered entries stay for the boot so a caller that awaits
/// late still learns the answer.
#[derive(Debug, Clone)]
pub(crate) struct RequestEntry {
    pub(crate) record: GrantRequestRecord,
    pub(crate) state: GrantRequestState,
}

pub(crate) type Requests = BTreeMap<Uuid7, RequestEntry>;

pub(crate) struct GrantRequests {
    queue: watch::Sender<Requests>,
    /// Each cartridge's grants as the last reconcile left them (see [`Self::publish_grants`]).
    grants: watch::Sender<GrantsFeed>,
    /// Grants in force for this boot only, which the state actor attaches to each cartridge's
    /// config document snapshot so a routine's handles are minted from them exactly as from a
    /// datum.
    boot_grants: BTreeMap<CartridgeId, BTreeMap<GrantKey, GrantBody>>,
    /// The (cartridge, permbind) pairs a person declined this boot: filing one again is answered
    /// `Declined` at once, so a routine that retries cannot prompt in a loop.
    declined: BTreeSet<(CartridgeId, String)>,
    /// The permbinds each admitted cartridge's manifest declares, as the last reconcile saw
    /// them: what a request may name.
    declared: BTreeMap<CartridgeId, Declared>,
}

/// What a cartridge's last reconcile said of it: the permbinds its manifest declares, and the
/// network hosts its grants already cover.
#[derive(Debug, Clone, Default)]
pub(crate) struct Declared {
    pub(crate) permbinds: BTreeMap<String, Permbind>,
    pub(crate) nets: BTreeSet<NetTarget>,
}

/// What one routine's `uses` leave to be refused for want of a grant someone may still give.
#[derive(Debug, Clone, Default)]
pub(crate) struct RoutineUses {
    /// The operations its method permbinds declare that the cartridge's grants do not cover,
    /// and the permbind of each. An operation absent here was never declared (or is granted,
    /// in which case dispatch did not refuse it for want of the handle).
    pub(crate) ungranted: BTreeMap<String, String>,
    /// Every `#net` permbind it takes.
    pub(crate) nets: Vec<NetUse>,
}

/// One `#net` permbind a routine takes, and whether the cartridge holds a matching `grant/net`.
#[derive(Debug, Clone)]
pub(crate) struct NetUse {
    pub(crate) binding: String,
    pub(crate) permbind: PermbindNet,
    pub(crate) target: NetTarget,
    pub(crate) granted: bool,
}

/// How the nodule host tells a refusal of "nobody has said yes yet" from one of "the rules forbid
/// it" (ADR 049 §4.3), for one cartridge. Dispatch alone cannot: it sees an operation outside the
/// handle, and only the runtime knows the routine's `uses` and the grants.
///
/// The state actor rewrites [`Self::set`] at each reconcile, so every clone, which an executor
/// keeps for the cartridge's life, sees the routines' current handles.
#[derive(Clone)]
pub struct MissingAuthorityLookup {
    cartridge_id: CartridgeId,
    /// Per routine, by the dispatch handle it was delivered: what its `uses` leave refused for
    /// want of a grant, as of the last reconcile.
    routines: Arc<RwLock<HashMap<CapId, RoutineUses>>>,
    requests: watch::Receiver<Requests>,
}

impl MissingAuthorityLookup {
    pub(crate) fn set(&self, routines: HashMap<CapId, RoutineUses>) {
        *self.routines.write().expect("routine uses lock") = routines;
    }

    /// The pending request filed for `permbind` of this cartridge, if any (nothing files one
    /// here: only the routine decides to ask).
    fn filed(&self, permbind: &str) -> Option<String> {
        self.requests
            .borrow()
            .values()
            .find(|entry| {
                entry.state == GrantRequestState::Pending
                    && entry.record.cartridge_id == self.cartridge_id
                    && entry.record.permbind == permbind
            })
            .map(|entry| entry.record.id.to_string())
    }

    fn missing(&self, permbind: String) -> DispatchError {
        DispatchError::Context(ContextError::MissingAuthority {
            cartridge: self.cartridge_id.to_string(),
            request: self.filed(&permbind),
            permbind,
        })
    }

    /// Whether the routine holding the dispatch handle `capability` may send a `method` request
    /// to `host` on `port` (the URL's own or its scheme's default): its `uses` take a `#net`
    /// permbind naming that host and port and allowing the verb, and the cartridge holds a
    /// matching `grant/net`. Declared but not granted is the cartridge's `MissingAuthority`,
    /// carrying the request already filed for the permbind; anything else, which no grant could
    /// change, is [`DispatchError::Denied`] (ADR 049 §4.3). The one check every executor puts a
    /// guest's non-dispatch request through.
    pub fn authorize_net(
        &self,
        capability: CapId,
        method: &str,
        host: &str,
        port: u16,
    ) -> Result<(), DispatchError> {
        let routines = self.routines.read().expect("routine uses lock");
        let mut ungranted = None;
        for net in routines
            .get(&capability)
            .into_iter()
            .flat_map(|uses| &uses.nets)
        {
            if !net.target.reaches(host, port) || !net.permbind.allows_method(method) {
                continue;
            }
            if net.granted {
                return Ok(());
            }
            ungranted.get_or_insert(&net.binding);
        }
        Err(match ungranted {
            Some(binding) => self.missing(binding.clone()),
            None => DispatchError::Denied,
        })
    }

    /// `error`, as the routine holding the dispatch handle `capability` should see it for a call
    /// of `operation`: a [`DispatchError::Denied`] for a declared but ungranted operation becomes
    /// the cartridge's `MissingAuthority`, carrying the request already filed for its permbind
    /// (nothing files one here: only the routine decides to ask). Any other refusal is returned
    /// as it was.
    pub fn refine(
        &self,
        capability: CapId,
        operation: &str,
        error: DispatchError,
    ) -> DispatchError {
        if error != DispatchError::Denied {
            return error;
        }
        let Some(permbind) = self
            .routines
            .read()
            .expect("routine uses lock")
            .get(&capability)
            .and_then(|uses| uses.ungranted.get(operation))
            .cloned()
        else {
            return error;
        };
        self.missing(permbind)
    }
}

impl GrantRequests {
    /// The attribution of refusals for `cartridge_id`, which starts out knowing nothing.
    pub(crate) fn lookup(&self, cartridge_id: CartridgeId) -> MissingAuthorityLookup {
        MissingAuthorityLookup {
            cartridge_id,
            routines: Arc::default(),
            requests: self.queue.subscribe(),
        }
    }

    /// The requests, and the receivers of the queue and of the grants feed the service follows.
    pub(crate) fn new() -> (Self, watch::Receiver<Requests>, watch::Receiver<GrantsFeed>) {
        let (queue, receiver) = watch::channel(Requests::new());
        let (grants, grants_receiver) = watch::channel(GrantsFeed::new());
        (
            Self {
                queue,
                grants,
                boot_grants: BTreeMap::new(),
                declined: BTreeSet::new(),
                declared: BTreeMap::new(),
            },
            receiver,
            grants_receiver,
        )
    }

    /// Publishes each cartridge's grants, the written ones in `docs` and this boot's, to the
    /// feed `awaitCartridgeGrants` follows. The state actor calls it at the end of every
    /// reconcile, which every grant change runs: a written grant moves a config document, and an
    /// "only this time" answer or its revoke reconciles where it is made.
    pub(crate) fn publish_grants(&self, docs: &BTreeMap<CartridgeId, CartridgeConfig>) {
        let grants = self.list(docs, None);
        self.grants.send_if_modified(|feed| feed.update(grants));
    }

    /// Records the permbinds the admitted cartridges declare, replacing what the last reconcile
    /// saw.
    pub(crate) fn declare(&mut self, declared: BTreeMap<CartridgeId, Declared>) {
        self.declared = declared;
    }

    /// The grants of `cartridge_id` that last the boot, for its config snapshot.
    pub(crate) fn boot_grants(
        &self,
        cartridge_id: &CartridgeId,
    ) -> Option<&BTreeMap<GrantKey, GrantBody>> {
        self.boot_grants.get(cartridge_id)
    }

    /// Takes back the cartridge's "only this time" grants: all of them, or one.
    pub(crate) fn revoke_boot_grants(
        &mut self,
        cartridge_id: &CartridgeId,
        grant: Option<&GrantKey>,
    ) {
        match grant {
            Some(grant) => {
                if let Some(grants) = self.boot_grants.get_mut(cartridge_id) {
                    grants.remove(grant);
                }
            }
            None => {
                self.boot_grants.remove(cartridge_id);
            }
        }
    }

    /// Files a request for `permbind` of `cartridge_id`, whose manifest must declare it. `held` is
    /// the cartridge's record from the last reconcile, which says what its grants already give.
    pub(crate) fn file(
        &mut self,
        cartridge_id: CartridgeId,
        permbind: String,
        drawer: Option<DrawerId>,
        held: Option<&NoduleRecord>,
    ) -> Res<FiledGrantRequest> {
        let held_nets = self
            .declared
            .get(&cartridge_id)
            .map(|declared| &declared.nets);
        let declared = self
            .declared
            .get(&cartridge_id)
            .and_then(|declared| declared.permbinds.get(&permbind))
            .ok_or_else(|| {
                NodulesError::Rejected(format!(
                    "cartridge `{cartridge_id}` declares no permbind `{permbind}`"
                ))
            })?;
        let (description, access) = match declared {
            Permbind::Method(method) => {
                if drawer.is_some() {
                    return Err(NodulesError::Rejected(
                        "a method permbind names no drawer".to_owned(),
                    ));
                }
                (
                    method.description.clone(),
                    RequestedAccess::Method {
                        nsid: method.nsid.to_string(),
                    },
                )
            }
            Permbind::Drawers(drawers) => {
                let access = drawers.declared_access().ok_or_else(|| {
                    NodulesError::Rejected(format!(
                        "permbind `{permbind}` declares no read, write or delete, so no drawer \
                         grant satisfies it"
                    ))
                })?;
                (
                    drawers.description.clone(),
                    RequestedAccess::Drawers { access, drawer },
                )
            }
            Permbind::Net(net) => {
                if drawer.is_some() {
                    return Err(NodulesError::Rejected(
                        "a net permbind names no drawer".to_owned(),
                    ));
                }
                (
                    net.description.clone(),
                    RequestedAccess::Net {
                        host: net.host.to_ascii_lowercase(),
                        port: net.port,
                    },
                )
            }
            Permbind::Datum(_) | Permbind::Unknown(_) => {
                return Err(NodulesError::Rejected(format!(
                    "permbind `{permbind}` is not requestable: only a method, a drawers or a net \
                     permbind has an answer of its own"
                )));
            }
        };
        if held.is_some_and(|record| covers(record, held_nets, &access)) {
            return Ok(FiledGrantRequest {
                request: None,
                state: GrantRequestState::Allowed,
            });
        }
        if self
            .declined
            .contains(&(cartridge_id.clone(), permbind.clone()))
        {
            return Ok(FiledGrantRequest {
                request: None,
                state: GrantRequestState::Declined,
            });
        }
        if let Some(pending) = self.queue.borrow().values().find(|entry| {
            entry.state == GrantRequestState::Pending
                && entry.record.cartridge_id == cartridge_id
                && entry.record.permbind == permbind
        }) {
            return Ok(FiledGrantRequest {
                request: Some(pending.record.id),
                state: GrantRequestState::Pending,
            });
        }
        let id = Uuid7::new();
        let record = GrantRequestRecord {
            id,
            cartridge_id,
            permbind,
            description,
            access,
            filed_at_ms: id.unix_millis(),
        };
        self.queue.send_modify(|queue| {
            queue.insert(
                id,
                RequestEntry {
                    record,
                    state: GrantRequestState::Pending,
                },
            );
        });
        Ok(FiledGrantRequest {
            request: Some(id),
            state: GrantRequestState::Pending,
        })
    }

    /// The pending request `request`.
    pub(crate) fn pending(&self, request: Uuid7) -> Res<GrantRequestRecord> {
        let queue = self.queue.borrow();
        let entry = queue.get(&request).ok_or_else(|| {
            NodulesError::Rejected(format!("there is no grant request `{request}`"))
        })?;
        if entry.state != GrantRequestState::Pending {
            return Err(NodulesError::Rejected(format!(
                "grant request `{request}` is already answered"
            )));
        }
        Ok(entry.record.clone())
    }

    /// The grant that answering `record` with a yes writes: a method's, a network host's, or the
    /// drawer `picked` (else the one the request names) at the access the permbind declares.
    pub(crate) fn grant_for(
        record: &GrantRequestRecord,
        picked: Option<DrawerId>,
    ) -> Res<(GrantKey, GrantBody)> {
        match &record.access {
            RequestedAccess::Method { nsid } => {
                if picked.is_some() {
                    return Err(NodulesError::Rejected(
                        "a method request names no drawer".to_owned(),
                    ));
                }
                Ok((GrantKey::Method(nsid.clone()), GrantBody::present()))
            }
            RequestedAccess::Net { host, port } => {
                if picked.is_some() {
                    return Err(NodulesError::Rejected(
                        "a net request names no drawer".to_owned(),
                    ));
                }
                let target = NetTarget::new(host, *port)
                    .expect("a filed net request names the host of an admitted permbind");
                Ok((GrantKey::Net(target), GrantBody::present()))
            }
            RequestedAccess::Drawers { access, drawer } => {
                let drawer = picked.or_else(|| drawer.clone()).ok_or_else(|| {
                    NodulesError::Rejected(
                        "pick the drawer to grant: the request names none".to_owned(),
                    )
                })?;
                Ok((
                    GrantKey::Drawer(drawer),
                    GrantBody::access((*access).into()),
                ))
            }
        }
    }

    /// Records a no: the request is settled and the permbind is not asked again this boot.
    pub(crate) fn decline(&mut self, record: &GrantRequestRecord) {
        self.declined
            .insert((record.cartridge_id.clone(), record.permbind.clone()));
        self.settle(record.id, GrantRequestState::Declined);
    }

    /// Adds an "only this time" grant, never narrowing one the cartridge already holds this boot.
    pub(crate) fn grant_this_boot(
        &mut self,
        cartridge_id: CartridgeId,
        grant: GrantKey,
        body: GrantBody,
    ) {
        let access = |body: &GrantBody| body.access.as_ref().and_then(|access| access.known());
        let held = self.boot_grants.entry(cartridge_id).or_default();
        if held
            .get(&grant)
            .is_none_or(|held| access(held) < access(&body))
        {
            held.insert(grant, body);
        }
    }

    pub(crate) fn settle(&self, request: Uuid7, state: GrantRequestState) {
        self.queue.send_modify(|queue| {
            queue
                .get_mut(&request)
                .expect("a request stays in the queue until the boot ends")
                .state = state;
        });
    }

    /// A cartridge was uninstalled: its "only this time" grants end, and nobody is left to answer
    /// its pending requests.
    pub(crate) fn forget(&mut self, cartridge_id: &CartridgeId) {
        self.boot_grants.remove(cartridge_id);
        self.queue.send_modify(|queue| {
            for entry in queue.values_mut().filter(|entry| {
                entry.record.cartridge_id == *cartridge_id
                    && entry.state == GrantRequestState::Pending
            }) {
                entry.state = GrantRequestState::Declined;
            }
        });
    }

    /// The grants of `only`, or of every cartridge: the datums in their config documents, then
    /// the ones in force for this boot only.
    pub(crate) fn list(
        &self,
        docs: &BTreeMap<CartridgeId, CartridgeConfig>,
        only: Option<&CartridgeId>,
    ) -> Vec<CartridgeGrantRecord> {
        let wanted = |id: &CartridgeId| only.is_none_or(|only| only == id);
        let mut grants = Vec::new();
        for (id, doc) in docs.iter().filter(|(id, _)| wanted(id)) {
            grants.extend(doc.grants().filter_map(|(grant, slot)| match slot {
                DatumSlot::Accepted { value, .. } => Some(CartridgeGrantRecord {
                    cartridge_id: id.clone(),
                    grant: grant.clone(),
                    value: value.clone(),
                    this_boot: false,
                }),
                DatumSlot::Absent | DatumSlot::Invalid { .. } => None,
            }));
        }
        for (id, boot) in self.boot_grants.iter().filter(|(id, _)| wanted(id)) {
            grants.extend(boot.iter().map(|(grant, value)| CartridgeGrantRecord {
                cartridge_id: id.clone(),
                grant: grant.clone(),
                value: value.clone(),
                this_boot: true,
            }));
        }
        grants.sort_by(|a, b| {
            (&a.cartridge_id, a.this_boot, &a.grant).cmp(&(&b.cartridge_id, b.this_boot, &b.grant))
        });
        grants
    }
}

/// Whether the cartridge's grants, as the last reconcile resolved them, already give `access`: a
/// datum grant or an "only this time" one, which look the same from here.
fn covers(
    record: &NoduleRecord,
    nets: Option<&BTreeSet<NetTarget>>,
    access: &RequestedAccess,
) -> bool {
    match access {
        RequestedAccess::Net { host, port } => nets.is_some_and(|nets| {
            nets.iter()
                .any(|net| net.reaches(host, port.unwrap_or(NetTarget::DEFAULT_PORT)))
        }),
        RequestedAccess::Method { nsid } => record
            .methods
            .as_ref()
            .is_some_and(|methods| methods.desired.operations.contains(nsid)),
        RequestedAccess::Drawers {
            access,
            drawer: Some(drawer),
        } => record
            .drawers
            .get(drawer)
            .is_some_and(|status| status.desired >= *access),
        RequestedAccess::Drawers { drawer: None, .. } => false,
    }
}

/// Each cartridge's grants as the last reconcile left them, for `awaitCartridgeGrants` to follow:
/// the written ones and the "only this time" ones, as [`GrantRequests::list`] lists them. A
/// cartridge's revision is this boot and the feed sequence its grants last changed at, so a
/// revision from an earlier boot never names the current grants even where the sequence repeats.
#[derive(Debug)]
pub(crate) struct GrantsFeed {
    boot: Uuid7,
    seq: u64,
    cartridges: BTreeMap<CartridgeId, (u64, Vec<CartridgeGrantRecord>)>,
}

impl GrantsFeed {
    pub(crate) fn new() -> Self {
        Self {
            boot: Uuid7::new(),
            seq: 0,
            cartridges: BTreeMap::new(),
        }
    }

    pub(crate) fn revision(&self, cartridge_id: &CartridgeId) -> String {
        let seq = self.cartridges.get(cartridge_id).map_or(0, |(seq, _)| *seq);
        format!("{}.{seq}", self.boot)
    }

    pub(crate) fn grants(&self, cartridge_id: &CartridgeId) -> &[CartridgeGrantRecord] {
        self.cartridges
            .get(cartridge_id)
            .map_or(&[], |(_, grants)| grants.as_slice())
    }

    /// Takes every cartridge's grants from `grants`, moving the revision of each cartridge whose
    /// grants changed. A cartridge whose grants are all gone keeps an empty entry, so its
    /// revision moves too. Whether anything changed, for `watch::Sender::send_if_modified`.
    pub(crate) fn update(&mut self, grants: Vec<CartridgeGrantRecord>) -> bool {
        let mut next: BTreeMap<CartridgeId, Vec<CartridgeGrantRecord>> = BTreeMap::new();
        for grant in grants {
            next.entry(grant.cartridge_id.clone())
                .or_default()
                .push(grant);
        }
        let gone: Vec<CartridgeId> = self
            .cartridges
            .iter()
            .filter(|(id, (_, held))| !held.is_empty() && !next.contains_key(*id))
            .map(|(id, _)| id.clone())
            .collect();
        next.extend(gone.into_iter().map(|id| (id, Vec::new())));
        let mut changed = false;
        for (id, grants) in next {
            if self.grants(&id) == grants.as_slice() {
                continue;
            }
            self.seq += 1;
            changed = true;
            self.cartridges.insert(id, (self.seq, grants));
        }
        changed
    }
}
