//! The runtime grant requests of this boot (ADR 049 §4.2) and the grants that last as long.
//!
//! Everything here is in memory by design: "only this time" and a decline last until the app and
//! node next start, so a routine refiles after a restart and nothing here is ever written to a
//! config document. The state actor owns one [`GrantRequests`]; the service reads the queue
//! through the [`watch`] receiver [`GrantRequests::new`] hands out, for its list and its long
//! polls.

use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Permbind;
use big_cartridges_core::config::GrantBody;
use big_cartridges_core::config::GrantKey;
use big_cartridges_core::protocol::CartridgeGrantRecord;
use big_cartridges_core::protocol::FiledGrantRequest;
use big_cartridges_core::protocol::GrantRequestRecord;
use big_cartridges_core::protocol::GrantRequestState;
use big_cartridges_core::protocol::RequestedAccess;
use big_nodule_core::config::CartridgeConfig;
use big_nodule_core::config::DatumSlot;
use big_nodule_core::protocol::NoduleRecord;
use big_nodule_core::protocol::NodulesError;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
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
    /// Grants in force for this boot only, which the state actor attaches to each cartridge's
    /// config document snapshot so a routine's handles are minted from them exactly as from a
    /// datum.
    boot_grants: BTreeMap<CartridgeId, BTreeMap<GrantKey, GrantBody>>,
    /// The (cartridge, permbind) pairs a person declined this boot: filing one again is answered
    /// `Declined` at once, so a routine that retries cannot prompt in a loop.
    declined: BTreeSet<(CartridgeId, String)>,
    /// The permbinds each admitted cartridge's manifest declares, as the last reconcile saw
    /// them: what a request may name.
    declared: BTreeMap<CartridgeId, BTreeMap<String, Permbind>>,
}

impl GrantRequests {
    pub(crate) fn new() -> (Self, watch::Receiver<Requests>) {
        let (queue, receiver) = watch::channel(Requests::new());
        (
            Self {
                queue,
                boot_grants: BTreeMap::new(),
                declined: BTreeSet::new(),
                declared: BTreeMap::new(),
            },
            receiver,
        )
    }

    /// Records the permbinds the admitted cartridges declare, replacing what the last reconcile
    /// saw.
    pub(crate) fn declare(&mut self, declared: BTreeMap<CartridgeId, BTreeMap<String, Permbind>>) {
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
        let declared = self
            .declared
            .get(&cartridge_id)
            .and_then(|permbinds| permbinds.get(&permbind))
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
            Permbind::Datum(_) | Permbind::Unknown(_) => {
                return Err(NodulesError::Rejected(format!(
                    "permbind `{permbind}` is not requestable: only a method or a drawers \
                     permbind has an answer of its own"
                )));
            }
        };
        if held.is_some_and(|record| covers(record, &access)) {
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

    /// The grant that answering `record` with a yes writes: a method's, or the drawer `picked`
    /// (else the one the request names) at the access the permbind declares.
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
fn covers(record: &NoduleRecord, access: &RequestedAccess) -> bool {
    match access {
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
