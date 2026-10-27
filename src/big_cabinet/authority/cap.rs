use super::AuthorityError;
use big_cabinet_core::authority::AttenuateCapability;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::MintCapability;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::RetractCapability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::dmeta::Uuid7;
use service_utils_rs::Spawner;
use std::collections::HashMap;
use std::collections::HashSet;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use utils_rs::expect_tags::ERROR_ACTOR;
use utils_rs::expect_tags::ERROR_CHANNEL;

pub(super) const MAX_CHAIN_DEPTH: usize = 64;
pub(super) const MAX_CHAIN_NODES: usize = 4096;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Object,
    Namespace,
}

pub(super) struct CapRow {
    pub(super) id: CapId,
    pub(super) object: String,
    pub(super) rights: Rights,
    pub(super) dependencies: Vec<CapId>,
    pub(super) issuer: Uuid7,
    pub(super) kind: Kind,
    depth: usize,
}

/// The actor an actor capability names, with the `actor_caps` key that reuses it (the
/// operator's root has none), so retracting the capability drops both entries without a scan.
pub(super) struct ActorBinding {
    pub(super) actor_id: Uuid7,
    pub(super) reuse_key: Option<(Uuid7, CapId, i64)>,
}

impl CapRow {
    pub(super) fn wire(&self) -> Capability {
        Capability {
            capability_id: self.id,
            object: self.object.clone(),
            rights: self.rights,
            issuer_id: self.issuer,
        }
    }
}

/// Every live capability. Retraction deletes rows: a capability is invalid exactly when it
/// has no row, and `dependents` is the reverse of `CapRow::dependencies` so retracting one
/// capability removes everything derived from it in one pass instead of leaving dead rows
/// to be discovered walk by walk.
#[derive(Default)]
pub(super) struct State {
    pub(super) caps: HashMap<CapId, CapRow>,
    pub(super) actors: HashMap<CapId, ActorBinding>,
    pub(super) actor_caps: HashMap<(Uuid7, CapId, i64), CapId>,
    namespaces: HashMap<Uuid7, CapId>,
    dependents: HashMap<CapId, HashSet<CapId>>,
}

impl State {
    pub(super) fn insert(&mut self, cap: CapRow) {
        for dependency in &cap.dependencies {
            assert!(
                self.dependents
                    .entry(*dependency)
                    .or_default()
                    .insert(cap.id)
            );
        }
        assert!(
            self.caps.insert(cap.id, cap).is_none(),
            "capability id collision"
        );
    }

    /// Delete `root` and every capability that depends on it, transitively, together with
    /// the actor and namespace entries that pointed at the deleted rows.
    pub(super) fn retract(&mut self, root: CapId) {
        let mut pending = vec![root];
        while let Some(id) = pending.pop() {
            let Some(row) = self.caps.remove(&id) else {
                continue;
            };
            for dependency in &row.dependencies {
                if let Some(siblings) = self.dependents.get_mut(dependency) {
                    siblings.remove(&id);
                    if siblings.is_empty() {
                        self.dependents.remove(dependency);
                    }
                }
            }
            if let Some(dependents) = self.dependents.remove(&id) {
                pending.extend(dependents);
            }
            if let Some(ActorBinding {
                reuse_key: Some(key),
                ..
            }) = self.actors.remove(&id)
            {
                assert_eq!(self.actor_caps.remove(&key), Some(id));
            }
            if row.kind == Kind::Namespace && self.namespaces.get(&row.issuer) == Some(&id) {
                self.namespaces.remove(&row.issuer);
            }
        }
    }

    pub(super) fn open_namespace(&mut self, namespace_id: Uuid7) -> Namespace {
        if let Some(capability) = self.namespaces.get(&namespace_id).copied() {
            return Namespace {
                namespace_id,
                capability,
            };
        }
        let capability = CapId::new_random();
        self.insert(CapRow {
            id: capability,
            object: format!("namespace {namespace_id}"),
            rights: Rights::all(),
            dependencies: Vec::new(),
            issuer: namespace_id,
            kind: Kind::Namespace,
            depth: 1,
        });
        assert!(self.namespaces.insert(namespace_id, capability).is_none());
        Namespace {
            namespace_id,
            capability,
        }
    }

    pub(super) fn child(
        &self,
        object: String,
        rights: Rights,
        issuer: Uuid7,
        kind: Kind,
        dependencies: impl IntoIterator<Item = CapId>,
    ) -> Result<CapRow, AuthorityError> {
        let mut unique = HashSet::new();
        let mut direct = Vec::new();
        for dependency in dependencies {
            if unique.insert(dependency) {
                if unique.len() >= MAX_CHAIN_NODES {
                    return Err(AuthorityError::Escalation);
                }
                direct.push(dependency);
            }
        }
        let (_, depth) = self.walk(direct.iter().copied(), MAX_CHAIN_NODES - 1)?;
        if depth >= MAX_CHAIN_DEPTH {
            return Err(AuthorityError::Escalation);
        }
        Ok(CapRow {
            id: CapId::new_random(),
            object,
            rights,
            dependencies: direct,
            issuer,
            kind,
            depth: depth + 1,
        })
    }

    fn walk(
        &self,
        roots: impl IntoIterator<Item = CapId>,
        max_nodes: usize,
    ) -> Result<(HashSet<CapId>, usize), AuthorityError> {
        let mut visited = HashSet::new();
        let mut pending = Vec::new();
        for id in roots {
            if visited.insert(id) {
                if visited.len() > max_nodes {
                    return Err(AuthorityError::Escalation);
                }
                pending.push(id);
            }
        }
        let mut depth = 0;
        while let Some(id) = pending.pop() {
            let row = self
                .caps
                .get(&id)
                .ok_or(AuthorityError::InvalidCapability)?;
            if row.depth > MAX_CHAIN_DEPTH {
                return Err(AuthorityError::InvalidCapability);
            }
            depth = depth.max(row.depth);
            for dependency in &row.dependencies {
                if visited.insert(*dependency) {
                    if visited.len() > max_nodes {
                        return Err(AuthorityError::Escalation);
                    }
                    pending.push(*dependency);
                }
            }
        }
        Ok((visited, depth))
    }
}

pub(super) struct Chain<'a> {
    pub(super) cap: &'a CapRow,
    pub(super) ancestors: HashSet<CapId>,
}

impl Chain<'_> {
    pub(super) fn require(&self, right: Rights) -> Result<(), AuthorityError> {
        if self.cap.rights.contains(right) {
            Ok(())
        } else {
            Err(AuthorityError::Unauthorized)
        }
    }

    pub(super) fn namespace(&self, right: Rights) -> Result<Uuid7, AuthorityError> {
        if self.cap.kind != Kind::Namespace {
            return Err(AuthorityError::Unauthorized);
        }
        self.require(right)?;
        Ok(self.cap.issuer)
    }

    pub(super) fn bound(&self, rights: Rights) -> Result<(), AuthorityError> {
        if rights.is_subset_of(self.cap.rights) {
            Ok(())
        } else {
            Err(AuthorityError::Escalation)
        }
    }
}

pub(super) fn walk<'a>(state: &'a State, cap: &CapId) -> Result<Chain<'a>, AuthorityError> {
    let (mut ancestors, _) = state
        .walk([*cap], MAX_CHAIN_NODES)
        .map_err(|error| match error {
            AuthorityError::Escalation => AuthorityError::InvalidCapability,
            error => error,
        })?;
    ancestors.remove(cap);
    Ok(Chain {
        cap: state
            .caps
            .get(cap)
            .expect("the traversal checked the presented capability"),
        ancestors,
    })
}

pub(super) fn may_retract(target: &Chain<'_>, via: &CapId) -> Result<(), AuthorityError> {
    if target.cap.id == *via || target.ancestors.contains(via) {
        Ok(())
    } else {
        Err(AuthorityError::Unauthorized)
    }
}

/// The answer to [`Command::Resolve`]: what a live capability says, and the actor it names
/// if it is an actor capability.
pub(super) struct Resolved {
    pub(super) capability: Capability,
    pub(super) actor: Option<Uuid7>,
}

impl Resolved {
    pub(super) fn require(&self, right: Rights) -> Result<(), AuthorityError> {
        if self.capability.rights.contains(right) {
            Ok(())
        } else {
            Err(AuthorityError::Unauthorized)
        }
    }

    /// The actor this capability presents, provided it carries `right`. Like
    /// [`super::actor::recognize`], a capability that names no actor is refused as such
    /// before its rights are looked at.
    pub(super) fn actor(&self, right: Rights) -> Result<Uuid7, AuthorityError> {
        let actor = self.actor.ok_or(AuthorityError::UnrecognizedActor)?;
        self.require(right)?;
        Ok(actor)
    }
}

type Reply<T> = oneshot::Sender<Result<T, AuthorityError>>;

/// A request to the ledger task, the single owner of [`State`]. The task runs one command
/// at a time to completion, and no command awaits anything while it holds the state, so
/// every command sees the ledger exactly as the previous one left it: a retraction either
/// precedes a mint that depends on it (and the mint fails) or follows it (and deletes it).
pub(super) enum Command {
    OpenNamespace(Uuid7, oneshot::Sender<Namespace>),
    Mint(MintCapability, Reply<Capability>),
    Attenuate(AttenuateCapability, Reply<Capability>),
    Retract(RetractCapability, Reply<()>),
    Resolve(CapId, Reply<Resolved>),
    /// The first half of `OpenActor`: authorize opening a child of `parent` and stage its
    /// capability, before any registry row is written.
    AdmitActor {
        parent: CapId,
        rights: Rights,
        object: String,
        namespace: Namespace,
        reply: Reply<(Uuid7, CapRow)>,
    },
    /// The second half of `OpenActor`, once the registry row is committed: publish the
    /// staged capability, or reuse the one already issued for the same grant.
    BindActor {
        issued: CapRow,
        actor_id: Uuid7,
        parent: CapId,
        reply: Reply<CapId>,
    },
    #[cfg(test)]
    Peek(Box<dyn FnOnce(&State) + Send>),
    /// Stall the ledger until the sender is fired or dropped, so a test can queue commands
    /// behind it in a chosen order.
    #[cfg(test)]
    Hold(oneshot::Receiver<()>),
}

/// Start the ledger task on `tasks`; it stops when the authority's stop token fires. It
/// owns `state` outright and bumps `changes` after every retraction.
pub(super) fn spawn(
    mut state: State,
    changes: watch::Sender<u64>,
    tasks: &Spawner,
) -> mpsc::Sender<Command> {
    let (commands, mut inbox) = mpsc::channel(64);
    let owner = tasks.clone();
    tasks
        .spawn(async move {
            loop {
                let command = tokio::select! {
                    biased;
                    _ = owner.cancelled() => break,
                    command = inbox.recv() => match command {
                        Some(command) => command,
                        None => break,
                    },
                };
                state.handle(command, &changes).await;
            }
        })
        .expect("the ledger starts before its stop token exists outside boot");
    commands
}

/// Send `command` to the ledger and wait for its answer. The authority stops its host,
/// joining every call, before the ledger, so the ledger outlives every caller.
pub(super) async fn ask<T>(
    ledger: &mpsc::Sender<Command>,
    command: impl FnOnce(oneshot::Sender<T>) -> Command,
) -> T {
    let (reply, answer) = oneshot::channel();
    ledger.send(command(reply)).await.expect(ERROR_ACTOR);
    answer.await.expect(ERROR_CHANNEL)
}

/// Run a command for a caller still waiting on `reply`. A caller that gave up before the
/// command's turn gets nothing applied, as if it had been cancelled before calling; one that
/// gives up while the command runs loses only the answer, as if cancelled after it returned.
fn respond<T>(reply: oneshot::Sender<T>, run: impl FnOnce() -> T) {
    if reply.is_closed() {
        return;
    }
    drop(reply.send(run()));
}

impl State {
    async fn handle(&mut self, command: Command, changes: &watch::Sender<u64>) {
        match command {
            Command::OpenNamespace(namespace_id, reply) => {
                respond(reply, || self.open_namespace(namespace_id));
            }
            Command::Mint(request, reply) => respond(reply, || self.mint(request)),
            Command::Attenuate(request, reply) => respond(reply, || self.attenuate(request)),
            Command::Retract(request, reply) => respond(reply, || {
                self.retract_via(&request)?;
                changes.send_modify(|revision| {
                    *revision = revision
                        .checked_add(1)
                        .expect("authority revision overflow");
                });
                Ok(())
            }),
            Command::Resolve(capability, reply) => respond(reply, || self.resolve(&capability)),
            Command::AdmitActor {
                parent,
                rights,
                object,
                namespace,
                reply,
            } => respond(reply, || {
                self.admit_actor(parent, rights, object, namespace)
            }),
            Command::BindActor {
                issued,
                actor_id,
                parent,
                reply,
            } => respond(reply, || self.bind_actor(issued, actor_id, parent)),
            #[cfg(test)]
            Command::Peek(peek) => peek(self),
            #[cfg(test)]
            Command::Hold(release) => drop(release.await),
        }
    }

    fn resolve(&self, capability: &CapId) -> Result<Resolved, AuthorityError> {
        let chain = walk(self, capability)?;
        Ok(Resolved {
            capability: chain.cap.wire(),
            actor: self.actors.get(capability).map(|binding| binding.actor_id),
        })
    }
}
