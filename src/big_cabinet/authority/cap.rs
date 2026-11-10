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
    /// The actor this handle is bound to; see [`Capability::subject`]. Namespace roots have none.
    pub(super) subject: Option<Uuid7>,
    depth: usize,
}

/// The actor an actor capability names, with the `actor_caps` key that reuses it (the
/// operator's root has none), so retracting the capability drops both entries without a scan.
pub(super) struct ActorBinding {
    pub(super) actor_id: Uuid7,
    /// The actor this one was opened under; the operator's root has none.
    pub(super) parent_actor: Option<Uuid7>,
    pub(super) reuse_key: Option<(Uuid7, CapId, i64)>,
}

impl CapRow {
    pub(super) fn wire(&self) -> Capability {
        Capability {
            capability_id: self.id,
            object: self.object.clone(),
            rights: self.rights,
            issuer_id: self.issuer,
            subject: self.subject,
        }
    }
}

/// An actor's liveness capability: an internal row, never returned on the wire, that exists
/// while at least one of the actor's identity capabilities is live. `OpenActor` reuses one
/// identity capability per parent and rights, so an actor can hold several, and a handle
/// introduced to the actor cannot depend on "the" identity capability. It depends on this
/// row instead, which the last identity capability's retraction takes down with it.
struct Liveness {
    cap: CapId,
    identities: usize,
    /// The actor's parent in the actor tree, recorded when its first identity capability is
    /// bound (a path names its parent, so every later binding agrees). An introduction walks
    /// these to decide whether one actor is a descendant of another. A live child implies
    /// live ancestors, because its identity capability depends on its parent's.
    parent: Option<Uuid7>,
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
    liveness: HashMap<Uuid7, Liveness>,
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
            if let Some(binding) = self.actors.remove(&id) {
                if let Some(key) = binding.reuse_key {
                    assert_eq!(self.actor_caps.remove(&key), Some(id));
                }
                let liveness = self
                    .liveness
                    .get_mut(&binding.actor_id)
                    .expect("a live identity capability keeps its actor's liveness row");
                liveness.identities -= 1;
                if liveness.identities == 0 {
                    // The last identity capability is gone: retract the liveness row in this
                    // same pass, which takes every handle introduced to the actor with it.
                    pending.push(liveness.cap);
                    self.liveness.remove(&binding.actor_id);
                }
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
            subject: None,
            depth: 1,
        });
        assert!(self.namespaces.insert(namespace_id, capability).is_none());
        Namespace {
            namespace_id,
            capability,
        }
    }

    /// Publish `cap` as the identity capability of `binding.actor_id`, bound to that actor,
    /// and keep the actor's liveness row: created with its first identity capability, and
    /// retracted by [`Self::retract`] with the last.
    pub(super) fn bind_identity(&mut self, mut cap: CapRow, binding: ActorBinding) {
        let actor_id = binding.actor_id;
        let parent = binding.parent_actor;
        cap.subject = Some(actor_id);
        let id = cap.id;
        self.insert(cap);
        assert!(self.actors.insert(id, binding).is_none());
        if let Some(liveness) = self.liveness.get_mut(&actor_id) {
            liveness.identities += 1;
            return;
        }
        let liveness = CapRow {
            id: CapId::new_random(),
            object: format!("actor liveness {actor_id}"),
            rights: Rights::empty(),
            dependencies: Vec::new(),
            issuer: actor_id,
            kind: Kind::Object,
            subject: Some(actor_id),
            depth: 1,
        };
        let cap = liveness.id;
        self.insert(liveness);
        self.liveness.insert(
            actor_id,
            Liveness {
                cap,
                identities: 1,
                parent,
            },
        );
    }

    /// Whether `actor` is a strict descendant of `ancestor` in the actor tree, walking parents
    /// at most [`MAX_CHAIN_DEPTH`] steps. An actor is not its own descendant.
    pub(super) fn is_descendant(&self, actor: Uuid7, ancestor: Uuid7) -> bool {
        let mut current = actor;
        for _ in 0..MAX_CHAIN_DEPTH {
            let Some(parent) = self
                .liveness
                .get(&current)
                .and_then(|liveness| liveness.parent)
            else {
                return false;
            };
            if parent == ancestor {
                return true;
            }
            current = parent;
        }
        false
    }

    /// The liveness capability of `actor`, or `ActorNotFound` when it has no live identity
    /// capability (never opened, or every one retracted).
    pub(super) fn liveness_of(&self, actor: Uuid7) -> Result<CapId, AuthorityError> {
        self.liveness
            .get(&actor)
            .map(|liveness| liveness.cap)
            .ok_or(AuthorityError::ActorNotFound)
    }

    pub(super) fn child(
        &self,
        object: String,
        rights: Rights,
        issuer: Uuid7,
        kind: Kind,
        subject: Option<Uuid7>,
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
            subject,
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
/// if it is an identity capability (a handle's `subject` is the actor it is bound to, which an
/// identity capability shares with every handle derived from it).
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

    /// The actor this handle is bound to, provided it carries `right`. Like
    /// [`super::actor::recognize`], a handle bound to no actor is refused as such before its
    /// rights are looked at.
    pub(super) fn subject(&self, right: Rights) -> Result<Uuid7, AuthorityError> {
        let subject = self
            .capability
            .subject
            .ok_or(AuthorityError::UnrecognizedActor)?;
        self.require(right)?;
        Ok(subject)
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
        parent_actor: Uuid7,
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
                parent_actor,
                parent,
                reply,
            } => respond(reply, || {
                self.bind_actor(issued, actor_id, parent_actor, parent)
            }),
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
