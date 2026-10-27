use super::AuthorityError;
use super::CapId;
use super::Capability;
use super::Namespace;
use super::Rights;
use big_cabinet_core::dmeta::Uuid7;
use std::collections::HashMap;
use std::collections::HashSet;

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
