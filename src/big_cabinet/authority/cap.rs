use super::{AuthorityError, CapId, Capability, Namespace, Rights};
use big_cabinet_core::dmeta::Uuid7;
use std::collections::{HashMap, HashSet};

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
    pub(super) retracted: bool,
    depth: usize,
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

#[derive(Default)]
pub(super) struct State {
    pub(super) caps: HashMap<CapId, CapRow>,
    pub(super) actors: HashMap<CapId, Uuid7>,
    pub(super) actor_caps: HashMap<(Uuid7, CapId, i64), CapId>,
    namespaces: HashMap<Uuid7, CapId>,
}

impl State {
    pub(super) fn insert(&mut self, cap: CapRow) {
        assert!(
            self.caps.insert(cap.id, cap).is_none(),
            "capability id collision"
        );
    }

    pub(super) fn open_namespace(&mut self, namespace_id: Uuid7) -> Namespace {
        let previous = self.namespaces.get(&namespace_id).copied();
        if let Some(capability) = previous
            && !self.caps[&capability].retracted
        {
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
            retracted: false,
            depth: 1,
        });
        assert_eq!(self.namespaces.insert(namespace_id, capability), previous);
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
            retracted: false,
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
            if row.retracted || row.depth > MAX_CHAIN_DEPTH {
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
