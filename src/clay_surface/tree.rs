//! The retained surface arena: ingress validation, the `WriteMutations` stack machine, and
//! identity/budget bookkeeping for one mounted surface.
use crate::{
    render::{static_attribute, supported_listener, supported_tag},
    replay::replay,
    wire::{AttributeValue, Mutation, NodeId, PresentationError},
};
use dioxus_core::{BorrowedAttributeValue, ElementId, WriteMutations};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SurfaceNodeId(usize, pub(crate) u64);

impl SurfaceNodeId {
    pub(crate) const ROOT: Self = Self(0, 0);
}

#[derive(Clone, Copy, Debug)]
struct StackEntry {
    node: SurfaceNodeId,
    element_id: Option<ElementId>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SurfaceNodeKind<C> {
    Root,
    Element {
        tag: String,
        namespace: Option<String>,
        attributes: BTreeMap<(String, Option<String>), AttributeValue>,
        listeners: BTreeSet<String>,
        element_id: Option<ElementId>,
    },
    Text(String),
    Component(C),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SurfaceNode<C> {
    generation: u64,
    pub(crate) kind: SurfaceNodeKind<C>,
    parent: Option<SurfaceNodeId>,
    pub(crate) children: Vec<SurfaceNodeId>,
}

/// Retained logical tree for one mounted surface.
///
/// Replaying wire batches into [`SurfaceTree`] executes the `WriteMutations` stack machine against
/// an arena of surface-local nodes so that [`SurfaceTree::render`] can project the subtree into a
/// host `VirtualDom` while translating host events back to the guest's surface-local `ElementId`s.
#[derive(Clone, Debug)]
pub struct SurfaceTree<C = ()> {
    nodes: Vec<Option<SurfaceNode<C>>>,
    vacant_slots: Vec<usize>,
    next_generation: u64,
    stack: Vec<StackEntry>,
    element_to_node: Vec<Option<SurfaceNodeId>>,
    pub(crate) revision: u64,
    retained_bytes: usize,
    failure: Option<PresentationError>,
}

impl<C: Clone> Default for SurfaceTree<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: PartialEq> PartialEq for SurfaceTree<C> {
    fn eq(&self, other: &Self) -> bool {
        self.revision == other.revision
            && self.nodes == other.nodes
            && self.failure == other.failure
    }
}

impl<C: Clone> SurfaceTree<C> {
    pub const MAX_BATCH_MUTATIONS: usize = 4096;
    pub const MAX_BATCH_BYTES: usize = 256 * 1024;
    pub const MAX_STRING_BYTES: usize = 64 * 1024;
    pub const MAX_NODE_SLOTS: usize = 8192;
    pub const MAX_ELEMENT_ID: NodeId = 8191;
    pub const MAX_STACK_DEPTH: usize = 256;
    pub const MAX_TREE_DEPTH: usize = 64;
    pub const MAX_RETAINED_BYTES: usize = 1024 * 1024;
    pub const MAX_ATTRIBUTES_PER_NODE: usize = 64;
    pub const MAX_LISTENERS_PER_NODE: usize = 32;
    pub const MAX_BATCH_WORK: usize = 128 * 1024;

    pub fn new() -> Self {
        Self {
            nodes: vec![Some(SurfaceNode {
                generation: 0,
                kind: SurfaceNodeKind::Root,
                parent: None,
                children: Vec::new(),
            })],
            vacant_slots: Vec::new(),
            next_generation: 1,
            stack: vec![StackEntry {
                node: SurfaceNodeId::ROOT,
                element_id: Some(ElementId::ROOT),
            }],
            element_to_node: vec![Some(SurfaceNodeId::ROOT)],
            revision: 0,
            retained_bytes: 0,
            failure: None,
        }
    }

    pub fn apply(&mut self, batch: &[Mutation<C>]) -> Result<u64, PresentationError> {
        if self.failure.is_some() {
            return Err(PresentationError::Revoked);
        }
        let result = self.apply_batch(batch);
        if let Err(error) = &result {
            self.clear();
            self.failure = Some(error.clone());
        }
        result
    }

    fn apply_batch(&mut self, batch: &[Mutation<C>]) -> Result<u64, PresentationError> {
        if batch.len() > Self::MAX_BATCH_MUTATIONS {
            return Err(PresentationError::Unsupported(
                "batch mutation limit".into(),
            ));
        }
        if self.stack.len() != 1 || self.top().node != SurfaceNodeId::ROOT {
            return Err(PresentationError::InvalidBatch(
                "unbalanced initial stack".into(),
            ));
        }
        let revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| PresentationError::Unsupported("surface revision limit".into()))?;
        let mut bytes = 0;
        let mut work = 0;
        for mutation in batch {
            self.validate_mutation(mutation, &mut bytes, &mut work)?;
            match mutation {
                Mutation::MountComponent(reference) => {
                    let node = self.alloc_node(SurfaceNodeKind::Component(reference.clone()));
                    self.stack.push(StackEntry {
                        node,
                        element_id: None,
                    });
                }
                _ => replay(std::slice::from_ref(mutation), self)?,
            }
            if self
                .stack
                .iter()
                .any(|entry| self.live_node(entry.node).is_none())
            {
                return Err(PresentationError::InvalidBatch(
                    "removed node remains on stack".into(),
                ));
            }
        }
        if self.stack.len() != 1 || self.top().node != SurfaceNodeId::ROOT {
            return Err(PresentationError::InvalidBatch(
                "unbalanced final stack".into(),
            ));
        }
        self.revision = revision;
        Ok(revision)
    }

    fn charge(
        total: &mut usize,
        amount: usize,
        limit: usize,
        reason: &'static str,
    ) -> Result<(), PresentationError> {
        *total = total
            .checked_add(amount)
            .filter(|&next| next <= limit)
            .ok_or_else(|| PresentationError::Unsupported(reason.into()))?;
        Ok(())
    }

    fn charge_string(bytes: &mut usize, value: &str) -> Result<(), PresentationError> {
        if value.len() > Self::MAX_STRING_BYTES {
            return Err(PresentationError::Unsupported("string byte limit".into()));
        }
        Self::charge(
            bytes,
            value.len(),
            Self::MAX_BATCH_BYTES,
            "batch byte limit",
        )
    }

    fn charge_work(work: &mut usize, amount: usize) -> Result<(), PresentationError> {
        Self::charge(work, amount, Self::MAX_BATCH_WORK, "batch work limit")
    }

    fn check_capacity(
        &self,
        nodes: usize,
        removed_nodes: usize,
        bytes: usize,
    ) -> Result<(), PresentationError> {
        let mut slots = self.nodes.len() - self.vacant_slots.len() - removed_nodes;
        Self::charge(&mut slots, nodes, Self::MAX_NODE_SLOTS, "node slot limit")?;
        self.next_generation
            .checked_add(nodes as u64)
            .ok_or_else(|| PresentationError::Unsupported("node generation limit".into()))?;
        let mut retained = self.retained_bytes;
        Self::charge(
            &mut retained,
            bytes,
            Self::MAX_RETAINED_BYTES,
            "retained byte limit",
        )
    }

    fn attribute_bytes(name: &str, namespace: Option<&str>, value: &AttributeValue) -> usize {
        name.len()
            + namespace.map_or(0, str::len)
            + match value {
                AttributeValue::Text(text) => text.len(),
                _ => 0,
            }
    }

    fn kind_bytes(kind: &SurfaceNodeKind<C>) -> usize {
        match kind {
            SurfaceNodeKind::Root | SurfaceNodeKind::Component(_) => 0,
            SurfaceNodeKind::Text(text) => text.len(),
            SurfaceNodeKind::Element {
                tag,
                namespace,
                attributes,
                listeners,
                ..
            } => {
                tag.len()
                    + namespace.as_ref().map_or(0, String::len)
                    + attributes
                        .iter()
                        .map(|((name, ns), value)| {
                            Self::attribute_bytes(name, ns.as_deref(), value)
                        })
                        .sum::<usize>()
                    + listeners.iter().map(String::len).sum::<usize>()
            }
        }
    }

    fn subtree_stats(
        &self,
        id: SurfaceNodeId,
        work: &mut usize,
    ) -> Result<(usize, usize, usize), PresentationError> {
        let node = self.node(id);
        let mut count = 1;
        let mut bytes = Self::kind_bytes(&node.kind);
        Self::charge_work(work, 1 + bytes.div_ceil(64))?;
        let mut height = 1;
        for &child in &node.children {
            let (child_count, child_bytes, child_height) = self.subtree_stats(child, work)?;
            count += child_count;
            bytes += child_bytes;
            height = height.max(child_height + 1);
        }
        Ok((count, bytes, height))
    }

    fn depth(&self, mut id: SurfaceNodeId, work: &mut usize) -> Result<usize, PresentationError> {
        let mut depth = 0;
        while let Some(parent) = self.node(id).parent {
            Self::charge_work(work, 1)?;
            depth += 1;
            id = parent;
        }
        Ok(depth)
    }

    fn validate_attachment(
        &self,
        mutation: &Mutation<C>,
        count: usize,
        work: &mut usize,
    ) -> Result<(), PresentationError> {
        let invalid = || PresentationError::InvalidBatch("invalid attachment or arity".into());
        if count >= self.stack.len() {
            return Err(invalid());
        }
        let split = self.stack.len() - count;
        let anchor = self.stack[split - 1].node;
        let replacing = matches!(mutation, Mutation::ReplaceWith(_));
        let parent = if matches!(mutation, Mutation::AppendChildren(_)) {
            if !matches!(
                self.node(anchor).kind,
                SurfaceNodeKind::Root | SurfaceNodeKind::Element { .. }
            ) {
                return Err(invalid());
            }
            anchor
        } else {
            if anchor == SurfaceNodeId::ROOT || (replacing && split <= 1) {
                return Err(invalid());
            }
            let parent = self.node(anchor).parent.ok_or_else(invalid)?;
            Self::charge_work(work, self.node(parent).children.len())?;
            if !self.node(parent).children.contains(&anchor) {
                return Err(invalid());
            }
            parent
        };
        let selected: BTreeSet<_> = self.stack[split..].iter().map(|entry| entry.node).collect();
        if selected.len() != count || selected.contains(&SurfaceNodeId::ROOT) {
            return Err(invalid());
        }
        let mut ancestor = Some(anchor);
        while let Some(id) = ancestor {
            Self::charge_work(work, 1)?;
            if selected.contains(&id) {
                return Err(invalid());
            }
            ancestor = self.node(id).parent;
        }
        let parent_depth = self.depth(parent, work)?;
        Self::charge_work(work, count)?;
        for &child in &selected {
            let mut ancestor = self.node(child).parent;
            while let Some(id) = ancestor {
                Self::charge_work(work, 1)?;
                if selected.contains(&id) {
                    return Err(invalid());
                }
                ancestor = self.node(id).parent;
            }
            if let Some(old_parent) = self.node(child).parent {
                Self::charge_work(work, self.node(old_parent).children.len())?;
            }
            let (_, _, height) = self.subtree_stats(child, work)?;
            if parent_depth + height > Self::MAX_TREE_DEPTH {
                return Err(PresentationError::Unsupported("tree depth limit".into()));
            }
        }
        if replacing {
            self.subtree_stats(anchor, work)?;
        }
        Ok(())
    }

    fn validate_mutation(
        &self,
        mutation: &Mutation<C>,
        bytes: &mut usize,
        work: &mut usize,
    ) -> Result<(), PresentationError> {
        Self::charge_work(work, self.stack.len() + 1)?;
        Self::charge(
            bytes,
            std::mem::size_of::<Mutation<C>>(),
            Self::MAX_BATCH_BYTES,
            "batch byte limit",
        )?;
        let invalid = || PresentationError::InvalidBatch("invalid mutation target or stack".into());
        let top = self.top().node;
        let node = self.node(top);
        if matches!(
            mutation,
            Mutation::PushId(_)
                | Mutation::CreateElement(_)
                | Mutation::CreateText(_)
                | Mutation::MountComponent(_)
        ) && self.stack.len() >= Self::MAX_STACK_DEPTH
        {
            return Err(PresentationError::Unsupported("stack depth limit".into()));
        }
        match mutation {
            Mutation::PushId(id) | Mutation::SetId(id) => {
                if *id > Self::MAX_ELEMENT_ID {
                    return Err(PresentationError::Unsupported("element id limit".into()));
                }
                if matches!(mutation, Mutation::PushId(_)) {
                    if self
                        .element_to_node
                        .get(*id as usize)
                        .copied()
                        .flatten()
                        .and_then(|node| self.live_node(node))
                        .is_none()
                    {
                        return Err(invalid());
                    }
                } else if (*id == 0) != (top == SurfaceNodeId::ROOT) {
                    return Err(invalid());
                }
            }
            Mutation::Child(index) => {
                if self.stack.len() <= 1 || *index as usize >= node.children.len() {
                    return Err(invalid());
                }
            }
            Mutation::Pop => {
                if self.stack.len() <= 1 {
                    return Err(invalid());
                }
            }
            Mutation::CreateElement((tag, ns)) => {
                Self::charge_string(bytes, tag)?;
                if let Some(ns) = ns {
                    Self::charge_string(bytes, ns)?;
                }
                if !supported_tag(tag, ns.as_deref()) {
                    return Err(PresentationError::Unsupported(
                        "element tag or namespace".into(),
                    ));
                }
                self.check_capacity(1, 0, tag.len() + ns.as_ref().map_or(0, String::len))?;
            }
            Mutation::CreateText(text) => {
                Self::charge_string(bytes, text)?;
                self.check_capacity(1, 0, text.len())?;
            }
            Mutation::MountComponent(_) => self.check_capacity(1, 0, 0)?,
            Mutation::CloneNode => {
                if top == SurfaceNodeId::ROOT || self.stack.len() <= 1 {
                    return Err(invalid());
                }
                let (count, bytes, _) = self.subtree_stats(top, work)?;
                self.check_capacity(count, 0, bytes)?;
            }
            Mutation::AppendChildren(count)
            | Mutation::ReplaceWith(count)
            | Mutation::InsertAfter(count)
            | Mutation::InsertBefore(count) => {
                self.validate_attachment(mutation, *count as usize, work)?;
            }
            Mutation::SetAttribute((name, ns, value)) => {
                Self::charge_string(bytes, name)?;
                if let Some(ns) = ns {
                    Self::charge_string(bytes, ns)?;
                }
                if let AttributeValue::Text(text) = value {
                    Self::charge_string(bytes, text)?;
                }
                if static_attribute(name, ns.as_deref()).is_none() {
                    return Err(PresentationError::Unsupported(
                        "attribute name or namespace".into(),
                    ));
                }
                if matches!(value, AttributeValue::Float(float) if !float.is_finite()) {
                    return Err(PresentationError::Unsupported(
                        "non-finite attribute".into(),
                    ));
                }
                let SurfaceNodeKind::Element { attributes, .. } = &node.kind else {
                    return Err(invalid());
                };
                Self::charge_work(work, attributes.len())?;
                let existing = attributes
                    .iter()
                    .find(|((old_name, old_ns), _)| old_name == name && old_ns == ns);
                if !matches!(value, AttributeValue::None | AttributeValue::Boolean(false)) {
                    if existing.is_none() && attributes.len() >= Self::MAX_ATTRIBUTES_PER_NODE {
                        return Err(PresentationError::Unsupported(
                            "attribute count limit".into(),
                        ));
                    }
                    let previous = existing.map_or(0, |(_, value)| {
                        Self::attribute_bytes(name, ns.as_deref(), value)
                    });
                    self.check_capacity(
                        0,
                        0,
                        Self::attribute_bytes(name, ns.as_deref(), value).saturating_sub(previous),
                    )?;
                }
            }
            Mutation::SetText(text) => {
                Self::charge_string(bytes, text)?;
                match &node.kind {
                    SurfaceNodeKind::Root | SurfaceNodeKind::Component(_) => return Err(invalid()),
                    SurfaceNodeKind::Text(previous) => {
                        self.check_capacity(0, 0, text.len().saturating_sub(previous.len()))?
                    }
                    SurfaceNodeKind::Element { .. } => {
                        let mut previous_nodes = 0;
                        let mut previous_bytes = 0;
                        for &child in &node.children {
                            let (count, bytes, _) = self.subtree_stats(child, work)?;
                            previous_nodes += count;
                            previous_bytes += bytes;
                        }
                        self.check_capacity(
                            1,
                            previous_nodes,
                            text.len().saturating_sub(previous_bytes),
                        )?;
                        if self.depth(top, work)? + 1 > Self::MAX_TREE_DEPTH {
                            return Err(PresentationError::Unsupported("tree depth limit".into()));
                        }
                    }
                }
            }
            Mutation::AddEventListener(name) | Mutation::RemoveEventListener(name) => {
                Self::charge_string(bytes, name)?;
                if !supported_listener(name) {
                    return Err(PresentationError::Unsupported("event listener".into()));
                }
                let SurfaceNodeKind::Element {
                    listeners,
                    element_id,
                    ..
                } = &node.kind
                else {
                    return Err(invalid());
                };
                if matches!(mutation, Mutation::AddEventListener(_)) {
                    let id = element_id.ok_or_else(invalid)?;
                    if self.element_to_node.get(id.raw()).copied().flatten() != Some(top) {
                        return Err(invalid());
                    }
                    if !listeners.contains(name) {
                        if listeners.len() >= Self::MAX_LISTENERS_PER_NODE {
                            return Err(PresentationError::Unsupported(
                                "listener count limit".into(),
                            ));
                        }
                        self.check_capacity(0, 0, name.len())?;
                    }
                }
            }
            Mutation::Remove => {
                if top == SurfaceNodeId::ROOT || self.stack.len() <= 1 {
                    return Err(invalid());
                }
                self.subtree_stats(top, work)?;
                if let Some(parent) = node.parent {
                    Self::charge_work(work, self.node(parent).children.len())?;
                }
            }
        }
        Ok(())
    }

    pub fn error(&self) -> Option<&PresentationError> {
        self.failure.as_ref()
    }

    pub fn clear(&mut self) {
        let revision = self.revision.saturating_add(1);
        let failure = self.failure.take();
        let next_generation = self.next_generation;
        *self = Self::new();
        self.revision = revision;
        self.next_generation = next_generation;
        self.failure = failure;
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn is_empty(&self) -> bool {
        self.node(SurfaceNodeId::ROOT).children.is_empty()
    }

    pub fn text_content(&self) -> String {
        let mut out = String::new();
        self.collect_text(SurfaceNodeId::ROOT, &mut out);
        out
    }

    pub fn listener_id(&self, tag: &str, event: &str) -> Option<ElementId> {
        fn resolve<C: Clone>(
            tree: &SurfaceTree<C>,
            id: SurfaceNodeId,
            tag: &str,
            event: &str,
        ) -> Option<ElementId> {
            let node = tree.node(id);
            if let SurfaceNodeKind::Element {
                tag: node_tag,
                listeners,
                element_id: Some(element_id),
                ..
            } = &node.kind
                && node_tag == tag
                && listeners.contains(event)
            {
                return Some(*element_id);
            }
            node.children
                .iter()
                .find_map(|&child| resolve(tree, child, tag, event))
        }

        resolve(self, SurfaceNodeId::ROOT, tag, event)
    }

    fn collect_text(&self, id: SurfaceNodeId, out: &mut String) {
        let node = self.node(id);
        if let SurfaceNodeKind::Text(text) = &node.kind {
            out.push_str(text);
        }
        for &child in &node.children {
            self.collect_text(child, out);
        }
    }

    fn alloc_node(&mut self, kind: SurfaceNodeKind<C>) -> SurfaceNodeId {
        let index = self.vacant_slots.pop().unwrap_or(self.nodes.len());
        let id = SurfaceNodeId(index, self.next_generation);
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .expect("validated node generation");
        self.retained_bytes += Self::kind_bytes(&kind);
        let node = Some(SurfaceNode {
            generation: id.1,
            kind,
            parent: None,
            children: Vec::new(),
        });
        if index == self.nodes.len() {
            self.nodes.push(node);
        } else {
            self.nodes[index] = node;
        }
        id
    }

    fn live_node(&self, id: SurfaceNodeId) -> Option<&SurfaceNode<C>> {
        self.nodes
            .get(id.0)
            .and_then(Option::as_ref)
            .filter(|node| node.generation == id.1)
    }

    pub(crate) fn node(&self, id: SurfaceNodeId) -> &SurfaceNode<C> {
        self.live_node(id)
            .unwrap_or_else(|| panic!("surface node {id:?} does not exist"))
    }

    fn node_mut(&mut self, id: SurfaceNodeId) -> &mut SurfaceNode<C> {
        self.nodes
            .get_mut(id.0)
            .and_then(Option::as_mut)
            .filter(|node| node.generation == id.1)
            .unwrap_or_else(|| panic!("surface node {id:?} does not exist"))
    }

    fn lookup(&self, id: ElementId) -> SurfaceNodeId {
        self.element_to_node
            .get(id.raw())
            .copied()
            .flatten()
            .unwrap_or_else(|| panic!("surface asked for unknown ElementId {}", id.raw()))
    }

    fn set_mapping(&mut self, id: ElementId, node: SurfaceNodeId) {
        let index = id.raw();
        if self.element_to_node.len() <= index {
            self.element_to_node.resize(index + 1, None);
        }
        if let Some(previous) = self.element_to_node[index]
            && previous != node
            && let Some(SurfaceNode {
                kind: SurfaceNodeKind::Element { element_id, .. },
                generation,
                ..
            }) = self.nodes.get_mut(previous.0).and_then(Option::as_mut)
            && *generation == previous.1
            && *element_id == Some(id)
        {
            *element_id = None;
        }
        self.element_to_node[index] = Some(node);
        if let SurfaceNodeKind::Element { element_id, .. } = &mut self.node_mut(node).kind {
            *element_id = Some(id);
        }
    }

    fn top(&self) -> StackEntry {
        *self.stack.last().expect("surface stack unexpectedly empty")
    }

    fn pop_entry(&mut self) -> StackEntry {
        self.stack.pop().expect("surface stack unexpectedly empty")
    }

    fn pop_nodes(&mut self, m: usize) -> Vec<SurfaceNodeId> {
        let split = self.stack.len() - m;
        self.stack
            .split_off(split)
            .into_iter()
            .map(|entry| entry.node)
            .collect()
    }

    fn detach_from_parent(&mut self, child: SurfaceNodeId) {
        if let Some(parent) = self.node_mut(child).parent.take()
            && let Some(parent_node) = self
                .nodes
                .get_mut(parent.0)
                .and_then(Option::as_mut)
                .filter(|node| node.generation == parent.1)
        {
            parent_node.children.retain(|&existing| existing != child);
        }
    }

    fn drop_subtree(&mut self, root: SurfaceNodeId) {
        let Some(slot) = self
            .nodes
            .get_mut(root.0)
            .filter(|slot| slot.as_ref().is_some_and(|node| node.generation == root.1))
        else {
            return;
        };
        let node = slot.take().unwrap();
        self.retained_bytes -= Self::kind_bytes(&node.kind);
        if let SurfaceNodeKind::Element {
            element_id: Some(id),
            ..
        } = node.kind
            && let Some(slot) = self.element_to_node.get_mut(id.raw())
            && *slot == Some(root)
        {
            *slot = None;
        }
        for child in node.children {
            self.drop_subtree(child);
        }
        self.vacant_slots.push(root.0);
    }

    fn deep_clone(&mut self, source: SurfaceNodeId) -> SurfaceNodeId {
        let (kind, children) = {
            let node = self.node(source);
            let kind = match &node.kind {
                SurfaceNodeKind::Root => panic!("cannot clone surface root"),
                SurfaceNodeKind::Element {
                    tag,
                    namespace,
                    attributes,
                    ..
                } => SurfaceNodeKind::Element {
                    tag: tag.clone(),
                    namespace: namespace.clone(),
                    attributes: attributes.clone(),
                    listeners: BTreeSet::new(),
                    element_id: None,
                },
                SurfaceNodeKind::Text(text) => SurfaceNodeKind::Text(text.clone()),
                SurfaceNodeKind::Component(reference) => {
                    SurfaceNodeKind::Component(reference.clone())
                }
            };
            (kind, node.children.clone())
        };
        let cloned = self.alloc_node(kind);
        let cloned_children: Vec<SurfaceNodeId> =
            children.into_iter().map(|c| self.deep_clone(c)).collect();
        for &child in &cloned_children {
            self.node_mut(child).parent = Some(cloned);
        }
        self.node_mut(cloned).children = cloned_children;
        cloned
    }
}

impl<C: Clone> WriteMutations for SurfaceTree<C> {
    fn push_id(&mut self, id: ElementId) {
        let node = self.lookup(id);
        self.stack.push(StackEntry {
            node,
            element_id: Some(id),
        });
    }

    fn set_id(&mut self, id: ElementId) {
        let node = self.top().node;
        self.set_mapping(id, node);
        if let Some(top) = self.stack.last_mut() {
            top.element_id = Some(id);
        }
    }

    fn child(&mut self, index: usize) {
        let parent = self.top().node;
        let child = self.node(parent).children[index];
        let element_id = match &self.node(child).kind {
            SurfaceNodeKind::Element { element_id, .. } => *element_id,
            _ => None,
        };
        *self
            .stack
            .last_mut()
            .expect("surface stack unexpectedly empty") = StackEntry {
            node: child,
            element_id,
        };
    }

    fn pop(&mut self) {
        self.pop_entry();
    }

    fn create_element(&mut self, tag: &str, ns: Option<&str>) {
        let node = self.alloc_node(SurfaceNodeKind::Element {
            tag: tag.to_owned(),
            namespace: ns.map(str::to_owned),
            attributes: BTreeMap::new(),
            listeners: BTreeSet::new(),
            element_id: None,
        });
        self.stack.push(StackEntry {
            node,
            element_id: None,
        });
    }

    fn create_text(&mut self, value: &str) {
        let node = self.alloc_node(SurfaceNodeKind::Text(value.to_owned()));
        self.stack.push(StackEntry {
            node,
            element_id: None,
        });
    }

    fn clone(&mut self) {
        let source = self.top().node;
        let cloned = self.deep_clone(source);
        *self
            .stack
            .last_mut()
            .expect("surface stack unexpectedly empty") = StackEntry {
            node: cloned,
            element_id: None,
        };
    }

    fn append_children(&mut self, m: usize) {
        let children = self.pop_nodes(m);
        let parent = self.top().node;
        for &child in &children {
            self.detach_from_parent(child);
            self.node_mut(child).parent = Some(parent);
        }
        self.node_mut(parent).children.extend(children);
    }

    fn replace_with(&mut self, m: usize) {
        let replacements = self.pop_nodes(m);
        let target = self.pop_entry();
        for &node in &replacements {
            self.detach_from_parent(node);
        }
        let parent = self
            .node(target.node)
            .parent
            .expect("replaced surface node must have a parent");
        for &node in &replacements {
            self.node_mut(node).parent = Some(parent);
        }
        let siblings = &mut self.node_mut(parent).children;
        let pos = siblings
            .iter()
            .position(|&existing| existing == target.node)
            .expect("target node must be in parent's children");
        siblings.splice(pos..=pos, replacements);
        self.drop_subtree(target.node);
    }

    fn insert_after(&mut self, m: usize) {
        let nodes = self.pop_nodes(m);
        let anchor = self.top().node;
        for &node in &nodes {
            self.detach_from_parent(node);
        }
        let parent = self
            .node(anchor)
            .parent
            .expect("anchor surface node must have a parent");
        for &node in &nodes {
            self.node_mut(node).parent = Some(parent);
        }
        let siblings = &mut self.node_mut(parent).children;
        let pos = siblings
            .iter()
            .position(|&existing| existing == anchor)
            .expect("anchor node must be in parent's children");
        siblings.splice(pos + 1..pos + 1, nodes);
    }

    fn insert_before(&mut self, m: usize) {
        let nodes = self.pop_nodes(m);
        let anchor = self.top().node;
        for &node in &nodes {
            self.detach_from_parent(node);
        }
        let parent = self
            .node(anchor)
            .parent
            .expect("anchor surface node must have a parent");
        for &node in &nodes {
            self.node_mut(node).parent = Some(parent);
        }
        let siblings = &mut self.node_mut(parent).children;
        let pos = siblings
            .iter()
            .position(|&existing| existing == anchor)
            .expect("anchor node must be in parent's children");
        siblings.splice(pos..pos, nodes);
    }

    fn set_attribute(&mut self, name: &str, ns: Option<&str>, value: BorrowedAttributeValue<'_>) {
        let node = self.top().node;
        let SurfaceNodeKind::Element { attributes, .. } = &mut self.node_mut(node).kind else {
            panic!("set_attribute called on non-element surface node");
        };
        let key = (name.to_owned(), ns.map(str::to_owned));
        let previous_bytes = attributes
            .get(&key)
            .map_or(0, |value| Self::attribute_bytes(name, ns, value));
        match value {
            BorrowedAttributeValue::None | BorrowedAttributeValue::Bool(false) => {
                attributes.remove(&key);
            }
            BorrowedAttributeValue::Text(text) => {
                attributes.insert(key, AttributeValue::Text(text.to_owned()));
            }
            BorrowedAttributeValue::Float(float) => {
                attributes.insert(key, AttributeValue::Float(float));
            }
            BorrowedAttributeValue::Int(int) => {
                attributes.insert(key, AttributeValue::Int(int));
            }
            BorrowedAttributeValue::Bool(true) => {
                attributes.insert(key, AttributeValue::Boolean(true));
            }
            BorrowedAttributeValue::Any(_) => {
                panic!("`Any` attribute `{name}` cannot cross the presentation boundary")
            }
        }
        let SurfaceNodeKind::Element { attributes, .. } = &self.node(node).kind else {
            unreachable!()
        };
        let next_bytes = attributes
            .iter()
            .find(|((key_name, key_ns), _)| key_name == name && key_ns.as_deref() == ns)
            .map_or(0, |(_, value)| Self::attribute_bytes(name, ns, value));
        self.retained_bytes = self.retained_bytes - previous_bytes + next_bytes;
    }

    fn set_text(&mut self, value: &str) {
        let node = self.top().node;
        match &mut self.node_mut(node).kind {
            SurfaceNodeKind::Text(text) => {
                let previous_bytes = text.len();
                *text = value.to_owned();
                self.retained_bytes = self.retained_bytes - previous_bytes + value.len();
            }
            SurfaceNodeKind::Element { .. } => {
                let children = self.node(node).children.clone();
                for child in children {
                    self.drop_subtree(child);
                }
                let text_node = self.alloc_node(SurfaceNodeKind::Text(value.to_owned()));
                self.node_mut(text_node).parent = Some(node);
                self.node_mut(node).children = vec![text_node];
            }
            SurfaceNodeKind::Root | SurfaceNodeKind::Component(_) => {
                panic!("set_text called on a non-DOM surface node")
            }
        }
    }

    fn add_event_listener(&mut self, name: &str) {
        let entry = self.top();
        let element_id = entry
            .element_id
            .filter(|id| self.element_to_node.get(id.raw()).copied().flatten() == Some(entry.node))
            .or_else(|| match &self.node(entry.node).kind {
                SurfaceNodeKind::Element { element_id, .. } => *element_id,
                _ => None,
            })
            .expect("event listener target must be mapped to an ElementId");
        self.set_mapping(element_id, entry.node);
        let SurfaceNodeKind::Element { listeners, .. } = &mut self.node_mut(entry.node).kind else {
            panic!("add_event_listener called on non-element surface node");
        };
        if listeners.insert(name.to_owned()) {
            self.retained_bytes += name.len();
        }
    }

    fn remove_event_listener(&mut self, name: &str) {
        let node = self.top().node;
        if let SurfaceNodeKind::Element { listeners, .. } = &mut self.node_mut(node).kind
            && listeners.remove(name)
        {
            self.retained_bytes -= name.len();
        }
    }

    fn remove(&mut self) {
        let entry = self.pop_entry();
        self.detach_from_parent(entry.node);
        self.drop_subtree(entry.node);
    }
}

#[cfg(test)]
#[path = "tree_tests.rs"]
mod tests;
