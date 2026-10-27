//! The retained surface arena: ingress validation, the `WriteMutations` stack machine, and
//! identity/budget bookkeeping for one mounted surface.
use crate::render::static_attribute;
use crate::render::supported_listener;
use crate::render::supported_tag;
use crate::replay::replay;
use crate::wire::AttributeValue;
use crate::wire::Mutation;
use crate::wire::NodeId;
use crate::wire::PresentationError;
use dioxus_core::BorrowedAttributeValue;
use dioxus_core::ElementId;
use dioxus_core::WriteMutations;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

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

mod validate;
mod write;

#[cfg(test)]
mod tests;
