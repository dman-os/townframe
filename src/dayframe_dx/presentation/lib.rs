//! Engine-neutral presentation wire, `WriteMutations` replay, and retained surface tree.
//!
//! The WIT `presentation` contract ships ordered batches of [`Mutation`] operations that mirror
//! `dioxus_core::WriteMutations` one-to-one. A renderer backend is any `WriteMutations`
//! implementation plus event translation:
//! - Direct Blitz documents replay batches into `dioxus_native_dom::DioxusState::writer`.
//! - Host-composed surfaces replay batches into [`SurfaceTree`], which renders into the host
//!   Dioxus `VirtualDom` through `ComponentMount` without `ElementId` collisions
//!   and withdraws its subtree when its host `Mount` or `Instance` closes.

use dioxus::prelude::*;
use dioxus_core::{BorrowedAttributeValue, ElementId, ListenerCallback, WriteMutations};
use dioxus_html::PlatformEventData;
use std::{
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};

pub type NodeId = u32;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PresentationError {
    #[error("presentation surface or mount point has been revoked")]
    Revoked,
    #[error("unsupported presentation operation: {0}")]
    Unsupported(String),
    #[error("invalid presentation mutation batch: {0}")]
    InvalidBatch(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum AttributeValue {
    Text(String),
    Float(f64),
    Int(i64),
    Boolean(bool),
    None,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Mutation {
    PushId(NodeId),
    SetId(NodeId),
    Child(u32),
    Pop,
    CreateElement((String, Option<String>)),
    CreateText(String),
    CloneNode,
    AppendChildren(u32),
    ReplaceWith(u32),
    InsertAfter(u32),
    InsertBefore(u32),
    SetAttribute((String, Option<String>, AttributeValue)),
    SetText(String),
    AddEventListener(String),
    RemoveEventListener(String),
    Remove,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub revision: u64,
    pub element: NodeId,
    pub name: String,
    pub bubbles: bool,
    pub data_json: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventResponse {
    pub prevent_default: bool,
    pub stop_propagation: bool,
}

pub type SurfaceEventHandler = Rc<dyn Fn(Event) -> EventResponse>;

/// Replays one wire batch into any `dioxus_core::WriteMutations` writer in order.
pub fn replay(batch: &[Mutation], to: &mut impl WriteMutations) {
    let element = |id: NodeId| ElementId::from_raw(id as usize);
    for mutation in batch {
        match mutation {
            Mutation::PushId(id) => to.push_id(element(*id)),
            Mutation::SetId(id) => to.set_id(element(*id)),
            Mutation::Child(index) => to.child(*index as usize),
            Mutation::Pop => to.pop(),
            Mutation::CreateElement((tag, ns)) => to.create_element(tag, ns.as_deref()),
            Mutation::CreateText(value) => to.create_text(value),
            Mutation::CloneNode => to.clone(),
            Mutation::AppendChildren(m) => to.append_children(*m as usize),
            Mutation::ReplaceWith(m) => to.replace_with(*m as usize),
            Mutation::InsertAfter(m) => to.insert_after(*m as usize),
            Mutation::InsertBefore(m) => to.insert_before(*m as usize),
            Mutation::SetAttribute((name, ns, value)) => {
                let value = match value {
                    AttributeValue::Text(text) => BorrowedAttributeValue::Text(text),
                    AttributeValue::Float(float) => BorrowedAttributeValue::Float(*float),
                    AttributeValue::Int(int) => BorrowedAttributeValue::Int(*int),
                    AttributeValue::Boolean(bool) => BorrowedAttributeValue::Bool(*bool),
                    AttributeValue::None => BorrowedAttributeValue::None,
                };
                to.set_attribute(name, ns.as_deref(), value);
            }
            Mutation::SetText(value) => to.set_text(value),
            Mutation::AddEventListener(name) => to.add_event_listener(name),
            Mutation::RemoveEventListener(name) => to.remove_event_listener(name),
            Mutation::Remove => to.remove(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SurfaceNodeId(usize, u64);

impl SurfaceNodeId {
    const ROOT: Self = Self(0, 0);
}

#[derive(Clone, Copy, Debug)]
struct StackEntry {
    node: SurfaceNodeId,
    element_id: Option<ElementId>,
}

#[derive(Clone, Debug, PartialEq)]
enum SurfaceNodeKind {
    Root,
    Element {
        tag: String,
        namespace: Option<String>,
        attributes: BTreeMap<(String, Option<String>), AttributeValue>,
        listeners: BTreeSet<String>,
        element_id: Option<ElementId>,
    },
    Text(String),
}

#[derive(Clone, Debug, PartialEq)]
struct SurfaceNode {
    generation: u64,
    kind: SurfaceNodeKind,
    parent: Option<SurfaceNodeId>,
    children: Vec<SurfaceNodeId>,
}

/// Retained logical tree for one mounted surface.
///
/// Replaying wire batches into [`SurfaceTree`] executes the `WriteMutations` stack machine against
/// an arena of surface-local nodes so that [`SurfaceTree::render`] can project the subtree into a
/// host `VirtualDom` while translating host events back to the guest's surface-local `ElementId`s.
#[derive(Clone, Debug)]
pub struct SurfaceTree {
    nodes: Vec<Option<SurfaceNode>>,
    vacant_slots: Vec<usize>,
    next_generation: u64,
    stack: Vec<StackEntry>,
    element_to_node: Vec<Option<SurfaceNodeId>>,
    revision: u64,
    retained_bytes: usize,
    failure: Option<PresentationError>,
}

impl Default for SurfaceTree {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for SurfaceTree {
    fn eq(&self, other: &Self) -> bool {
        self.revision == other.revision
            && self.nodes == other.nodes
            && self.failure == other.failure
    }
}

impl SurfaceTree {
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

    pub fn apply(&mut self, batch: &[Mutation]) -> Result<u64, PresentationError> {
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

    fn apply_batch(&mut self, batch: &[Mutation]) -> Result<u64, PresentationError> {
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
            replay(std::slice::from_ref(mutation), self);
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

    fn kind_bytes(kind: &SurfaceNodeKind) -> usize {
        match kind {
            SurfaceNodeKind::Root => 0,
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
        mutation: &Mutation,
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
        mutation: &Mutation,
        bytes: &mut usize,
        work: &mut usize,
    ) -> Result<(), PresentationError> {
        Self::charge_work(work, self.stack.len() + 1)?;
        Self::charge(
            bytes,
            std::mem::size_of::<Mutation>(),
            Self::MAX_BATCH_BYTES,
            "batch byte limit",
        )?;
        let invalid = || PresentationError::InvalidBatch("invalid mutation target or stack".into());
        let top = self.top().node;
        let node = self.node(top);
        if matches!(
            mutation,
            Mutation::PushId(_) | Mutation::CreateElement(_) | Mutation::CreateText(_)
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
                    SurfaceNodeKind::Root => return Err(invalid()),
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
        fn resolve(
            tree: &SurfaceTree,
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

    fn alloc_node(&mut self, kind: SurfaceNodeKind) -> SurfaceNodeId {
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

    fn live_node(&self, id: SurfaceNodeId) -> Option<&SurfaceNode> {
        self.nodes
            .get(id.0)
            .and_then(Option::as_ref)
            .filter(|node| node.generation == id.1)
    }

    fn node(&self, id: SurfaceNodeId) -> &SurfaceNode {
        self.live_node(id)
            .unwrap_or_else(|| panic!("surface node {id:?} does not exist"))
    }

    fn node_mut(&mut self, id: SurfaceNodeId) -> &mut SurfaceNode {
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

    pub fn render(&self, dispatch: &SurfaceEventHandler) -> Element {
        let root_children = &self.node(SurfaceNodeId::ROOT).children;
        rsx! {
            for &child in root_children {
                {self.render_node(child, dispatch)}
            }
        }
    }

    fn render_node(&self, id: SurfaceNodeId, dispatch: &SurfaceEventHandler) -> Element {
        let node = self.node(id);
        match &node.kind {
            SurfaceNodeKind::Root => unreachable!("root is never a child node"),
            SurfaceNodeKind::Text(text) => rsx! { "{text}" },
            SurfaceNodeKind::Element {
                tag,
                namespace,
                attributes,
                listeners,
                element_id,
            } => {
                let mut attrs = Vec::with_capacity(attributes.len() + listeners.len());
                for ((name, ns), value) in attributes {
                    let (attr_name, attr_ns) =
                        static_attribute(name, ns.as_deref()).expect("validated surface attribute");
                    let volatile = attr_name == "value" || attr_name == "checked";
                    let attr_value = match value {
                        AttributeValue::Text(text) => {
                            dioxus_core::AttributeValue::Text(text.clone())
                        }
                        AttributeValue::Float(float) => dioxus_core::AttributeValue::Float(*float),
                        AttributeValue::Int(int) => dioxus_core::AttributeValue::Int(*int),
                        AttributeValue::Boolean(bool) => dioxus_core::AttributeValue::Bool(*bool),
                        AttributeValue::None => dioxus_core::AttributeValue::None,
                    };
                    attrs.push(dioxus_core::Attribute::new(
                        attr_name, attr_value, attr_ns, volatile,
                    ));
                }
                if let Some(guest_element) = *element_id {
                    let guest_node_id =
                        NodeId::try_from(guest_element.raw()).expect("element id fits NodeId");
                    for listener_name in listeners {
                        attrs.push(
                            surface_listener_attribute(
                                listener_name,
                                self.revision,
                                guest_node_id,
                                dispatch.clone(),
                            )
                            .expect("validated surface listener"),
                        );
                    }
                }
                let children = node
                    .children
                    .iter()
                    .map(|&child| self.render_node(child, dispatch));
                let key = id.1;
                render_element_tag(tag, namespace.as_deref(), key, attrs, children)
            }
        }
    }
}

impl WriteMutations for SurfaceTree {
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
            SurfaceNodeKind::Root => panic!("set_text called on surface root"),
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

fn static_attribute(
    name: &str,
    namespace: Option<&str>,
) -> Option<(&'static str, Option<&'static str>)> {
    macro_rules! vocabulary {
        ($($ns:pat => $canonical:expr => [$($name:literal),* $(,)?]),* $(,)?) => {
            match (name, namespace) {
                $($(($name, $ns) => Some(($name, $canonical)),)*)*
                _ => None,
            }
        };
    }
    vocabulary!(
        None => None => [
            "id", "class", "style", "title", "role", "tabindex", "hidden", "lang", "dir",
            "accesskey", "contenteditable", "draggable", "spellcheck", "translate",
            "value", "checked", "disabled", "readonly", "required", "multiple", "selected",
            "name", "type", "placeholder", "autocomplete", "autofocus", "inputmode", "pattern",
            "min", "max", "step", "minlength", "maxlength", "size", "rows", "cols", "wrap",
            "for", "form", "action", "method", "enctype", "novalidate", "accept", "accept-charset",
            "href", "target", "rel", "download", "src", "alt", "width", "height", "loading",
            "srcset", "sizes", "crossorigin", "referrerpolicy", "open", "datetime", "start",
            "reversed", "colspan", "rowspan", "scope", "headers", "label", "low", "high",
            "optimum", "data-testid", "data-state", "data-value",
            "aria-label", "aria-labelledby", "aria-describedby", "aria-hidden", "aria-live",
            "aria-atomic", "aria-busy", "aria-controls", "aria-current", "aria-disabled",
            "aria-expanded", "aria-haspopup", "aria-invalid", "aria-modal", "aria-multiline",
            "aria-multiselectable", "aria-orientation", "aria-pressed", "aria-readonly",
            "aria-required", "aria-selected", "aria-checked", "aria-valuemin", "aria-valuemax",
            "aria-valuenow", "aria-valuetext", "aria-level", "aria-posinset", "aria-setsize",
            "viewBox", "preserveAspectRatio", "d", "fill", "fill-rule", "fill-opacity", "stroke",
            "stroke-width", "stroke-linecap", "stroke-linejoin", "stroke-dasharray",
            "stroke-dashoffset", "stroke-opacity", "opacity", "transform", "cx", "cy", "r",
            "rx", "ry", "x", "y", "x1", "y1", "x2", "y2", "points", "xmlns", "clip-path"
        ],
        Some("style") => Some("style") => [
            "display", "position", "top", "right", "bottom", "left", "inset", "z-index",
            "width", "height", "min-width", "max-width", "min-height", "max-height",
            "margin", "margin-top", "margin-right", "margin-bottom", "margin-left",
            "padding", "padding-top", "padding-right", "padding-bottom", "padding-left",
            "gap", "row-gap", "column-gap", "flex", "flex-direction", "flex-wrap", "flex-grow",
            "flex-shrink", "flex-basis", "align-items", "align-self", "justify-content",
            "grid-template-columns", "grid-template-rows", "grid-column", "grid-row",
            "overflow", "overflow-x", "overflow-y", "color", "background", "background-color",
            "border", "border-color", "border-width", "border-style", "border-radius",
            "font", "font-family", "font-size", "font-weight", "font-style", "line-height",
            "text-align", "text-decoration", "text-overflow", "white-space", "word-break",
            "cursor", "pointer-events", "user-select", "opacity", "visibility", "box-sizing",
            "box-shadow", "transform", "transform-origin", "transition", "object-fit"
        ],
        Some("http://www.w3.org/1999/xlink") => Some("http://www.w3.org/1999/xlink") => ["href"]
    )
}

macro_rules! expand_surface_event_listener {
    (
        enum Event {
            $(
                #[convert = $converter:ident]
                #[events = [
                    $(
                        $( #[$attr:meta] )*
                        $name:ident => $raw:ident,
                    )*
                ]]
                $(#[raw = [$($raw_only:ident),* $(,)?]])?
                $group:ident($data:ident),
            )*
        }
    ) => {
        fn supported_listener(name: &str) -> bool {
            match name {
                $($(stringify!($raw) => true,)*)*
                _ => false,
            }
        }

        fn surface_listener_attribute(
            raw_name: &str,
            revision: u64,
            element: NodeId,
            dispatch: SurfaceEventHandler,
        ) -> Option<dioxus_core::Attribute> {
            match raw_name {
                $(
                    $(
                        stringify!($raw) => {
                            let event_name = stringify!($raw);
                            let listener = ListenerCallback::new(
                                move |evt: dioxus_core::Event<PlatformEventData>| {
                                    let platform_rc = evt.data();
                                    let data_json = expand_surface_event_listener!(
                                        @serialize $group, $data, platform_rc.as_ref()
                                    );
                                    let response = dispatch(Event {
                                        revision,
                                        element,
                                        name: event_name.to_owned(),
                                        bubbles: evt.propagates(),
                                        data_json,
                                    });
                                    if response.prevent_default {
                                        evt.prevent_default();
                                    }
                                    if response.stop_propagation {
                                        evt.stop_propagation();
                                    }
                                },
                            );
                            return Some(dioxus_core::Attribute::new(
                                stringify!($name),
                                dioxus_core::AttributeValue::Listener(listener.erase()),
                                None,
                                false,
                            ));
                        }
                    )*
                )*
                _ => None,
            }
        }
    };
    (@serialize Mounted, $data:ident, $platform:expr) => {{
        let _ = $platform;
        "null".to_owned()
    }};
    (@serialize $group:ident, $data:ident, $platform:expr) => {{
        pastey::paste! {
            let platform: &PlatformEventData = $platform;
            if let Some(serialized) = platform.downcast::<dioxus_html::[<Serialized $data>]>() {
                serde_json::to_string(serialized).expect("event data serializes to JSON")
            } else {
                let converted = dioxus_html::$data::from(platform);
                serde_json::to_string(&converted).expect("event data serializes to JSON")
            }
        }
    }};
}

dioxus_html::with_html_event_groups!(expand_surface_event_listener);

macro_rules! surface_tags {
    ($($namespace:pat => [$($tag:ident),* $(,)?]),* $(,)?) => {
        fn supported_tag(tag: &str, namespace: Option<&str>) -> bool {
            match (tag, namespace) {
                $($((stringify!($tag), $namespace) => true,)*)*
                _ => false,
            }
        }

        fn render_element_tag(
            tag: &str,
            namespace: Option<&str>,
            key: u64,
            attrs: Vec<dioxus_core::Attribute>,
            children: impl Iterator<Item = Element>,
        ) -> Element {
            match (tag, namespace) {
                $($((stringify!($tag), $namespace) => rsx! {
                    $tag {
                        key: "{key}",
                        ..attrs,
                        {children}
                    }
                },)*)*
                _ => unreachable!("validated surface element"),
            }
        }
    };
}

surface_tags!(
    None => [
        div, section, article, aside, header, footer, main, nav, h1, h2, h3, h4, h5, h6, p, span,
        pre, code, blockquote, strong, em, small, b, i, u, s, mark, kbd, sub, sup, br, hr, ul, ol,
        li, dl, dt, dd, table, thead, tbody, tfoot, tr, th, td, caption, form, label, input,
        textarea, button, select, option, optgroup, fieldset, legend, details, summary, dialog, a,
        img, figure, figcaption, progress, meter, output, time, data
    ],
    Some("http://www.w3.org/2000/svg") => [
        svg, g, path, circle, rect, line, polyline, polygon, defs
    ]
);

#[cfg(test)]
mod tests {
    use super::*;

    fn counter_initial_batch() -> Vec<Mutation> {
        vec![
            Mutation::PushId(0),
            Mutation::CreateElement(("button".into(), None)),
            Mutation::SetAttribute(("class".into(), None, AttributeValue::Text("btn".into()))),
            Mutation::CreateText("0".into()),
            Mutation::AppendChildren(1),
            Mutation::SetId(1),
            Mutation::AddEventListener("click".into()),
            Mutation::PushId(1),
            Mutation::Child(0),
            Mutation::SetId(2),
            Mutation::Pop,
            Mutation::AppendChildren(1),
            Mutation::Pop,
        ]
    }

    fn counter_text_update_batch(value: &str) -> Vec<Mutation> {
        vec![
            Mutation::PushId(2),
            Mutation::SetText(value.into()),
            Mutation::Pop,
        ]
    }

    #[test]
    fn surface_tree_replays_batches_and_clears_on_withdrawal() {
        let mut tree = SurfaceTree::new();
        assert!(tree.is_empty());
        assert_eq!(tree.apply(&counter_initial_batch()), Ok(1));
        assert!(!tree.is_empty());
        assert_eq!(tree.text_content(), "0");

        assert_eq!(tree.apply(&counter_text_update_batch("1")), Ok(2));
        assert_eq!(tree.text_content(), "1");

        tree.clear();
        assert!(tree.is_empty());
        assert_eq!(tree.text_content(), "");
        assert_eq!(tree.revision(), 3);
    }

    #[test]
    fn listener_resolution_excludes_detached_removed_and_cleared_nodes() {
        let mut tree = SurfaceTree::new();
        tree.apply(&[
            Mutation::CreateElement(("button".into(), None)),
            Mutation::SetId(3),
            Mutation::AddEventListener("click".into()),
            Mutation::Pop,
        ])
        .unwrap();
        assert_eq!(tree.listener_id("button", "click"), None);

        tree.apply(&counter_initial_batch()).unwrap();
        assert_eq!(
            tree.listener_id("button", "click"),
            Some(ElementId::from_raw(1))
        );
        assert_eq!(tree.listener_id("button", "keydown"), None);
        assert_eq!(tree.listener_id("input", "click"), None);

        tree.apply(&[
            Mutation::PushId(1),
            Mutation::RemoveEventListener("click".into()),
            Mutation::Pop,
        ])
        .unwrap();
        assert_eq!(tree.listener_id("button", "click"), None);

        tree.apply(&[
            Mutation::PushId(1),
            Mutation::AddEventListener("click".into()),
            Mutation::Pop,
        ])
        .unwrap();
        assert_eq!(
            tree.listener_id("button", "click"),
            Some(ElementId::from_raw(1))
        );
        tree.apply(&[Mutation::PushId(1), Mutation::Remove])
            .unwrap();
        assert_eq!(tree.listener_id("button", "click"), None);

        tree.apply(&counter_initial_batch()).unwrap();
        tree.clear();
        assert_eq!(tree.listener_id("button", "click"), None);
    }

    fn assert_terminal(batch: &[Mutation], unsupported: bool) -> PresentationError {
        let mut tree = SurfaceTree::new();
        tree.apply(&counter_initial_batch()).unwrap();
        let error = tree.apply(batch).unwrap_err();
        assert_eq!(
            matches!(error, PresentationError::Unsupported(_)),
            unsupported,
            "{batch:?}: {error}"
        );
        assert!(tree.is_empty());
        assert_eq!(tree.nodes.len(), 1);
        assert_eq!(tree.retained_bytes, 0);
        assert_eq!(tree.error(), Some(&error));
        assert_eq!(tree.listener_id("button", "click"), None);
        assert_eq!(
            tree.apply(&counter_initial_batch()),
            Err(PresentationError::Revoked)
        );
        tree.clear();
        assert_eq!(tree.error(), Some(&error));
        assert_eq!(tree.apply(&[]), Err(PresentationError::Revoked));
        error
    }

    #[test]
    fn malformed_operations_clear_and_terminally_revoke_only_the_target_tree() {
        let batches = vec![
            vec![Mutation::PushId(42)],
            vec![Mutation::SetId(1)],
            vec![Mutation::Child(0)],
            vec![Mutation::Pop],
            vec![Mutation::CloneNode],
            vec![Mutation::AppendChildren(1)],
            vec![Mutation::ReplaceWith(0)],
            vec![Mutation::InsertAfter(0)],
            vec![Mutation::InsertBefore(0)],
            vec![Mutation::Remove],
            vec![Mutation::SetText("root".into())],
            vec![Mutation::SetAttribute((
                "class".into(),
                None,
                AttributeValue::Text("root".into()),
            ))],
            vec![Mutation::AddEventListener("click".into())],
            vec![Mutation::RemoveEventListener("click".into())],
            vec![Mutation::PushId(1), Mutation::Child(u32::MAX)],
            vec![Mutation::PushId(1), Mutation::SetId(0)],
            vec![
                Mutation::PushId(2),
                Mutation::SetAttribute(("class".into(), None, AttributeValue::Int(1))),
            ],
            vec![
                Mutation::PushId(2),
                Mutation::AddEventListener("click".into()),
            ],
            vec![
                Mutation::CreateElement(("button".into(), None)),
                Mutation::AddEventListener("click".into()),
            ],
            vec![Mutation::PushId(1), Mutation::AppendChildren(u32::MAX)],
            vec![
                Mutation::PushId(1),
                Mutation::PushId(1),
                Mutation::AppendChildren(1),
            ],
            vec![
                Mutation::PushId(2),
                Mutation::CreateText("child".into()),
                Mutation::AppendChildren(1),
            ],
            vec![Mutation::CreateText("unbalanced".into())],
            vec![Mutation::PushId(1), Mutation::Remove, Mutation::PushId(1)],
            vec![Mutation::PushId(2), Mutation::PushId(1), Mutation::Remove],
            vec![
                Mutation::PushId(2),
                Mutation::PushId(1),
                Mutation::SetText("replacement".into()),
            ],
            vec![
                Mutation::PushId(0),
                Mutation::PushId(1),
                Mutation::PushId(1),
                Mutation::AppendChildren(2),
            ],
            vec![
                Mutation::PushId(2),
                Mutation::PushId(1),
                Mutation::InsertBefore(1),
            ],
            vec![
                Mutation::PushId(1),
                Mutation::PushId(1),
                Mutation::ReplaceWith(1),
            ],
        ];
        let mut sibling = SurfaceTree::new();
        sibling.apply(&counter_initial_batch()).unwrap();
        let expected = sibling.clone();
        for batch in batches {
            assert_terminal(&batch, false);
            assert_eq!(sibling, expected);
        }
        sibling
            .apply(&counter_text_update_batch("still live"))
            .unwrap();
        assert_eq!(sibling.text_content(), "still live");
    }

    #[test]
    fn unsupported_vocabulary_is_rejected_without_dynamic_interning_or_tag_substitution() {
        let batches = vec![
            vec![Mutation::CreateElement(("unknown-widget".into(), None))],
            vec![Mutation::CreateElement((
                "div".into(),
                Some("unknown".into()),
            ))],
            vec![Mutation::CreateElement(("svg".into(), None))],
            vec![Mutation::CreateElement((
                "div".into(),
                Some("http://www.w3.org/2000/svg".into()),
            ))],
            vec![
                Mutation::PushId(1),
                Mutation::SetAttribute(("unknown-property".into(), None, AttributeValue::Int(1))),
            ],
            vec![
                Mutation::PushId(1),
                Mutation::SetAttribute((
                    "class".into(),
                    Some("unknown".into()),
                    AttributeValue::Int(1),
                )),
            ],
            vec![
                Mutation::PushId(1),
                Mutation::SetAttribute((
                    "onclick".into(),
                    None,
                    AttributeValue::Text("code".into()),
                )),
            ],
            vec![
                Mutation::PushId(1),
                Mutation::AddEventListener("unknown-event".into()),
            ],
            vec![
                Mutation::PushId(1),
                Mutation::RemoveEventListener("unknown-event".into()),
            ],
            vec![
                Mutation::PushId(1),
                Mutation::SetAttribute(("value".into(), None, AttributeValue::Float(f64::NAN))),
            ],
            vec![
                Mutation::PushId(1),
                Mutation::SetAttribute((
                    "value".into(),
                    None,
                    AttributeValue::Float(f64::INFINITY),
                )),
            ],
        ];
        for batch in batches {
            assert_terminal(&batch, true);
        }
    }

    #[test]
    fn ingress_limits_reject_sparse_ids_oversized_batches_strings_and_stacks() {
        for mutation in [Mutation::PushId(u32::MAX), Mutation::SetId(u32::MAX)] {
            assert_terminal(&[mutation], true);
        }
        assert_terminal(
            &vec![Mutation::Pop; SurfaceTree::MAX_BATCH_MUTATIONS + 1],
            true,
        );
        assert_terminal(
            &[Mutation::CreateText(
                "x".repeat(SurfaceTree::MAX_STRING_BYTES + 1),
            )],
            true,
        );
        assert_terminal(
            &vec![Mutation::PushId(0); SurfaceTree::MAX_STACK_DEPTH],
            true,
        );
        let mut batch = Vec::new();
        for _ in 0..5 {
            batch.push(Mutation::CreateText(
                "x".repeat(SurfaceTree::MAX_STRING_BYTES),
            ));
            batch.push(Mutation::Pop);
        }
        assert_eq!(
            assert_terminal(&batch, true),
            PresentationError::Unsupported("batch byte limit".into())
        );
    }

    #[test]
    fn maximum_element_id_is_usable_but_larger_ids_cannot_grow_the_mapping() {
        let mut tree = SurfaceTree::new();
        tree.apply(&[
            Mutation::CreateElement(("button".into(), None)),
            Mutation::SetId(SurfaceTree::MAX_ELEMENT_ID),
            Mutation::AddEventListener("click".into()),
            Mutation::AppendChildren(1),
        ])
        .unwrap();
        assert_eq!(
            tree.listener_id("button", "click"),
            Some(ElementId::from_raw(SurfaceTree::MAX_ELEMENT_ID as usize))
        );
        assert_eq!(
            tree.element_to_node.len(),
            SurfaceTree::MAX_ELEMENT_ID as usize + 1
        );
        tree.apply(&[Mutation::PushId(SurfaceTree::MAX_ELEMENT_ID), Mutation::Pop])
            .unwrap();
        assert_eq!(
            tree.apply(&[Mutation::SetId(SurfaceTree::MAX_ELEMENT_ID + 1)]),
            Err(PresentationError::Unsupported("element id limit".into()))
        );
        assert_eq!(tree.element_to_node, vec![Some(SurfaceNodeId::ROOT)]);
        assert!(tree.is_empty());
    }

    #[test]
    fn maximum_tree_depth_is_usable_but_text_children_cannot_exceed_it() {
        let mut tree = SurfaceTree::new();
        let mut batch: Vec<_> = (0..SurfaceTree::MAX_TREE_DEPTH)
            .map(|_| Mutation::CreateElement(("div".into(), None)))
            .collect();
        batch.push(Mutation::SetId(1));
        batch.extend((0..SurfaceTree::MAX_TREE_DEPTH).map(|_| Mutation::AppendChildren(1)));
        tree.apply(&batch).unwrap();
        let mut work = 0;
        assert_eq!(
            tree.depth(tree.lookup(ElementId::from_raw(1)), &mut work)
                .unwrap(),
            SurfaceTree::MAX_TREE_DEPTH
        );
        assert_eq!(
            tree.apply(&[
                Mutation::PushId(1),
                Mutation::SetText("too deep".into()),
                Mutation::Pop
            ]),
            Err(PresentationError::Unsupported("tree depth limit".into()))
        );
        assert!(tree.is_empty());
    }

    #[test]
    fn stale_aliases_and_revision_exhaustion_return_typed_terminal_errors() {
        let mut tree = SurfaceTree::new();
        tree.apply(&counter_initial_batch()).unwrap();
        tree.apply(&[Mutation::PushId(2), Mutation::SetId(3), Mutation::Remove])
            .unwrap();
        assert!(matches!(
            tree.apply(&[Mutation::PushId(2)]),
            Err(PresentationError::InvalidBatch(_))
        ));
        assert!(tree.is_empty());

        let mut tree = SurfaceTree::new();
        tree.apply(&counter_initial_batch()).unwrap();
        tree.revision = u64::MAX;
        let error = PresentationError::Unsupported("surface revision limit".into());
        assert_eq!(tree.apply(&[]), Err(error.clone()));
        assert_eq!(tree.error(), Some(&error));
        assert_eq!(tree.revision(), u64::MAX);
        assert!(tree.is_empty());
        assert_eq!(tree.apply(&[]), Err(PresentationError::Revoked));
    }

    #[test]
    fn retained_node_and_byte_limits_apply_across_batches() {
        let mut bytes_tree = SurfaceTree::new();
        let batch = [
            Mutation::CreateText("x".repeat(SurfaceTree::MAX_STRING_BYTES)),
            Mutation::AppendChildren(1),
        ];
        for _ in 0..SurfaceTree::MAX_RETAINED_BYTES / SurfaceTree::MAX_STRING_BYTES {
            bytes_tree.apply(&batch).unwrap();
        }
        assert_eq!(bytes_tree.retained_bytes, SurfaceTree::MAX_RETAINED_BYTES);
        assert_eq!(
            bytes_tree.apply(&[Mutation::CreateText("x".into())]),
            Err(PresentationError::Unsupported("retained byte limit".into()))
        );
        assert!(bytes_tree.is_empty());

        let mut nodes_tree = SurfaceTree::new();
        let batch: Vec<_> = (0..1024)
            .flat_map(|_| [Mutation::CreateText(String::new()), Mutation::Pop])
            .collect();
        for _ in 0..7 {
            nodes_tree.apply(&batch).unwrap();
        }
        let last: Vec<_> = (0..1023)
            .flat_map(|_| [Mutation::CreateText(String::new()), Mutation::Pop])
            .collect();
        nodes_tree.apply(&last).unwrap();
        assert_eq!(nodes_tree.nodes.len(), SurfaceTree::MAX_NODE_SLOTS);
        assert_eq!(
            nodes_tree.apply(&[Mutation::CreateText(String::new())]),
            Err(PresentationError::Unsupported("node slot limit".into()))
        );
        assert!(nodes_tree.is_empty());
    }

    #[test]
    fn remove_recreate_churn_reuses_slots_without_changing_retained_sibling_identity() {
        let mut tree = SurfaceTree::new();
        tree.apply(&counter_initial_batch()).unwrap();
        tree.apply(&[
            Mutation::CreateElement(("span".into(), None)),
            Mutation::SetId(3),
            Mutation::CreateText("fixed".into()),
            Mutation::AppendChildren(1),
            Mutation::AppendChildren(1),
        ])
        .unwrap();
        let sibling = tree.lookup(ElementId::from_raw(3));
        let mut batch = vec![Mutation::PushId(1), Mutation::Remove];
        batch.extend(counter_initial_batch());
        for _ in 0..=SurfaceTree::MAX_NODE_SLOTS {
            let previous = tree.lookup(ElementId::from_raw(1));
            tree.apply(&batch).unwrap();
            let current = tree.lookup(ElementId::from_raw(1));
            assert_eq!(current.0, previous.0);
            assert_ne!(current, previous);
            assert!(tree.live_node(previous).is_none());
            assert_eq!(tree.lookup(ElementId::from_raw(3)), sibling);
            assert_eq!(tree.nodes.len(), 5);
            assert!(tree.vacant_slots.is_empty());
            assert_eq!(tree.text_content(), "fixed0");
            assert_eq!(
                tree.listener_id("button", "click"),
                Some(ElementId::from_raw(1))
            );
        }
        assert!(tree.error().is_none());
    }

    #[test]
    fn recycled_slots_cannot_resurrect_stale_element_ids_or_stack_entries() {
        let mut tree = SurfaceTree::new();
        tree.apply(&counter_initial_batch()).unwrap();
        let stale = tree.lookup(ElementId::from_raw(2));
        tree.apply(&[
            Mutation::PushId(2),
            Mutation::SetId(3),
            Mutation::Remove,
            Mutation::CreateText("replacement".into()),
            Mutation::SetId(4),
            Mutation::AppendChildren(1),
        ])
        .unwrap();
        let replacement = tree.lookup(ElementId::from_raw(4));
        assert_eq!(replacement.0, stale.0);
        assert_ne!(replacement, stale);
        assert!(tree.live_node(stale).is_none());
        assert!(matches!(
            tree.apply(&[Mutation::PushId(2)]),
            Err(PresentationError::InvalidBatch(_))
        ));
        assert!(tree.is_empty());
        assert_terminal(
            &[
                Mutation::PushId(2),
                Mutation::PushId(1),
                Mutation::SetText("replacement".into()),
            ],
            false,
        );
    }

    #[test]
    fn text_replacement_reuses_slots_even_when_all_retained_slots_are_occupied() {
        let mut tree = SurfaceTree::new();
        tree.apply(&counter_initial_batch()).unwrap();
        let batch: Vec<_> = (0..1024)
            .flat_map(|_| [Mutation::CreateText(String::new()), Mutation::Pop])
            .collect();
        for _ in 0..7 {
            tree.apply(&batch).unwrap();
        }
        let last: Vec<_> = (0..1021)
            .flat_map(|_| [Mutation::CreateText(String::new()), Mutation::Pop])
            .collect();
        tree.apply(&last).unwrap();
        assert_eq!(tree.nodes.len(), SurfaceTree::MAX_NODE_SLOTS);
        let previous = tree.lookup(ElementId::from_raw(2));
        for _ in 0..16 {
            tree.apply(&[
                Mutation::PushId(1),
                Mutation::SetText("replacement".into()),
                Mutation::Pop,
            ])
            .unwrap();
            assert_eq!(tree.nodes.len(), SurfaceTree::MAX_NODE_SLOTS);
            assert!(tree.vacant_slots.is_empty());
        }
        assert!(tree.live_node(previous).is_none());
        assert_eq!(tree.text_content(), "replacement");
    }

    #[test]
    fn recycled_elements_have_distinct_host_keys_and_clearing_does_not_reset_identity() {
        #[derive(Clone, PartialEq)]
        struct Props {
            tree: Rc<std::cell::RefCell<SurfaceTree>>,
            keys: Rc<std::cell::RefCell<Vec<Option<String>>>>,
        }
        fn app(props: Props) -> Element {
            let tree = props.tree.borrow();
            let child = tree.node(SurfaceNodeId::ROOT).children[0];
            let dispatch: SurfaceEventHandler = Rc::new(|_| EventResponse::default());
            let rendered = tree.render_node(child, &dispatch)?;
            props
                .keys
                .borrow_mut()
                .push(rendered.key().map(str::to_owned));
            tree.render(&dispatch)
        }
        let tree = Rc::new(std::cell::RefCell::new(SurfaceTree::new()));
        tree.borrow_mut().apply(&counter_initial_batch()).unwrap();
        let original = tree.borrow().lookup(ElementId::from_raw(1));
        let keys = Rc::new(std::cell::RefCell::new(Vec::new()));
        let mut dom = dioxus_core::VirtualDom::new_with_props(
            app,
            Props {
                tree: tree.clone(),
                keys: keys.clone(),
            },
        );
        let mut host_tree = SurfaceTree::new();
        dom.rebuild(&mut host_tree);
        let original_host = host_tree.node(SurfaceNodeId::ROOT).children[0];
        let mut batch = vec![Mutation::PushId(1), Mutation::Remove];
        batch.extend(counter_initial_batch());
        tree.borrow_mut().apply(&batch).unwrap();
        let replacement = tree.borrow().lookup(ElementId::from_raw(1));
        dom.mark_dirty(dioxus_core::ScopeId::APP);
        dom.render_immediate(&mut host_tree);
        let replacement_host = host_tree.node(SurfaceNodeId::ROOT).children[0];
        assert_ne!(original_host, replacement_host);
        assert_eq!(
            &*keys.borrow(),
            &[
                Some(original.1.to_string()),
                Some(replacement.1.to_string())
            ]
        );
        assert_ne!(keys.borrow()[0], keys.borrow()[1]);
        tree.borrow_mut()
            .apply(&counter_text_update_batch("updated"))
            .unwrap();
        dom.mark_dirty(dioxus_core::ScopeId::APP);
        dom.render_immediate(&mut host_tree);
        assert_eq!(
            host_tree.node(SurfaceNodeId::ROOT).children[0],
            replacement_host
        );
        assert_eq!(host_tree.text_content(), "updated");

        tree.borrow_mut().clear();
        tree.borrow_mut().apply(&counter_initial_batch()).unwrap();
        let after_clear = tree.borrow().lookup(ElementId::from_raw(1));
        assert_eq!(original.0, after_clear.0);
        assert_ne!(original, after_clear);
        assert_ne!(replacement, after_clear);
        dom.mark_dirty(dioxus_core::ScopeId::APP);
        dom.render_immediate(&mut host_tree);
        assert_ne!(
            host_tree.node(SurfaceNodeId::ROOT).children[0],
            replacement_host
        );
        assert_eq!(host_tree.text_content(), "0");
    }

    #[test]
    fn subtree_depth_and_clone_expansion_are_bounded_before_recursing_or_allocating() {
        let mut batch = vec![Mutation::PushId(0)];
        batch.extend(
            (0..=SurfaceTree::MAX_TREE_DEPTH)
                .map(|_| Mutation::CreateElement(("div".into(), None))),
        );
        batch.extend((0..=SurfaceTree::MAX_TREE_DEPTH).map(|_| Mutation::AppendChildren(1)));
        batch.push(Mutation::Pop);
        assert_eq!(
            assert_terminal(&batch, true),
            PresentationError::Unsupported("tree depth limit".into())
        );

        let mut tree = SurfaceTree::new();
        tree.apply(&[
            Mutation::CreateText("leaf".into()),
            Mutation::SetId(1),
            Mutation::Pop,
        ])
        .unwrap();
        let clone_batch = [
            Mutation::CreateElement(("div".into(), None)),
            Mutation::PushId(1),
            Mutation::CloneNode,
            Mutation::PushId(1),
            Mutation::CloneNode,
            Mutation::AppendChildren(2),
            Mutation::SetId(1),
            Mutation::Pop,
        ];
        let error = (0..16)
            .find_map(|_| tree.apply(&clone_batch).err())
            .expect("clones hit the arena limit");
        assert_eq!(
            error,
            PresentationError::Unsupported("node slot limit".into())
        );
        assert!(tree.is_empty());
    }

    #[test]
    fn batch_work_limits_cover_cloned_bytes_and_repeated_sibling_scans() {
        let mut tree = SurfaceTree::new();
        tree.apply(&[
            Mutation::CreateText("x".repeat(SurfaceTree::MAX_STRING_BYTES)),
            Mutation::SetId(1),
            Mutation::Pop,
        ])
        .unwrap();
        let batch: Vec<_> = (0..256)
            .flat_map(|_| [Mutation::PushId(1), Mutation::CloneNode, Mutation::Remove])
            .collect();
        assert_eq!(
            tree.apply(&batch),
            Err(PresentationError::Unsupported("batch work limit".into()))
        );
        assert!(tree.is_empty());

        let mut tree = SurfaceTree::new();
        tree.apply(&[
            Mutation::CreateText("anchor".into()),
            Mutation::SetId(1),
            Mutation::AppendChildren(1),
        ])
        .unwrap();
        let batch: Vec<_> = (0..1024)
            .flat_map(|_| {
                [
                    Mutation::PushId(1),
                    Mutation::CreateText(String::new()),
                    Mutation::InsertAfter(1),
                    Mutation::Pop,
                ]
            })
            .collect();
        let error = tree.apply(&batch).unwrap_err();
        assert_eq!(
            error,
            PresentationError::Unsupported("batch work limit".into())
        );
        assert!(tree.is_empty());
    }

    #[test]
    fn valid_move_insert_replace_clone_and_id_reuse_preserve_stack_machine_semantics() {
        let mut tree = SurfaceTree::new();
        tree.apply(&counter_initial_batch()).unwrap();
        tree.apply(&[
            Mutation::PushId(1),
            Mutation::CreateText("before".into()),
            Mutation::SetId(3),
            Mutation::InsertBefore(1),
            Mutation::Pop,
            Mutation::PushId(1),
            Mutation::CreateText("after".into()),
            Mutation::SetId(4),
            Mutation::InsertAfter(1),
            Mutation::Pop,
            Mutation::PushId(3),
            Mutation::PushId(4),
            Mutation::InsertBefore(1),
            Mutation::Pop,
        ])
        .unwrap();
        assert_eq!(tree.text_content(), "afterbefore0");
        tree.apply(&[
            Mutation::PushId(3),
            Mutation::CreateText("replacement".into()),
            Mutation::SetId(3),
            Mutation::ReplaceWith(1),
            Mutation::PushId(1),
            Mutation::CloneNode,
            Mutation::SetId(5),
            Mutation::AddEventListener("click".into()),
            Mutation::AppendChildren(1),
        ])
        .unwrap();
        assert_eq!(tree.text_content(), "afterreplacement00");
        tree.apply(&[Mutation::PushId(1), Mutation::Remove])
            .unwrap();
        assert_eq!(
            tree.listener_id("button", "click"),
            Some(ElementId::from_raw(5))
        );
        assert_eq!(
            tree.retained_bytes,
            tree.nodes
                .iter()
                .flatten()
                .map(|node| SurfaceTree::kind_bytes(&node.kind))
                .sum::<usize>()
        );
    }

    #[test]
    fn valid_static_attributes_namespaces_and_listeners_project_into_host_vdom() {
        #[derive(Clone, PartialEq)]
        struct Props {
            tree: SurfaceTree,
        }
        fn app(props: Props) -> Element {
            let dispatch: SurfaceEventHandler = Rc::new(|_| EventResponse::default());
            props.tree.render(&dispatch)
        }
        let mut tree = SurfaceTree::new();
        tree.apply(&[
            Mutation::CreateElement(("svg".into(), Some("http://www.w3.org/2000/svg".into()))),
            Mutation::SetAttribute((
                "viewBox".into(),
                None,
                AttributeValue::Text("0 0 24 24".into()),
            )),
            Mutation::CreateElement(("path".into(), Some("http://www.w3.org/2000/svg".into()))),
            Mutation::SetAttribute(("d".into(), None, AttributeValue::Text("M0 0L24 24".into()))),
            Mutation::AppendChildren(1),
            Mutation::AppendChildren(1),
            Mutation::CreateElement(("input".into(), None)),
            Mutation::SetId(1),
            Mutation::SetAttribute((
                "aria-label".into(),
                None,
                AttributeValue::Text("Name".into()),
            )),
            Mutation::SetAttribute((
                "color".into(),
                Some("style".into()),
                AttributeValue::Text("red".into()),
            )),
            Mutation::SetAttribute(("value".into(), None, AttributeValue::Text("hello".into()))),
            Mutation::AddEventListener("input".into()),
            Mutation::AppendChildren(1),
        ])
        .unwrap();
        let mut dom = dioxus_core::VirtualDom::new_with_props(app, Props { tree });
        let mut host_tree = SurfaceTree::new();
        dom.rebuild(&mut host_tree);
        assert!(host_tree.listener_id("input", "input").is_some());
        let svg = host_tree.node(host_tree.node(SurfaceNodeId::ROOT).children[0]);
        assert!(
            matches!(&svg.kind, SurfaceNodeKind::Element { tag, namespace: Some(namespace), .. }
            if tag == "svg" && namespace == "http://www.w3.org/2000/svg")
        );
    }

    #[test]
    fn replacing_values_and_removing_subtrees_release_retained_bytes() {
        let mut tree = SurfaceTree::new();
        tree.apply(&counter_initial_batch()).unwrap();
        for value in ["x".repeat(4096), "short".into()] {
            tree.apply(&[
                Mutation::PushId(1),
                Mutation::SetAttribute(("class".into(), None, AttributeValue::Text(value.clone()))),
                Mutation::AddEventListener("click".into()),
                Mutation::SetText(value),
                Mutation::Pop,
            ])
            .unwrap();
            assert_eq!(
                tree.retained_bytes,
                tree.nodes
                    .iter()
                    .flatten()
                    .map(|node| SurfaceTree::kind_bytes(&node.kind))
                    .sum::<usize>()
            );
        }
        tree.apply(&[
            Mutation::PushId(1),
            Mutation::SetAttribute(("class".into(), None, AttributeValue::None)),
            Mutation::RemoveEventListener("click".into()),
            Mutation::Remove,
        ])
        .unwrap();
        assert_eq!(tree.retained_bytes, 0);
        assert!(tree.is_empty());
        assert!(tree.apply(&[Mutation::PushId(2)]).is_err());
    }

    #[test]
    fn per_node_attribute_and_listener_counts_are_bounded() {
        let names = [
            "id",
            "class",
            "style",
            "title",
            "role",
            "tabindex",
            "hidden",
            "lang",
            "dir",
            "accesskey",
            "contenteditable",
            "draggable",
            "spellcheck",
            "translate",
            "value",
            "checked",
            "disabled",
            "readonly",
            "required",
            "multiple",
            "selected",
            "name",
            "type",
            "placeholder",
            "autocomplete",
            "autofocus",
            "inputmode",
            "pattern",
            "min",
            "max",
            "step",
            "minlength",
            "maxlength",
            "size",
            "rows",
            "cols",
            "wrap",
            "for",
            "form",
            "action",
            "method",
            "enctype",
            "novalidate",
            "accept",
            "accept-charset",
            "href",
            "target",
            "rel",
            "download",
            "src",
            "alt",
            "width",
            "height",
            "loading",
            "srcset",
            "sizes",
            "crossorigin",
            "referrerpolicy",
            "open",
            "datetime",
            "start",
            "reversed",
            "colspan",
            "rowspan",
            "scope",
        ];
        assert_eq!(names.len(), SurfaceTree::MAX_ATTRIBUTES_PER_NODE + 1);
        let mut tree = SurfaceTree::new();
        tree.apply(&[
            Mutation::CreateElement(("button".into(), None)),
            Mutation::SetId(1),
            Mutation::Pop,
        ])
        .unwrap();
        let mut batch = vec![Mutation::PushId(1)];
        batch.extend(
            names[..SurfaceTree::MAX_ATTRIBUTES_PER_NODE]
                .iter()
                .map(|name| Mutation::SetAttribute(((*name).into(), None, AttributeValue::Int(1)))),
        );
        batch.push(Mutation::Pop);
        tree.apply(&batch).unwrap();
        tree.apply(&[
            Mutation::PushId(1),
            Mutation::SetAttribute(("class".into(), None, AttributeValue::Int(2))),
            Mutation::Pop,
        ])
        .unwrap();
        assert_eq!(
            tree.apply(&[
                Mutation::PushId(1),
                Mutation::SetAttribute(("scope".into(), None, AttributeValue::Int(1)))
            ]),
            Err(PresentationError::Unsupported(
                "attribute count limit".into()
            ))
        );

        let names = [
            "copy",
            "cut",
            "paste",
            "compositionstart",
            "compositionupdate",
            "compositionend",
            "keydown",
            "keypress",
            "keyup",
            "focus",
            "blur",
            "focusin",
            "focusout",
            "change",
            "input",
            "invalid",
            "reset",
            "submit",
            "click",
            "contextmenu",
            "dblclick",
            "drag",
            "dragend",
            "dragenter",
            "dragexit",
            "dragleave",
            "dragover",
            "dragstart",
            "drop",
            "mousedown",
            "mouseenter",
            "mouseleave",
            "mousemove",
            "mouseout",
            "mouseover",
            "mouseup",
            "wheel",
        ];
        let supported: Vec<_> = names
            .into_iter()
            .filter(|name| supported_listener(name))
            .collect();
        assert!(supported.len() > SurfaceTree::MAX_LISTENERS_PER_NODE);
        let mut tree = SurfaceTree::new();
        tree.apply(&[
            Mutation::CreateElement(("button".into(), None)),
            Mutation::SetId(1),
            Mutation::Pop,
        ])
        .unwrap();
        let mut batch = vec![Mutation::PushId(1)];
        batch.extend(
            supported[..SurfaceTree::MAX_LISTENERS_PER_NODE]
                .iter()
                .map(|name| Mutation::AddEventListener((*name).into())),
        );
        batch.push(Mutation::Pop);
        tree.apply(&batch).unwrap();
        tree.apply(&[
            Mutation::PushId(1),
            Mutation::AddEventListener(supported[0].into()),
            Mutation::Pop,
        ])
        .unwrap();
        assert_eq!(
            tree.apply(&[
                Mutation::PushId(1),
                Mutation::AddEventListener(supported[SurfaceTree::MAX_LISTENERS_PER_NODE].into())
            ]),
            Err(PresentationError::Unsupported(
                "listener count limit".into()
            ))
        );
    }

    #[test]
    fn short_adversarial_instruction_sequences_preserve_invariants_without_unwinding() {
        let operations = [
            Mutation::PushId(0),
            Mutation::PushId(1),
            Mutation::PushId(2),
            Mutation::PushId(3),
            Mutation::SetId(0),
            Mutation::SetId(1),
            Mutation::SetId(3),
            Mutation::Child(0),
            Mutation::Child(1),
            Mutation::Pop,
            Mutation::CreateElement(("div".into(), None)),
            Mutation::CreateText("x".into()),
            Mutation::CloneNode,
            Mutation::AppendChildren(0),
            Mutation::AppendChildren(1),
            Mutation::AppendChildren(2),
            Mutation::ReplaceWith(0),
            Mutation::ReplaceWith(1),
            Mutation::InsertBefore(1),
            Mutation::InsertAfter(1),
            Mutation::SetAttribute(("class".into(), None, AttributeValue::None)),
            Mutation::SetText("text".into()),
            Mutation::AddEventListener("click".into()),
            Mutation::RemoveEventListener("click".into()),
            Mutation::Remove,
        ];
        let mut initial = SurfaceTree::new();
        initial.apply(&counter_initial_batch()).unwrap();
        for first in &operations {
            for second in &operations {
                for third in &operations {
                    let mut tree = initial.clone();
                    let batch = [first.clone(), second.clone(), third.clone()];
                    match tree.apply(&batch) {
                        Ok(_) => {
                            assert!(tree.error().is_none(), "{batch:?}");
                            assert_eq!(tree.stack.len(), 1, "{batch:?}");
                            assert_eq!(tree.stack[0].node, SurfaceNodeId::ROOT, "{batch:?}");
                            assert_eq!(
                                tree.retained_bytes,
                                tree.nodes
                                    .iter()
                                    .flatten()
                                    .map(|node| SurfaceTree::kind_bytes(&node.kind))
                                    .sum::<usize>(),
                                "{batch:?}"
                            );
                            tree.text_content();
                        }
                        Err(error) => {
                            assert_eq!(tree.error(), Some(&error), "{batch:?}");
                            assert!(tree.is_empty(), "{batch:?}");
                            assert_eq!(
                                tree.apply(&[]),
                                Err(PresentationError::Revoked),
                                "{batch:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn trusted_write_mutations_and_replay_remain_available() {
        let batch = counter_initial_batch();
        let mut checked = SurfaceTree::new();
        checked.apply(&batch).unwrap();
        let mut trusted = SurfaceTree::new();
        replay(&batch, &mut trusted);
        replay(&counter_text_update_batch("1"), &mut trusted);
        checked.apply(&counter_text_update_batch("1")).unwrap();
        trusted.revision = checked.revision;
        assert_eq!(checked, trusted);
        assert_eq!(checked.retained_bytes, trusted.retained_bytes);
    }
}
