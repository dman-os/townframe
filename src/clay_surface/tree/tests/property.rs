//! Property tests for the `WriteMutations` stack machine: random batch streams biased toward
//! validity are checked against a model-DOM oracle, every model-accepted batch must replay
//! identically through the trusted writer path, and unfiltered mutation streams must never
//! panic — only fail terminally (cleared tree, pinned error, permanent revocation).
use super::super::*;
use crate::replay::replay;
use proptest::prelude::*;
use std::collections::BTreeSet;
use std::fmt::Write as _;

type SurfaceTree = super::super::SurfaceTree<()>;
type Mutation = super::super::Mutation<()>;

const TAGS: [&str; 4] = ["div", "span", "button", "p"];
const TEXTS: [&str; 4] = ["", "x", "hello", "0"];
const LISTENERS: [&str; 2] = ["click", "input"];
const ATTRIBUTES: [(&str, Option<&str>); 3] = [("class", None), ("id", None), ("value", None)];
/// Generous caps so generated trees never trip the ingress budgets: the oracle compares
/// semantics, while budget enforcement is pinned by the example tests in `tree/tests.rs`.
const STACK_CAP: usize = 60;
const DEPTH_CAP: usize = 32;

fn attribute_value(index: usize) -> AttributeValue {
    match index % 5 {
        0 => AttributeValue::Text("a".into()),
        1 => AttributeValue::Text("b".into()),
        2 => AttributeValue::Int(1),
        3 => AttributeValue::Float(0.5),
        _ => AttributeValue::Boolean(true),
    }
}

// ── model oracle: a plain arena DOM mirroring the stack-machine semantics the wire format
//    documents; the compiler below only emits mutations the model accepts ────────────────

#[derive(Clone, Debug, PartialEq)]
enum ModelKind {
    Root,
    Element {
        tag: String,
        namespace: Option<String>,
        attributes: BTreeMap<(String, Option<String>), AttributeValue>,
        listeners: BTreeSet<String>,
        element_id: Option<NodeId>,
    },
    Text(String),
}

#[derive(Clone, Debug)]
struct ModelNode {
    kind: ModelKind,
    parent: Option<usize>,
    children: Vec<usize>,
    /// Dropped subtrees stay in the arena as tombstones; an id mapping that outlives its node
    /// (text-node ids are not cleared on drop) is dead weight the tree rejects on `PushId`.
    live: bool,
}

struct Model {
    nodes: Vec<ModelNode>,
}

impl Model {
    fn new() -> Self {
        Self {
            nodes: vec![ModelNode {
                kind: ModelKind::Root,
                parent: None,
                children: Vec::new(),
                live: true,
            }],
        }
    }

    fn alloc(&mut self, kind: ModelKind) -> usize {
        self.nodes.push(ModelNode {
            kind,
            parent: None,
            children: Vec::new(),
            live: true,
        });
        self.nodes.len() - 1
    }

    fn within(&self, subtree: usize, node: usize) -> bool {
        let mut current = Some(node);
        while let Some(id) = current {
            if id == subtree {
                return true;
            }
            current = self.nodes[id].parent;
        }
        false
    }

    fn height(&self, id: usize) -> usize {
        1 + self.nodes[id]
            .children
            .iter()
            .map(|&child| self.height(child))
            .max()
            .unwrap_or(0)
    }

    fn depth(&self, mut id: usize) -> usize {
        let mut depth = 0;
        while let Some(parent) = self.nodes[id].parent {
            depth += 1;
            id = parent;
        }
        depth
    }

    fn detach(&mut self, child: usize) {
        if let Some(parent) = self.nodes[child].parent.take() {
            self.nodes[parent]
                .children
                .retain(|&existing| existing != child);
        }
    }

    fn deep_clone(&mut self, source: usize) -> usize {
        let kind = match &self.nodes[source].kind {
            ModelKind::Root => unreachable!("the compiler never clones the root"),
            ModelKind::Element {
                tag,
                namespace,
                attributes,
                ..
            } => ModelKind::Element {
                tag: tag.clone(),
                namespace: namespace.clone(),
                attributes: attributes.clone(),
                listeners: BTreeSet::new(),
                element_id: None,
            },
            ModelKind::Text(text) => ModelKind::Text(text.clone()),
        };
        let cloned = self.alloc(kind);
        let children: Vec<usize> = self.nodes[source].children.clone();
        let cloned_children: Vec<usize> = children
            .into_iter()
            .map(|child| self.deep_clone(child))
            .collect();
        for &child in &cloned_children {
            self.nodes[child].parent = Some(cloned);
        }
        self.nodes[cloned].children = cloned_children;
        cloned
    }

    fn listener_id(&self, id: usize, tag: &str, event: &str) -> Option<NodeId> {
        if let ModelKind::Element {
            tag: node_tag,
            listeners,
            element_id: Some(element_id),
            ..
        } = &self.nodes[id].kind
            && node_tag == tag
            && listeners.contains(event)
        {
            return Some(*element_id);
        }
        self.nodes[id]
            .children
            .iter()
            .find_map(|&child| self.listener_id(child, tag, event))
    }

    fn dump(&self, id: usize, out: &mut String) {
        let node = &self.nodes[id];
        match &node.kind {
            ModelKind::Root => out.push_str("(root"),
            ModelKind::Text(text) => {
                write!(out, "(text {text:?})").unwrap();
                return;
            }
            ModelKind::Element {
                tag,
                namespace,
                attributes,
                listeners,
                element_id,
            } => {
                write!(
                    out,
                    "(element {tag} {namespace:?} {element_id:?} {attributes:?} {listeners:?}"
                )
                .unwrap();
            }
        }
        for &child in &node.children {
            out.push(' ');
            self.dump(child, out);
        }
        out.push(')');
    }
}

// ── compiler: maps abstract operations onto the live model state, skipping any whose
//    preconditions do not hold, so emitted batches exercise acceptance rather than rejection ──

#[derive(Clone, Copy, Debug)]
enum Attach {
    Append,
    Replace,
    InsertAfter,
    InsertBefore,
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Push(usize),
    Set(usize),
    Child(usize),
    Pop,
    CreateElement(usize),
    CreateText(usize),
    CloneTop,
    Attach(Attach, usize),
    SetAttribute(usize, usize),
    ClearAttribute(usize),
    SetText(usize),
    AddListener(usize),
    DropListener(usize),
    Remove,
}

struct Compiler {
    model: Model,
    ids: BTreeMap<NodeId, usize>,
    stack: Vec<usize>,
    batch: Vec<Mutation>,
}

impl Compiler {
    fn new() -> Self {
        Self {
            model: Model::new(),
            ids: BTreeMap::from([(0, 0)]),
            stack: vec![0],
            batch: Vec::new(),
        }
    }

    fn top(&self) -> usize {
        *self.stack.last().expect("model stack starts at root")
    }

    fn set_element_id(&mut self, node: usize, id: NodeId) {
        if let Some(previous) = self.ids.get(&id).copied()
            && previous != node
            && let ModelKind::Element { element_id, .. } = &mut self.model.nodes[previous].kind
            && *element_id == Some(id)
        {
            *element_id = None;
        }
        self.ids.insert(id, node);
        if let ModelKind::Element { element_id, .. } = &mut self.model.nodes[node].kind {
            *element_id = Some(id);
        }
    }

    fn drop_subtree(&mut self, root: usize) {
        let node = &mut self.model.nodes[root];
        node.live = false;
        if let ModelKind::Element {
            element_id: Some(id),
            ..
        } = &node.kind
            && self.ids.get(id) == Some(&root)
        {
            self.ids.remove(id);
        }
        let children = node.children.clone();
        for child in children {
            self.drop_subtree(child);
        }
    }

    /// Mirrors `validate_attachment`: returns the new parent for `selected` when the attachment
    /// is valid in the current model state.
    fn attachment_parent(&self, attach: Attach, count: usize) -> Option<usize> {
        if count >= self.stack.len() {
            return None;
        }
        let split = self.stack.len() - count;
        let anchor = self.stack[split - 1];
        let parent = match attach {
            Attach::Append => match &self.model.nodes[anchor].kind {
                ModelKind::Root | ModelKind::Element { .. } => anchor,
                ModelKind::Text(_) => return None,
            },
            _ => {
                if anchor == 0 || (matches!(attach, Attach::Replace) && split <= 1) {
                    return None;
                }
                self.model.nodes[anchor].parent?
            }
        };
        let selected: BTreeSet<usize> = self.stack[split..].iter().copied().collect();
        if selected.len() != count || selected.contains(&0) {
            return None;
        }
        let mut ancestor = Some(anchor);
        while let Some(id) = ancestor {
            if selected.contains(&id) {
                return None;
            }
            ancestor = self.model.nodes[id].parent;
        }
        for &child in &selected {
            let mut ancestor = self.model.nodes[child].parent;
            while let Some(id) = ancestor {
                if selected.contains(&id) {
                    return None;
                }
                ancestor = self.model.nodes[id].parent;
            }
            if self.model.depth(parent) + self.model.height(child) > DEPTH_CAP {
                return None;
            }
        }
        // Replacing drops the anchor subtree; any other stack entry inside it would go stale.
        if matches!(attach, Attach::Replace)
            && self.stack[..split - 1]
                .iter()
                .any(|&entry| self.model.within(anchor, entry))
        {
            return None;
        }
        Some(parent)
    }

    fn compile(&mut self, op: Op) {
        match op {
            Op::Push(index) => {
                // Only live nodes are pushable; the tree rejects a stale id mapping as
                // `InvalidBatch`, so the compiler must not emit one.
                let live: Vec<(NodeId, usize)> = self
                    .ids
                    .iter()
                    .filter(|&(_, &node)| self.model.nodes[node].live)
                    .map(|(&id, &node)| (id, node))
                    .collect();
                if !live.is_empty() {
                    let (id, node) = live[index % live.len()];
                    self.stack.push(node);
                    self.batch.push(Mutation::PushId(id));
                }
            }
            Op::Set(id) => {
                let top = self.top();
                let id = id as NodeId;
                if (id == 0) == (top == 0) {
                    self.set_element_id(top, id);
                    self.batch.push(Mutation::SetId(id));
                }
            }
            Op::Child(index) => {
                let top = self.top();
                let children = &self.model.nodes[top].children;
                if self.stack.len() > 1 && !children.is_empty() {
                    let index = index % children.len();
                    *self.stack.last_mut().unwrap() = children[index];
                    self.batch.push(Mutation::Child(index as u32));
                }
            }
            Op::Pop => {
                if self.stack.len() > 1 {
                    self.stack.pop();
                    self.batch.push(Mutation::Pop);
                }
            }
            Op::CreateElement(index) => {
                if self.stack.len() < STACK_CAP {
                    let tag = TAGS[index % TAGS.len()];
                    let node = self.model.alloc(ModelKind::Element {
                        tag: tag.into(),
                        namespace: None,
                        attributes: BTreeMap::new(),
                        listeners: BTreeSet::new(),
                        element_id: None,
                    });
                    self.stack.push(node);
                    self.batch.push(Mutation::CreateElement((tag.into(), None)));
                }
            }
            Op::CreateText(index) => {
                if self.stack.len() < STACK_CAP {
                    let text = TEXTS[index % TEXTS.len()];
                    let node = self.model.alloc(ModelKind::Text(text.into()));
                    self.stack.push(node);
                    self.batch.push(Mutation::CreateText(text.into()));
                }
            }
            Op::CloneTop => {
                let top = self.top();
                if top != 0 && self.stack.len() > 1 && self.stack.len() < STACK_CAP {
                    let cloned = self.model.deep_clone(top);
                    *self.stack.last_mut().unwrap() = cloned;
                    self.batch.push(Mutation::CloneNode);
                }
            }
            Op::Attach(attach, count) => {
                if let Some(parent) = self.attachment_parent(attach, count) {
                    let split = self.stack.len() - count;
                    let selected: Vec<usize> = self.stack.split_off(split);
                    for &node in &selected {
                        self.model.detach(node);
                        self.model.nodes[node].parent = Some(parent);
                    }
                    match attach {
                        Attach::Append => {
                            self.model.nodes[parent].children.extend(&selected);
                            self.batch.push(Mutation::AppendChildren(count as u32));
                        }
                        Attach::Replace => {
                            let target = self.stack.pop().unwrap();
                            let siblings = &mut self.model.nodes[parent].children;
                            let position = siblings
                                .iter()
                                .position(|&existing| existing == target)
                                .unwrap();
                            siblings.splice(position..=position, selected);
                            self.drop_subtree(target);
                            self.batch.push(Mutation::ReplaceWith(count as u32));
                        }
                        Attach::InsertAfter => {
                            let anchor = self.top();
                            let siblings = &mut self.model.nodes[parent].children;
                            let position = siblings
                                .iter()
                                .position(|&existing| existing == anchor)
                                .unwrap();
                            siblings.splice(position + 1..position + 1, selected);
                            self.batch.push(Mutation::InsertAfter(count as u32));
                        }
                        Attach::InsertBefore => {
                            let anchor = self.top();
                            let siblings = &mut self.model.nodes[parent].children;
                            let position = siblings
                                .iter()
                                .position(|&existing| existing == anchor)
                                .unwrap();
                            siblings.splice(position..position, selected);
                            self.batch.push(Mutation::InsertBefore(count as u32));
                        }
                    }
                }
            }
            Op::SetAttribute(attribute, value) => {
                let top = self.top();
                if let ModelKind::Element { attributes, .. } = &mut self.model.nodes[top].kind {
                    let (name, namespace) = ATTRIBUTES[attribute % ATTRIBUTES.len()];
                    let value = attribute_value(value);
                    attributes.insert((name.into(), namespace.map(str::to_owned)), value.clone());
                    self.batch.push(Mutation::SetAttribute((
                        name.into(),
                        namespace.map(str::to_owned),
                        value,
                    )));
                }
            }
            Op::ClearAttribute(attribute) => {
                let top = self.top();
                if let ModelKind::Element { attributes, .. } = &mut self.model.nodes[top].kind {
                    let (name, namespace) = ATTRIBUTES[attribute % ATTRIBUTES.len()];
                    attributes.remove(&(name.into(), namespace.map(str::to_owned)));
                    self.batch.push(Mutation::SetAttribute((
                        name.into(),
                        namespace.map(str::to_owned),
                        AttributeValue::None,
                    )));
                }
            }
            Op::SetText(index) => {
                let top = self.top();
                let text = TEXTS[index % TEXTS.len()];
                enum Target {
                    Skip,
                    Text,
                    Element,
                }
                let target = match &self.model.nodes[top].kind {
                    ModelKind::Root => Target::Skip,
                    ModelKind::Text(_) => Target::Text,
                    ModelKind::Element { .. } => Target::Element,
                };
                match target {
                    Target::Skip => {}
                    Target::Text => {
                        self.model.nodes[top].kind = ModelKind::Text(text.into());
                        self.batch.push(Mutation::SetText(text.into()));
                    }
                    Target::Element => {
                        // The tree drops the element's children here; a stack entry inside
                        // one of those subtrees would go stale and revoke the batch.
                        let children = self.model.nodes[top].children.clone();
                        let stale = children.iter().any(|&child| {
                            self.stack[..self.stack.len() - 1]
                                .iter()
                                .any(|&entry| self.model.within(child, entry))
                        });
                        if !stale {
                            for child in children {
                                self.drop_subtree(child);
                            }
                            let text_node = self.model.alloc(ModelKind::Text(text.into()));
                            self.model.nodes[text_node].parent = Some(top);
                            self.model.nodes[top].children = vec![text_node];
                            self.batch.push(Mutation::SetText(text.into()));
                        }
                    }
                }
            }
            Op::AddListener(index) => {
                let top = self.top();
                if let ModelKind::Element {
                    listeners,
                    element_id: Some(_),
                    ..
                } = &mut self.model.nodes[top].kind
                {
                    let name = LISTENERS[index % LISTENERS.len()];
                    listeners.insert(name.into());
                    self.batch.push(Mutation::AddEventListener(name.into()));
                }
            }
            Op::DropListener(index) => {
                let top = self.top();
                if let ModelKind::Element { listeners, .. } = &mut self.model.nodes[top].kind {
                    let name = LISTENERS[index % LISTENERS.len()];
                    listeners.remove(name);
                    self.batch.push(Mutation::RemoveEventListener(name.into()));
                }
            }
            Op::Remove => {
                let top = self.top();
                let stale = self.stack[..self.stack.len() - 1]
                    .iter()
                    .any(|&entry| self.model.within(top, entry));
                if top != 0 && self.stack.len() > 1 && !stale {
                    self.stack.pop();
                    self.model.detach(top);
                    self.drop_subtree(top);
                    self.batch.push(Mutation::Remove);
                }
            }
        }
    }

    /// A batch must return the stack to the root; trailing pops balance whatever the stream opened.
    fn balance(&mut self) {
        while self.stack.len() > 1 {
            self.stack.pop();
            self.batch.push(Mutation::Pop);
        }
    }
}

// ── oracle comparison: full structural dump, listener resolution, retained-byte accounting ──

fn dump(tree: &SurfaceTree, id: SurfaceNodeId, out: &mut String) {
    let node = tree.node(id);
    match &node.kind {
        SurfaceNodeKind::Root => out.push_str("(root"),
        SurfaceNodeKind::Text(text) => {
            write!(out, "(text {text:?})").unwrap();
            return;
        }
        SurfaceNodeKind::Component(()) => {
            out.push_str("(component)");
            return;
        }
        SurfaceNodeKind::Element {
            tag,
            namespace,
            attributes,
            listeners,
            element_id,
        } => {
            let element_id = element_id.map(|id| id.raw() as NodeId);
            write!(
                out,
                "(element {tag} {namespace:?} {element_id:?} {attributes:?} {listeners:?}"
            )
            .unwrap();
        }
    }
    for &child in &node.children {
        out.push(' ');
        dump(tree, child, out);
    }
    out.push(')');
}

fn assert_matches_model(tree: &SurfaceTree, compiler: &Compiler) {
    assert_eq!(tree.error(), None);
    assert_eq!(tree.stack.len(), 1);
    assert_eq!(tree.stack[0].node, SurfaceNodeId::ROOT);
    let mut actual = String::new();
    dump(tree, SurfaceNodeId::ROOT, &mut actual);
    let mut expected = String::new();
    compiler.model.dump(0, &mut expected);
    assert_eq!(actual, expected);
    assert_eq!(tree.text_content(), {
        let mut text = String::new();
        collect_model_text(&compiler.model, 0, &mut text);
        text
    });
    for (tag, event) in [("button", "click"), ("div", "click"), ("p", "input")] {
        assert_eq!(
            tree.listener_id(tag, event).map(|id| id.raw() as NodeId),
            compiler.model.listener_id(0, tag, event),
            "listener {tag}/{event}"
        );
    }
    assert_eq!(
        tree.retained_bytes,
        tree.nodes
            .iter()
            .flatten()
            .map(|node| SurfaceTree::kind_bytes(&node.kind))
            .sum::<usize>()
    );
}

fn collect_model_text(model: &Model, id: usize, out: &mut String) {
    if let ModelKind::Text(text) = &model.nodes[id].kind {
        out.push_str(text);
    }
    for &child in &model.nodes[id].children {
        collect_model_text(model, child, out);
    }
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0usize..8).prop_map(Op::Push),
        3 => (0usize..16).prop_map(Op::Set),
        2 => (0usize..4).prop_map(Op::Child),
        3 => Just(Op::Pop),
        4 => (0usize..4).prop_map(Op::CreateElement),
        3 => (0usize..4).prop_map(Op::CreateText),
        1 => Just(Op::CloneTop),
        4 => (
            prop_oneof![
                Just(Attach::Append),
                Just(Attach::Replace),
                Just(Attach::InsertAfter),
                Just(Attach::InsertBefore),
            ],
            0usize..4,
        )
            .prop_map(|(attach, count)| Op::Attach(attach, count)),
        3 => (0usize..3, 0usize..5).prop_map(|(attribute, value)| Op::SetAttribute(attribute, value)),
        1 => (0usize..3).prop_map(Op::ClearAttribute),
        3 => (0usize..4).prop_map(Op::SetText),
        2 => (0usize..2).prop_map(Op::AddListener),
        1 => (0usize..2).prop_map(Op::DropListener),
        2 => Just(Op::Remove),
    ]
}

fn stream() -> impl Strategy<Value = Vec<Vec<Op>>> {
    prop::collection::vec(prop::collection::vec(op(), 0..30), 1..5)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn valid_mutation_streams_match_the_model_oracle(batches in stream()) {
        let mut tree = SurfaceTree::new();
        let mut compiler = Compiler::new();
        for ops in &batches {
            for &op in ops {
                compiler.compile(op);
            }
            compiler.balance();
            let batch = std::mem::take(&mut compiler.batch);
            // The compiler only emits model-valid mutations, so acceptance is itself pinned:
            // a rejection here means tree and model disagree about the wire semantics.
            tree.apply(&batch)
                .unwrap_or_else(|error| panic!("model-valid batch rejected: {error}\n{batch:?}"));
            assert_matches_model(&tree, &compiler);
        }
    }

    #[test]
    fn replay_matches_checked_apply_on_every_model_accepted_batch(batches in stream()) {
        let mut checked = SurfaceTree::new();
        let mut trusted = SurfaceTree::new();
        let mut compiler = Compiler::new();
        for ops in &batches {
            for &op in ops {
                compiler.compile(op);
            }
            compiler.balance();
            let batch = std::mem::take(&mut compiler.batch);
            checked.apply(&batch).expect("model-valid batch");
            // The generator never emits component slots, so trusted replay must accept the batch.
            replay(&batch, &mut trusted).expect("checked batch replays");
            let mut checked_dump = String::new();
            dump(&checked, SurfaceNodeId::ROOT, &mut checked_dump);
            let mut trusted_dump = String::new();
            dump(&trusted, SurfaceNodeId::ROOT, &mut trusted_dump);
            assert_eq!(checked_dump, trusted_dump);
            assert_eq!(checked.retained_bytes, trusted.retained_bytes);
        }
    }
}

// ── unfiltered streams: any mutation sequence may fail, but never panic, and failure is
//    always the terminal contract (cleared tree, pinned error, permanent revocation) ──────

fn any_mutation() -> impl Strategy<Value = Mutation> {
    prop_oneof![
        3 => (0u32..8).prop_map(Mutation::PushId),
        1 => (8192u32..8200).prop_map(Mutation::PushId),
        3 => (0u32..8).prop_map(Mutation::SetId),
        1 => (8192u32..8200).prop_map(Mutation::SetId),
        2 => (0u32..4).prop_map(Mutation::Child),
        1 => Just(Mutation::Child(u32::MAX)),
        4 => Just(Mutation::Pop),
        2 => (0usize..4).prop_map(|index| Mutation::CreateElement((TAGS[index].into(), None))),
        1 => Just(Mutation::CreateElement(("unknown-widget".into(), None))),
        2 => (0usize..4).prop_map(|index| Mutation::CreateText(TEXTS[index].into())),
        1 => Just(Mutation::CloneNode),
        2 => (0u32..4).prop_map(Mutation::AppendChildren),
        1 => (0u32..3).prop_map(Mutation::ReplaceWith),
        1 => (0u32..3).prop_map(Mutation::InsertAfter),
        1 => (0u32..3).prop_map(Mutation::InsertBefore),
        1 => Just(Mutation::AppendChildren(u32::MAX)),
        2 => (0usize..3, 0usize..5).prop_map(|(attribute, value)| {
            let (name, namespace) = ATTRIBUTES[attribute];
            Mutation::SetAttribute((name.into(), namespace.map(str::to_owned), attribute_value(value)))
        }),
        1 => (0usize..4).prop_map(|index| Mutation::SetText(TEXTS[index].into())),
        2 => (0usize..2).prop_map(|index| Mutation::AddEventListener(LISTENERS[index].into())),
        1 => (0usize..2).prop_map(|index| Mutation::RemoveEventListener(LISTENERS[index].into())),
        1 => Just(Mutation::Remove),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn unfiltered_mutation_streams_never_panic_and_fail_terminally(
        batches in prop::collection::vec(prop::collection::vec(any_mutation(), 0..24), 1..4)
    ) {
        let mut tree = SurfaceTree::new();
        for batch in &batches {
            match tree.apply(batch) {
                Ok(_) => {
                    assert_eq!(tree.error(), None, "{batch:?}");
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
                    assert_eq!(tree.apply(&[]), Err(PresentationError::Revoked), "{batch:?}");
                    return Ok(());
                }
            }
        }
    }
}
