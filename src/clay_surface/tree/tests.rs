mod property;
use super::*;
use crate::replay::replay;
use crate::wire::EventResponse;
use crate::wire::SurfaceEventHandler;
use dioxus::prelude::*;
use std::rc::Rc;
type SurfaceTree = super::SurfaceTree<()>;
type Mutation = super::Mutation<()>;

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
        let rendered = tree.render_node(child, &dispatch, &|_| {
            unreachable!("test has no component slots")
        })?;
        props
            .keys
            .borrow_mut()
            .push(rendered.key().map(str::to_owned));
        tree.render(&dispatch, &|_| unreachable!("test has no component slots"))
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

#[test]
fn valid_static_attributes_namespaces_and_listeners_project_into_host_vdom() {
    #[derive(Clone, PartialEq)]
    struct Props {
        tree: SurfaceTree,
    }
    fn app(props: Props) -> Element {
        let dispatch: SurfaceEventHandler = Rc::new(|_| EventResponse::default());
        props
            .tree
            .render(&dispatch, &|_| unreachable!("test has no component slots"))
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
