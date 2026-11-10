use super::*;
type SurfaceTree = clay_surface::SurfaceTree<ComponentRef>;
type Mutation = clay_surface::Mutation<ComponentRef>;
use clay_surface::AttributeValue;
use dioxus_core::RenderTargetId;
use dioxus_core::VirtualDom;
use dioxus_html::EventData;
use dioxus_html::SerializedMouseData;
use std::cell::Cell;

#[tokio::test]
async fn wire_slots_mount_through_context_preserve_identity_and_revoke_nested_surfaces() {
    let host = Host::new();
    let components = NativeComponents::new(&host);
    let viewer = host.start("viewer");
    let layout = host.start("layout");
    let child = host.start("child");
    let mounts = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    let child_writers = Rc::new(RefCell::new(Vec::new()));
    let (reference, _child_surface) = components
        .publish_surface(&child, {
            let mounts = mounts.clone();
            let drops = drops.clone();
            let writers = child_writers.clone();
            move |_, writer| {
                mounts.set(mounts.get() + 1);
                writer.apply(&counter_initial_batch())?;
                writers.borrow_mut().push(writer.clone());
                let drops = drops.clone();
                Ok(SurfaceSession::with_drop(
                    |_| EventResponse::default(),
                    move || drops.set(drops.get() + 1),
                ))
            }
        })
        .unwrap();
    let parent_writer = Rc::new(RefCell::new(None));
    let (parent, _layout_surface) = components
        .publish_surface(&layout, {
            let parent_writer = parent_writer.clone();
            move |_, writer| {
                writer.apply(&[
                    Mutation::MountComponent(reference),
                    Mutation::SetId(1),
                    Mutation::AppendChildren(1),
                ])?;
                *parent_writer.borrow_mut() = Some(writer);
                Ok(SurfaceSession::new(|_| EventResponse::default()))
            }
        })
        .unwrap();
    fn app(props: (NativeComponents, Instance, ComponentRef)) -> Element {
        use_context_provider(|| props.0.clone());
        rsx! { ComponentMount { owner: props.1, reference: props.2 } }
    }
    let mut dom = VirtualDom::new_with_props(app, (components, viewer, parent));
    let mut tree = SurfaceTree::new();
    dom.rebuild(&mut tree);
    assert_eq!(tree.text_content(), "0");
    assert_eq!(mounts.get(), 1);
    let parent_writer = parent_writer.borrow().clone().unwrap();
    parent_writer.apply(&[]).unwrap();
    dom.wait_for_work().await;
    dom.render_immediate(&mut tree);
    assert_eq!((mounts.get(), drops.get()), (1, 0));
    parent_writer
        .apply(&[
            Mutation::PushId(1),
            Mutation::MountComponent(reference),
            Mutation::SetId(1),
            Mutation::ReplaceWith(1),
        ])
        .unwrap();
    dom.wait_for_work().await;
    dom.render_immediate(&mut tree);
    assert_eq!((mounts.get(), drops.get()), (2, 1));
    assert!(!child_writers.borrow()[0].is_live());
    assert!(child_writers.borrow()[1].is_live());
    parent_writer.withdraw();
    assert!(!child_writers.borrow()[1].is_live());
    assert_eq!(
        child_writers.borrow()[1].apply(&[]),
        Err(PresentationError::Revoked)
    );
    assert_eq!(host.check(&child), Ok(()));
    dom.wait_for_work().await;
    dom.render_immediate(&mut tree);
    assert_eq!(drops.get(), 2);
    drop(dom);
    host.close();
}

#[test]
fn recursive_component_slots_are_rejected_without_recursing_into_the_factory() {
    let host = Host::new();
    let components = NativeComponents::new(&host);
    let owner = host.start("recursive layout");
    let self_reference = Rc::new(Cell::new(None));
    let mounts = Rc::new(Cell::new(0));
    let (reference, _surface) = components
        .publish_surface(&owner, {
            let self_reference = self_reference.clone();
            let mounts = mounts.clone();
            move |_, writer| {
                mounts.set(mounts.get() + 1);
                writer.apply(&[
                    Mutation::MountComponent(self_reference.get().unwrap()),
                    Mutation::AppendChildren(1),
                ])?;
                Ok(SurfaceSession::new(|_| EventResponse::default()))
            }
        })
        .unwrap();
    self_reference.set(Some(reference));
    fn app(props: (NativeComponents, Instance, ComponentRef)) -> Element {
        rsx! { ComponentMount { components: props.0, owner: props.1, reference: props.2 } }
    }
    let mut dom = VirtualDom::new_with_props(app, (components, owner, reference));
    let mut tree = SurfaceTree::new();
    dom.rebuild(&mut tree);
    assert_eq!(mounts.get(), 1);
    assert_eq!(
        tree.text_content(),
        "Component unavailable: cyclic or excessively deep mount."
    );
    host.close();
}

/// A presentation publication mints its reference when the incarnation publishes it, before
/// any renderer knows it; attaching the engine's surface factory afterwards makes it mountable
/// exactly like one `publish_surface` issued.
#[test]
fn a_surface_attached_to_an_issued_reference_mounts_like_a_published_one() {
    let host = Host::new();
    let components = NativeComponents::new(&host);
    let owner = host.start("publisher");
    let reference = host.publish_component(&owner).unwrap();
    let mounts = Rc::new(Cell::new(0));
    let _surface = components.attach_surface(&owner, reference, {
        let mounts = mounts.clone();
        move |_, writer| {
            mounts.set(mounts.get() + 1);
            writer.apply(&counter_initial_batch())?;
            Ok(SurfaceSession::new(|_| EventResponse::default()))
        }
    });
    fn app(props: (NativeComponents, Instance, ComponentRef)) -> Element {
        rsx! { ComponentMount { components: props.0, owner: props.1, reference: props.2 } }
    }
    let mut dom = VirtualDom::new_with_props(app, (components, owner, reference));
    let mut tree = SurfaceTree::new();
    dom.rebuild(&mut tree);
    assert_eq!(mounts.get(), 1);
    assert_eq!(tree.text_content(), "0");
    host.close();
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

fn mounted_writer() -> (Host, Instance, Arc<Mount>, SurfaceWriter) {
    let host = Host::new();
    let owner = host.start("surface owner");
    let reference = host.publish_component(&owner).unwrap();
    let mount = Arc::new(host.mount(&owner, reference).unwrap());
    let writer = SurfaceWriter::new(mount.clone());
    (host, owner, mount, writer)
}

#[test]
fn withdraw_revokes_all_clones_without_closing_the_mount_or_sibling_surface() {
    let (_host, _owner, mount, writer) = mounted_writer();
    let clone = writer.clone();
    let sibling = SurfaceWriter::new(mount.clone());
    writer.apply(&counter_initial_batch()).unwrap();
    sibling.apply(&counter_initial_batch()).unwrap();
    let updates = writer.revisions.subscribe();
    writer.withdraw();
    assert!(mount.is_live());
    assert!(!clone.is_live());
    assert_eq!(
        clone.apply(&counter_initial_batch()),
        Err(PresentationError::Revoked)
    );
    assert!(writer.tree.lock().unwrap().is_empty());
    assert_eq!(*updates.borrow(), writer.revision());
    let revision = writer.revision();
    clone.withdraw();
    assert_eq!(writer.revision(), revision);
    sibling
        .apply(&counter_text_update_batch("sibling"))
        .unwrap();
    assert_eq!(sibling.tree.lock().unwrap().text_content(), "sibling");
}

#[test]
fn withdrawal_before_first_apply_permanently_revokes_escaped_clones() {
    let (_host, _owner, mount, writer) = mounted_writer();
    let clone = writer.clone();
    let mut updates = writer.revisions.subscribe();
    writer.withdraw();
    assert!(mount.is_live());
    assert!(!writer.is_live());
    assert!(!clone.is_live());
    assert_eq!(clone.apply(&[]), Err(PresentationError::Revoked));
    assert_eq!(
        clone.apply(&counter_initial_batch()),
        Err(PresentationError::Revoked)
    );
    assert_eq!(*updates.borrow_and_update(), writer.revision());
    let revision = writer.revision();
    clone.withdraw();
    assert_eq!(writer.revision(), revision);
    assert!(!updates.has_changed().unwrap());
    assert!(!writer.tree.is_poisoned());
}

#[test]
fn invalid_batch_publishes_only_terminal_clear_and_preserves_sibling_revision() {
    let (_host, _owner, mount, writer) = mounted_writer();
    let sibling = SurfaceWriter::new(mount.clone());
    writer.apply(&counter_initial_batch()).unwrap();
    sibling.apply(&counter_initial_batch()).unwrap();
    let updates = writer.revisions.subscribe();
    let mut sibling_updates = sibling.revisions.subscribe();
    let sibling_revision = *sibling_updates.borrow_and_update();
    let revision = writer.revision();
    let mut edits = counter_text_update_batch("must not be published");
    edits.push(Mutation::Pop);
    let error = writer.apply(&edits).unwrap_err();
    assert!(matches!(error, PresentationError::InvalidBatch(_)));
    assert_eq!(*updates.borrow(), revision + 1);
    assert_eq!(writer.revision(), revision + 1);
    let tree = writer.tree.lock().unwrap();
    assert!(tree.is_empty());
    assert_eq!(tree.error(), Some(&error));
    drop(tree);
    assert!(mount.is_live());
    assert!(!writer.tree.is_poisoned());
    assert_eq!(sibling.revision(), sibling_revision);
    assert!(!sibling_updates.has_changed().unwrap());
    assert_eq!(sibling.tree.lock().unwrap().text_content(), "0");
    sibling
        .apply(&counter_text_update_batch("still live"))
        .unwrap();
    assert_eq!(sibling.tree.lock().unwrap().text_content(), "still live");
}

#[test]
fn malformed_guest_batches_do_not_panic_or_poison_the_shared_surface_mutex() {
    for edits in [
        vec![Mutation::Pop],
        vec![Mutation::PushId(u32::MAX)],
        vec![Mutation::PushId(1), Mutation::Child(u32::MAX)],
        vec![
            Mutation::PushId(1),
            Mutation::PushId(1),
            Mutation::AppendChildren(1),
        ],
        vec![
            Mutation::PushId(1),
            Mutation::AddEventListener("unsupported".into()),
        ],
        vec![Mutation::CreateText(
            "x".repeat(SurfaceTree::MAX_STRING_BYTES + 1),
        )],
    ] {
        let (_host, _owner, mount, writer) = mounted_writer();
        let clone = writer.clone();
        let sibling = SurfaceWriter::new(mount.clone());
        writer.apply(&counter_initial_batch()).unwrap();
        sibling.apply(&counter_initial_batch()).unwrap();
        let updates = writer.revisions.subscribe();
        let error = writer.apply(&edits).unwrap_err();
        assert!(matches!(
            error,
            PresentationError::InvalidBatch(_) | PresentationError::Unsupported(_)
        ));
        assert!(!writer.tree.is_poisoned());
        let tree = writer.tree.lock().unwrap();
        assert_eq!(tree.error(), Some(&error));
        assert!(tree.is_empty());
        assert_eq!(*updates.borrow(), tree.revision());
        drop(tree);
        assert!(mount.is_live());
        assert!(!clone.is_live());
        assert_eq!(
            clone.apply(&counter_initial_batch()),
            Err(PresentationError::Revoked)
        );
        clone.withdraw();
        sibling.apply(&counter_text_update_batch("alive")).unwrap();
        assert_eq!(sibling.tree.lock().unwrap().text_content(), "alive");
    }
}

#[test]
fn concurrent_apply_and_withdraw_cannot_resurrect_or_reorder_surface_publication() {
    for _ in 0..16 {
        let (_host, _owner, mount, writer) = mounted_writer();
        writer.apply(&counter_initial_batch()).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let apply_writer = writer.clone();
        let apply_barrier = barrier.clone();
        let apply = std::thread::spawn(move || {
            apply_barrier.wait();
            apply_writer.apply(&counter_text_update_batch("racing"))
        });
        let withdraw_writer = writer.clone();
        let withdraw_barrier = barrier.clone();
        let withdraw = std::thread::spawn(move || {
            withdraw_barrier.wait();
            withdraw_writer.withdraw();
        });
        barrier.wait();
        assert!(matches!(
            apply.join().unwrap(),
            Ok(_) | Err(PresentationError::Revoked)
        ));
        withdraw.join().unwrap();
        assert!(mount.is_live());
        assert!(!writer.is_live());
        assert_eq!(
            writer.apply(&counter_initial_batch()),
            Err(PresentationError::Revoked)
        );
        assert!(writer.tree.lock().unwrap().is_empty());
        assert_eq!(*writer.revisions.borrow(), writer.revision());
    }
}

#[test]
fn failed_factory_reports_the_error_and_revokes_escaped_writers() {
    let (_host, owner, mount, _writer) = mounted_writer();
    let slot = Rc::new(RefCell::new(None));
    let capture = slot.clone();
    let factory = SurfaceFactory(Rc::new(move |_mount, writer| {
        writer.apply(&counter_initial_batch())?;
        *capture.borrow_mut() = Some(writer);
        Err(PresentationError::Unsupported("factory failed".into()))
    }));
    #[derive(Clone, PartialEq)]
    struct Props {
        mount: SharedMount,
        owner: Instance,
        factory: SurfaceFactory,
    }
    fn app(props: Props) -> Element {
        rsx! { MountedSurface { mount: props.mount, owner: props.owner, factory: props.factory } }
    }
    let mut dom = VirtualDom::new_with_props(
        app,
        Props {
            mount: SharedMount(mount.clone()),
            owner,
            factory,
        },
    );
    let mut tree = SurfaceTree::new();
    dom.rebuild(&mut tree);
    assert!(tree.text_content().contains("factory failed"));
    let writer = slot.borrow().clone().unwrap();
    assert!(mount.is_live());
    assert!(!writer.is_live());
    assert_eq!(
        writer.apply(&counter_initial_batch()),
        Err(PresentationError::Revoked)
    );
    assert!(writer.tree.lock().unwrap().is_empty());
}

#[test]
fn stale_host_listener_is_suppressed_after_external_apply_then_current_listener_dispatches() {
    let (_host, owner, mount, _writer) = mounted_writer();
    let slot = Rc::new(RefCell::new(None));
    let capture = slot.clone();
    let received = Rc::new(RefCell::new(Vec::new()));
    let received_capture = received.clone();
    let factory = SurfaceFactory(Rc::new(move |_mount, writer| {
        writer.apply(&counter_initial_batch())?;
        *capture.borrow_mut() = Some(writer.clone());
        let received = received_capture.clone();
        Ok(SurfaceSession::new(move |event| {
            assert_eq!(event.name, "click");
            received.borrow_mut().push((event.revision, event.element));
            writer
                .apply(&counter_text_update_batch(
                    &received.borrow().len().to_string(),
                ))
                .unwrap();
            EventResponse::default()
        }))
    }));
    #[derive(Clone, PartialEq)]
    struct Props {
        mount: SharedMount,
        owner: Instance,
        factory: SurfaceFactory,
    }
    fn app(props: Props) -> Element {
        rsx! { MountedSurface { mount: props.mount, owner: props.owner, factory: props.factory } }
    }
    fn click(dom: &VirtualDom, id: dioxus_core::ElementId) {
        dom.runtime().handle_event_for_target(
            RenderTargetId::ROOT,
            "click",
            dioxus_core::Event::new(
                EventData::Mouse(SerializedMouseData::default()).into_any(),
                true,
            ),
            id,
        );
    }
    let mut dom = VirtualDom::new_with_props(
        app,
        Props {
            mount: SharedMount(mount),
            owner,
            factory,
        },
    );
    let mut host_tree = SurfaceTree::new();
    dom.rebuild(&mut host_tree);
    let stale_host_id = host_tree.listener_id("button", "click").unwrap();
    let writer = slot.borrow().clone().unwrap();
    let previous_revision = writer.revision();
    let mut replacement = vec![Mutation::PushId(1), Mutation::Remove];
    replacement.extend(counter_initial_batch());
    replacement.extend(counter_text_update_batch("replacement"));
    let current_revision = writer.apply(&replacement).unwrap();
    assert_eq!(current_revision, previous_revision + 1);
    assert_eq!(host_tree.text_content(), "0");
    click(&dom, stale_host_id);
    assert!(received.borrow().is_empty());
    assert_eq!(writer.revision(), current_revision);
    assert_eq!(writer.tree.lock().unwrap().text_content(), "replacement");
    dom.render_immediate(&mut host_tree);
    assert_eq!(host_tree.text_content(), "replacement");
    let current_host_id = host_tree.listener_id("button", "click").unwrap();
    click(&dom, current_host_id);
    assert_eq!(&*received.borrow(), &[(current_revision, 1)]);
    dom.render_immediate(&mut host_tree);
    assert_eq!(host_tree.text_content(), "1");
    let current_host_id = host_tree.listener_id("button", "click").unwrap();
    click(&dom, current_host_id);
    assert_eq!(
        &*received.borrow(),
        &[(current_revision, 1), (current_revision + 1, 1)]
    );
    dom.render_immediate(&mut host_tree);
    assert_eq!(host_tree.text_content(), "2");
}

#[test]
fn surface_mount_renders_in_host_vdom_dispatches_reentrantly_and_revokes_on_instance_close() {
    let host = Host::new();
    let components = NativeComponents::new(&host);
    let viewer = host.start("viewer");
    let micro = host.start("micro");

    let dropped = Rc::new(Cell::new(false));
    let dropped_flag = dropped.clone();
    let writer_slot: Rc<RefCell<Option<SurfaceWriter>>> = Rc::default();
    let writer_slot_capture = writer_slot.clone();

    let (reference, _surface) = components
        .publish_surface(&micro, move |_mount, writer| {
            writer.apply(&counter_initial_batch())?;
            *writer_slot_capture.borrow_mut() = Some(writer.clone());
            let clicks = Cell::new(0_u64);
            let flag = dropped_flag.clone();
            Ok(SurfaceSession::with_drop(
                move |event| {
                    assert_eq!(event.name, "click");
                    assert_eq!(event.element, 1);
                    let next = clicks.get() + 1;
                    clicks.set(next);
                    // Re-entrant apply inside event dispatch must not conflict with SurfaceTree borrow.
                    writer
                        .apply(&counter_text_update_batch(&next.to_string()))
                        .unwrap();
                    EventResponse::default()
                },
                move || flag.set(true),
            ))
        })
        .unwrap();

    #[derive(Clone, PartialEq)]
    struct HostProps {
        components: NativeComponents,
        viewer: Instance,
        reference: ComponentRef,
    }

    fn host_app(props: HostProps) -> Element {
        rsx! {
            section {
                ComponentMount {
                    components: props.components,
                    owner: props.viewer,
                    reference: props.reference,
                }
            }
        }
    }

    let mut host_dom = VirtualDom::new_with_props(
        host_app,
        HostProps {
            components,
            viewer,
            reference,
        },
    );
    let mut host_tree = SurfaceTree::new();
    host_dom.rebuild(&mut host_tree);
    assert_eq!(host_tree.text_content(), "0");

    // Find the host VirtualDom's ElementId for the mounted `<button>` (carrying the click listener,
    // not the detached template prototype).
    let host_button_id = host_tree
        .listener_id("button", "click")
        .expect("host tree rendered the surface button");

    let click = dioxus_core::Event::new(
        EventData::Mouse(SerializedMouseData::default()).into_any(),
        true,
    );
    host_dom.runtime().handle_event_for_target(
        RenderTargetId::ROOT,
        "click",
        click,
        host_button_id,
    );
    host_dom.render_immediate(&mut host_tree);
    assert_eq!(host_tree.text_content(), "1");

    // Closing the micro's Instance revokes the surface writer immediately.
    host.close_instance(&micro).unwrap();
    let writer = writer_slot.borrow().clone().unwrap();
    assert_eq!(
        writer.apply(&counter_text_update_batch("2")),
        Err(PresentationError::Revoked)
    );
    drop(host_dom);
    assert!(dropped.get());
}

/// What a host renderer's `onmounted` hands out, in these tests: it logs every call that reaches
/// it, so the order in which element commands arrive is observable. Scroll offsets and
/// `set_focus` are supported; the client rect is left at Dioxus's default `NotSupported`, which is
/// what a renderer that cannot measure answers.
#[derive(Clone)]
struct RecordingElement(Rc<RefCell<Vec<String>>>);

impl dioxus_html::RenderedElementBacking for RecordingElement {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get_scroll_offset(
        &self,
    ) -> std::pin::Pin<
        Box<dyn Future<Output = dioxus_html::MountedResult<dioxus_html::geometry::PixelsVector2D>>>,
    > {
        self.0.borrow_mut().push("get_scroll_offset".into());
        Box::pin(async { Ok(dioxus_html::geometry::PixelsVector2D::new(1.0, 2.0)) })
    }

    fn scroll(
        &self,
        coordinates: dioxus_html::geometry::PixelsVector2D,
        _behavior: dioxus_html::ScrollBehavior,
    ) -> std::pin::Pin<Box<dyn Future<Output = dioxus_html::MountedResult<()>>>> {
        self.0
            .borrow_mut()
            .push(format!("scroll({}, {})", coordinates.x, coordinates.y));
        Box::pin(async { Ok(()) })
    }

    fn set_focus(
        &self,
        focus: bool,
    ) -> std::pin::Pin<Box<dyn Future<Output = dioxus_html::MountedResult<()>>>> {
        self.0.borrow_mut().push(format!("set_focus({focus})"));
        Box::pin(async { Ok(()) })
    }
}

/// `SerializedHtmlEventConverter` for every event but `mounted`, which unpacks a
/// [`RecordingElement`] the way a real renderer unpacks its platform handle.
struct RecordingConverter;

macro_rules! delegate_to_serialized {
    (
        enum Event {
            $(
                #[convert = $converter:ident]
                #[events = [$($events:tt)*]]
                $(#[raw = [$($raw:ident),* $(,)?]])?
                $group:ident($data:ident),
            )*
        }
    ) => {
        impl dioxus_html::HtmlEventConverter for RecordingConverter {
            $( delegate_to_serialized!(@one $converter, $data); )*
        }
    };
    (@one convert_mounted_data, MountedData) => {
        fn convert_mounted_data(
            &self,
            event: &dioxus_html::PlatformEventData,
        ) -> dioxus_html::MountedData {
            dioxus_html::MountedData::new(event.downcast::<RecordingElement>().unwrap().clone())
        }
    };
    (@one $converter:ident, $data:ident) => {
        fn $converter(&self, event: &dioxus_html::PlatformEventData) -> dioxus_html::$data {
            dioxus_html::SerializedHtmlEventConverter.$converter(event)
        }
    };
}

dioxus_html::with_html_event_groups!(delegate_to_serialized);

#[tokio::test]
async fn element_commands_run_in_issue_order_after_the_dispatch_and_reads_get_replies() {
    use clay_surface::ElementRead;
    use clay_surface::ScrollBehavior;
    use clay_surface::ScrollOffsets;
    use clay_surface::SurfaceSink;
    dioxus_html::set_event_converter(Box::new(RecordingConverter));
    let (_host, owner, mount, _writer) = mounted_writer();
    let host_calls = Rc::new(RefCell::new(Vec::<String>::new()));
    let replies = Rc::new(RefCell::new(Vec::<serde_json::Value>::new()));
    let slot = Rc::new(RefCell::new(None));
    let factory = SurfaceFactory(Rc::new({
        let (slot, replies, host_calls) = (slot.clone(), replies.clone(), host_calls.clone());
        move |_mount, writer| {
            writer.apply(&[
                Mutation::PushId(0),
                Mutation::CreateElement(("div".into(), None)),
                Mutation::SetId(1),
                Mutation::AddEventListener("mounted".into()),
                Mutation::AppendChildren(1),
                Mutation::Pop,
            ])?;
            *slot.borrow_mut() = Some(writer.clone());
            let (replies, host_calls) = (replies.clone(), host_calls.clone());
            Ok(SurfaceSession::new(move |event| {
                if event.name == ELEMENT_REPLY {
                    replies
                        .borrow_mut()
                        .push(serde_json::from_str(&event.data_json).unwrap());
                    return EventResponse::default();
                }
                assert_eq!(event.name, "mounted");
                // What a guest's `onmounted` handler does with its `MountedData`: scroll, read,
                // focus, and read what the renderer cannot measure, all within one dispatch.
                let offsets = ScrollOffsets {
                    left: 0.0,
                    top: 40.0,
                    behavior: ScrollBehavior::Instant,
                };
                for command in [
                    ElementCommand::Scroll(offsets),
                    ElementCommand::Read {
                        request: 7,
                        what: ElementRead::ScrollOffset,
                    },
                    ElementCommand::SetFocus(true),
                    ElementCommand::Read {
                        request: 8,
                        what: ElementRead::ClientRect,
                    },
                ] {
                    writer.element_command(event.element, command).unwrap();
                }
                // The host runs nothing until the dispatch has returned.
                assert!(host_calls.borrow().is_empty());
                EventResponse::default()
            }))
        }
    }));
    #[derive(Clone, PartialEq)]
    struct Props {
        mount: SharedMount,
        owner: Instance,
        factory: SurfaceFactory,
    }
    fn app(props: Props) -> Element {
        rsx! { MountedSurface { mount: props.mount, owner: props.owner, factory: props.factory } }
    }
    let mut dom = VirtualDom::new_with_props(
        app,
        Props {
            mount: SharedMount(mount),
            owner,
            factory,
        },
    );
    let mut host_tree = SurfaceTree::new();
    dom.rebuild(&mut host_tree);
    // A command for an element that has not announced `mounted` is refused at the boundary.
    let writer = slot.borrow().clone().unwrap();
    assert!(matches!(
        writer.element_command(1, ElementCommand::SetFocus(true)),
        Err(PresentationError::InvalidBatch(_))
    ));
    let element = host_tree.listener_id("div", "mounted").unwrap();
    dom.runtime().handle_event_for_target(
        RenderTargetId::ROOT,
        "mounted",
        dioxus_core::Event::new(
            Rc::new(dioxus_html::PlatformEventData::new(Box::new(
                RecordingElement(host_calls.clone()),
            ))) as Rc<dyn std::any::Any>,
            false,
        ),
        element,
    );
    while replies.borrow().len() < 2 {
        dom.wait_for_work().await;
        dom.render_immediate(&mut host_tree);
    }
    assert_eq!(
        &*host_calls.borrow(),
        &["scroll(0, 40)", "get_scroll_offset", "set_focus(true)"]
    );
    let replies = replies.borrow();
    assert_eq!(
        replies[0],
        serde_json::json!({"request": 7, "value": {"x": 1.0, "y": 2.0}})
    );
    // The renderer cannot measure, so the guest gets an error reply it surfaces as a
    // `MountedError`; the micro is not faulted and the surface stays live.
    assert_eq!(replies[1]["request"], 8);
    assert!(replies[1]["error"].is_string());
    assert!(writer.is_live());
}
