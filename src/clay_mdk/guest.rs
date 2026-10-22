//! The guest backend: the same authoring shape as [`crate::native`], implemented over the WIT
//! imports a wasm micro is linked against. A micro writes one `activate(&Context)` and one
//! Dioxus view; [`export_micro!`] turns that into the `micro-client` world the host drives.
//! Activation receives already-granted configuration bindings and publishes one component —
//! the surface the host mounts — exactly as a native activation receives a [`crate::native::Context`].
//! What the guest cannot yet do (require provisions, write collections, mount roots) is absent
//! here rather than stubbed; it arrives with the collections WIT waist.
//!
//! Document flow: the host's `document-events` callbacks advance the [`DocumentMirror`]; the
//! mirror's one notifier bumps a Dioxus signal inside the guest runtime, so any component that
//! called [`use_document_changes`] (directly or via [`use_facet`]) re-renders and reads the
//! current snapshot. Reads never cross the boundary; only events and explicit resyncs do.
pub use crate::mirror::ConfigDocument;
pub use crate::mirror::Facet;
pub use crate::mirror::FacetState;
pub use crate::mirror::MirrorOperation;
pub use crate::mirror::MirrorSnapshot;
pub use crate::mirror::MirrorStatus;
pub use crate::mirror::MirrorWatch;
pub use dom::GuestDom;

use crate::documents;
use crate::mirror::DocumentMirror;
use crate::presentation;
use crate::presentation_events;
use dioxus::prelude::*;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

mod dom;

/// The value surface one guest activation may use: its granted configuration documents and the
/// publication of its presentation. Constructed only by the runtime behind [`export_micro!`].
pub struct Context {
    configs: BTreeMap<String, ConfigDocument>,
    presentation: Rc<RefCell<Option<GuestDom>>>,
    changes: Rc<Cell<Option<Signal<u64>>>>,
}

impl Context {
    /// A configuration document granted from the manifest's declared ACL. `None` means the host
    /// handed over no binding under that name — the manifest declared none, or the composition
    /// activated the micro standalone.
    pub fn config(&self, name: &str) -> Option<&ConfigDocument> {
        self.configs.get(name)
    }

    /// Publish this micro's presentation. The host mounts it into every placement it grants the
    /// incarnation; a guest has exactly one, so publishing twice is a programming error. Unlike
    /// the native twin this returns no [`crate::ComponentRef`]: a guest cannot yet place its own
    /// component anywhere (that arrives with the collections waist), so there is nothing to hold.
    ///
    /// `render` runs as the presentation's root scope and its output is rendered once per
    /// mounted surface, so hooks called directly in `render` hold state every surface shares
    /// — a child component's hooks hold per-surface state instead. Keep the micro's model in
    /// `render`; keep child components presentational.
    pub fn publish_component(&self, render: impl Fn() -> Element + 'static) {
        let dom = GuestDom::new_with_props(
            root,
            RootProps {
                changes: self.changes.clone(),
                render: Rc::new(render),
            },
        );
        assert!(
            self.presentation.borrow_mut().replace(dom).is_none(),
            "guest micro published more than one component"
        );
    }
}

/// Re-render the calling component on every document-mirror change — delivered snapshots, lag,
/// completions, resyncs. Reads of [`Facet`] or [`MirrorOperation`] taken after this hook in the
/// same render are current.
pub fn use_document_changes() {
    let changes = use_context::<DocumentChanges>();
    let _ = (changes.0)();
}

/// [`use_document_changes`] plus the facet's current snapshot.
pub fn use_facet<T>(facet: &Facet<T>) -> MirrorSnapshot {
    use_document_changes();
    facet.current()
}

/// Export the `micro-client` world for a guest authored as `activate(&Context)`. Invoke once at
/// the crate root of a `cdylib` micro; the generated runtime owns the document mirror and the
/// published presentation and routes every host callback to them.
#[macro_export]
macro_rules! export_micro {
    ($activate:path) => {
        struct ClayMdkExportedMicro;

        impl $crate::micro_lifecycle::Guest for ClayMdkExportedMicro {
            fn activate(
                configs: ::std::vec::Vec<$crate::documents::DocumentBinding>,
            ) -> ::std::result::Result<(), ::std::string::String> {
                $crate::guest::Runtime::activate($activate, configs)
            }
        }

        impl $crate::document_events::Guest for ClayMdkExportedMicro {
            fn changed(snapshot: $crate::documents::DocumentSnapshot) {
                $crate::guest::Runtime::changed(snapshot);
            }

            fn lagged(watch_id: u64) {
                $crate::guest::Runtime::lagged(watch_id);
            }

            fn completed(receipt: $crate::documents::OperationReceipt) {
                $crate::guest::Runtime::completed(receipt);
            }
        }

        impl $crate::presentation_events::Guest for ClayMdkExportedMicro {
            fn mount(
                placement: &$crate::presentation::MountPoint,
                components: ::std::vec::Vec<$crate::presentation::ComponentRef>,
            ) -> ::std::result::Result<
                $crate::presentation::Surface,
                $crate::presentation::PresentationError,
            > {
                $crate::guest::Runtime::mount(placement, components)
            }

            fn unmount(surface: &$crate::presentation::Surface) {
                $crate::guest::Runtime::unmount(surface);
            }

            fn dispatch(
                surface: &$crate::presentation::Surface,
                event: $crate::presentation_events::Event,
            ) -> $crate::presentation_events::EventResponse {
                $crate::guest::Runtime::dispatch(surface, event)
            }
        }

        $crate::bindings::export!(ClayMdkExportedMicro with_types_in $crate::bindings);
    };
}

/// One incarnation's guest-side state: the mirror every granted binding routes through and the
/// presentation activation published. A wasm store hosts one incarnation, so this is a
/// thread-local; the host serializes every call into it. Public only for [`export_micro!`].
#[doc(hidden)]
pub struct Runtime {
    mirror: DocumentMirror,
    presentation: Rc<RefCell<Option<GuestDom>>>,
    changes: Rc<Cell<Option<Signal<u64>>>>,
}

thread_local! {
    static RUNTIME: RefCell<Option<Runtime>> = const { RefCell::new(None) };
}

impl Runtime {
    pub fn activate(
        activate: impl FnOnce(&Context) -> Result<(), documents::DocumentError>,
        configs: Vec<documents::DocumentBinding>,
    ) -> Result<(), String> {
        let mirror = DocumentMirror::default();
        let presentation = Rc::new(RefCell::new(None));
        let changes = Rc::new(Cell::new(None));
        let context = Context {
            configs: configs
                .into_iter()
                .map(|binding| (binding.name.clone(), mirror.bind(binding)))
                .collect(),
            presentation: presentation.clone(),
            changes: changes.clone(),
        };
        let runtime = Self {
            mirror,
            presentation,
            changes,
        };
        runtime.mirror.set_notifier({
            let changes = runtime.changes.clone();
            move || {
                let Some(mut signal) = changes.get() else {
                    return;
                };
                signal += 1;
            }
        });
        RUNTIME.with_borrow_mut(|slot| {
            assert!(
                slot.replace(runtime).is_none(),
                "guest micro activated twice"
            );
        });
        activate(&context).map_err(|error| error.to_string())
    }

    fn with<T>(f: impl FnOnce(&Runtime) -> T) -> T {
        RUNTIME.with_borrow(|slot| {
            f(slot
                .as_ref()
                .expect("host drove a guest micro before activating it"))
        })
    }

    /// Run a mirror mutation where its notifier may write Dioxus state: inside the presentation's
    /// runtime when one is published (then flush the resulting edits), plainly otherwise.
    fn in_presentation(&self, change: impl FnOnce()) {
        match self.presentation.borrow_mut().as_mut() {
            Some(dom) => dom
                .update(change)
                .expect("guest presentation commit failed"),
            None => change(),
        }
    }

    pub fn changed(snapshot: documents::DocumentSnapshot) {
        Self::with(|runtime| {
            runtime.in_presentation(|| {
                runtime.mirror.changed(snapshot);
            })
        });
    }

    /// A lagged watch is resynchronized on the spot: the mirror is stale and a guest has nothing
    /// better to do with it than pull the current snapshot. Every failure mode is already
    /// reflected in the watch's status (still lagged, or withdrawn), which is what views show.
    pub fn lagged(watch_id: u64) {
        Self::with(|runtime| {
            runtime.in_presentation(|| {
                let Some(watch) = runtime.mirror.lagged(watch_id) else {
                    return;
                };
                match watch.resync() {
                    Ok(_)
                    | Err(
                        documents::DocumentError::SubscriptionLagged
                        | documents::DocumentError::Revoked
                        | documents::DocumentError::Denied,
                    ) => {}
                    Err(documents::DocumentError::InvalidValue(message)) => {
                        panic!("document watch read rejected its own selection: {message}")
                    }
                }
            })
        });
    }

    pub fn completed(receipt: documents::OperationReceipt) {
        Self::with(|runtime| {
            runtime.in_presentation(|| {
                runtime.mirror.completed(receipt);
            })
        });
    }

    pub fn mount(
        placement: &presentation::MountPoint,
        components: Vec<presentation::ComponentRef>,
    ) -> Result<presentation::Surface, presentation::PresentationError> {
        assert!(
            components.is_empty(),
            "component slots are not routed to Dioxus guest micros yet"
        );
        Self::with(|runtime| {
            runtime
                .presentation
                .borrow_mut()
                .as_mut()
                .expect("host mounted a guest micro that published no component")
                .mount(placement)
        })
    }

    pub fn unmount(surface: &presentation::Surface) {
        Self::with(|runtime| {
            runtime
                .presentation
                .borrow_mut()
                .as_mut()
                .expect("host unmounted a guest micro that published no component")
                .unmount(surface)
        });
    }

    pub fn dispatch(
        surface: &presentation::Surface,
        event: presentation_events::Event,
    ) -> presentation_events::EventResponse {
        Self::with(|runtime| {
            runtime
                .presentation
                .borrow_mut()
                .as_mut()
                .expect("host dispatched to a guest micro that published no component")
                .dispatch(surface, event)
        })
    }
}

/// The document-change tick [`use_document_changes`] subscribes to, provided at the published
/// component's root.
#[derive(Clone, Copy)]
struct DocumentChanges(Signal<u64>);

#[derive(Clone)]
struct RootProps {
    changes: Rc<Cell<Option<Signal<u64>>>>,
    render: Rc<dyn Fn() -> Element>,
}

fn root(props: RootProps) -> Element {
    let changes = use_hook(|| {
        let signal = Signal::new(0_u64);
        props.changes.set(Some(signal));
        signal
    });
    use_context_provider(|| DocumentChanges(changes));
    (props.render)()
}
