//! The guest backend: the same authoring shape as [`crate::native`], implemented over the WIT
//! imports a wasm micro is linked against. A micro writes one `activate(&Context)` and one
//! Dioxus view; [`export_micro!`] turns that into the `micro-client` world the host drives,
//! running `activate` inside `micro-lifecycle.initialize` (ADR 024 §5).
//! Initialization receives already-granted configuration bindings and publishes one component —
//! the surface the host mounts — exactly as a native activation receives a [`crate::native::Context`].
//! What the guest cannot yet do (require publications, write collections, mount roots, provide
//! contracts) is absent here rather than stubbed; such bindings fail initialization as an
//! invalid configuration.
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

use crate::capability_bank;
use crate::documents;
use crate::lifecycle_host_requests;
use crate::micro_lifecycle;
use crate::mirror::DocumentMirror;
use crate::presentation;
use crate::presentation_events;
use crate::serve;
use dioxus::prelude::*;
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use wit_bindgen::rt::async_support::StreamReader;

mod dom;

type Handler = Arc<dyn Fn(Vec<u8>) -> LocalBoxFuture<'static, serve::Outcome> + Send + Sync>;

/// The call handlers `activate` registered, by publication name. A `static`, not a
/// thread-local: `serve` runs in its own export task, which does not share thread-local state
/// with `initialize` or the sync presentation exports.
static HANDLERS: Mutex<BTreeMap<String, Handler>> = Mutex::new(BTreeMap::new());

/// The value surface one guest activation may use: its granted configuration documents and the
/// publication of its presentation. Constructed only by the runtime behind [`export_micro!`].
pub struct Context {
    configs: BTreeMap<String, ConfigDocument>,
    published: RefCell<Vec<micro_lifecycle::NamedPublication>>,
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

    /// Publish a call named `name` under `contract` and serve it with `handler`. The MDK's
    /// `serve` loop runs one `handler` future per request, concurrently with every other
    /// request, and drops it if the caller gives up. `name` must be a call the manifest
    /// declares.
    ///
    /// Handlers run in the guest's `serve` task, which does not share thread-local state with
    /// the sync presentation exports: state a handler shares with the presentation must live in
    /// a `static`.
    pub fn publish_call<F>(
        &self,
        name: &str,
        contract: &capability_bank::Contract,
        handler: impl Fn(Vec<u8>) -> F + Send + Sync + 'static,
    ) -> Result<(), capability_bank::CapabilityError>
    where
        F: Future<Output = serve::Outcome> + 'static,
    {
        let capability = capability_bank::publish_call(name, contract)?;
        self.published
            .borrow_mut()
            .push(micro_lifecycle::NamedPublication {
                name: name.to_owned(),
                capability,
            });
        let handler: Handler = Arc::new(move |payload| handler(payload).boxed_local());
        assert!(
            HANDLERS
                .lock()
                .unwrap()
                .insert(name.to_owned(), handler)
                .is_none(),
            "guest micro published call {name:?} twice"
        );
        Ok(())
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
            async fn initialize(
                context: $crate::micro_lifecycle::InitialContext,
            ) -> ::std::result::Result<
                $crate::micro_lifecycle::Initialized,
                $crate::micro_lifecycle::InitializationError,
            > {
                $crate::guest::Runtime::initialize($activate, context)
            }

            async fn serve(inbox: $crate::serve::Inbox) {
                $crate::guest::Runtime::serve(inbox).await
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
                publication: ::std::string::String,
                placement: &$crate::presentation::MountPoint,
                components: ::std::vec::Vec<$crate::presentation::ComponentRef>,
            ) -> ::std::result::Result<
                $crate::presentation::Surface,
                $crate::presentation::PresentationError,
            > {
                $crate::guest::Runtime::mount(&publication, placement, components)
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
    /// The MDK's `initialize`: bind the granted configuration documents, run `activate`, and
    /// answer with the calls `activate` published. Bindings the authoring surface
    /// cannot express yet (collections, registrations, requirements) are a configuration the
    /// micro cannot run under, not a guest fault. `arguments` are unused: no MDK micro takes
    /// any yet.
    pub fn initialize(
        activate: impl FnOnce(&Context) -> Result<(), documents::DocumentError>,
        context: micro_lifecycle::InitialContext,
    ) -> Result<micro_lifecycle::Initialized, micro_lifecycle::InitializationError> {
        let micro_lifecycle::InitialContext {
            arguments: _,
            configs,
            collections,
            registrations,
            requirements,
        } = context;
        if !(collections.is_empty() && registrations.is_empty() && requirements.is_empty()) {
            return Err(micro_lifecycle::InitializationError::InvalidConfiguration(
                "MDK guest micros accept only configuration document bindings".into(),
            ));
        }
        let mirror = DocumentMirror::default();
        let presentation = Rc::new(RefCell::new(None));
        let changes = Rc::new(Cell::new(None));
        let context = Context {
            configs: configs
                .into_iter()
                .map(|binding| (binding.name.clone(), mirror.bind(binding)))
                .collect(),
            published: RefCell::default(),
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
                "guest micro initialized twice"
            );
        });
        activate(&context)
            .map_err(|error| micro_lifecycle::InitializationError::Rejected(error.to_string()))?;
        Ok(micro_lifecycle::Initialized {
            publications: context.published.take(),
        })
    }

    fn with<T>(f: impl FnOnce(&Runtime) -> T) -> T {
        RUNTIME.with_borrow(|slot| {
            f(slot
                .as_ref()
                .expect("host drove a guest micro before initializing it"))
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

    /// The incarnation's main loop: one handler per request, looked up by publication name in
    /// the handlers `activate` registered through [`Context::publish_call`].
    pub async fn serve(inbox: StreamReader<lifecycle_host_requests::Request>) {
        serve::run(inbox, |publication, payload| {
            let handler = HANDLERS.lock().unwrap().get(publication).cloned();
            match handler {
                Some(handler) => handler(payload),
                None => {
                    let message = format!("this guest micro serves no call {publication:?}");
                    async move { Err(message) }.boxed_local()
                }
            }
        })
        .await
    }

    /// A guest micro has exactly one presentation until the authoring API grows named
    /// publications, so `publication` is not consulted.
    pub fn mount(
        _publication: &str,
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
