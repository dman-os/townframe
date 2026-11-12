//! The guest backend: the same [`crate::Context`] as the in-process one, implemented over the WIT
//! imports a wasm micro is linked against. A micro writes one `async fn activate(Context)` and
//! Dioxus views; [`crate::export_micro!`] turns that into the `micro-client` world the host
//! drives, running `activate` inside `micro-lifecycle.initialize` (ADR 030 §5). Initialization
//! receives every already-granted binding (configuration, collections, registrations,
//! requirements); the manifest the macro is given says which bindings the micro declared, so an
//! undeclared name is a programming error exactly as in-process and an unresolved optional one is
//! `None`.
//!
//! Document flow: the host's `document-events` callbacks advance the
//! [`crate::mirror::DocumentMirror`]; collection feeds fold pages into their own mirrors. Every
//! such change marks the runtime dirty and is then pushed into each presentation's tick signal
//! inside its Dioxus runtime, so a component that called a hook in [`crate::hooks`] re-renders
//! and reads the current state. Reads never cross the boundary; only events and explicit
//! resyncs do.
pub use dom::GuestDom;
pub(crate) use dom::Slots;
#[cfg(test)]
pub(crate) use dom::settle;

use crate::Access;
use crate::Capability;
use crate::Config;
use crate::Context;
use crate::Error;
use crate::DatumSnapshot;
use crate::MicroManifest;
use crate::OperationError;
use crate::OperationReceipt;
use crate::OperationStatus;
use crate::PublicationKind;
use crate::RefStatus;
use crate::RowAttachment;
use crate::capability_bank;
use crate::collection_registration;
use crate::collections;
use crate::context::Backend;
use crate::context::RawHandler;
use crate::context::Shared;
use crate::documents;
use crate::micro_lifecycle;
use crate::mirror::DocumentMirror;
use crate::mirror::Datum;
use crate::mirror::MirrorStatus;
use crate::presentation;
use crate::presentation_events;
use crate::rows::Feed;
use crate::rows::Stored;
use crate::rows::stored_page;
use clay_collection::Change;
use clay_collection::Cursor;
use clay_collection::Page;
use clay_collection::RowEvidence;
use clay_collection::Stamp;
use clay_collection::Stamped;
use clay_collection::Transaction;
use clay_collection::Version;
use clay_collection::WriteBasis;
use dioxus::prelude::*;
use serde::de::DeserializeOwned;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;
use std::task::Waker;

mod dom;

/// One row value as a guest's page carries it: its body and the references the host minted for
/// its attachments, already recipient-local to this incarnation.
pub(crate) struct GuestValue {
    pub(crate) body: Vec<u8>,
    pub(crate) capabilities: Vec<Rc<capability_bank::CapabilityRef>>,
}

/// The change tick [`crate::hooks`] subscribes to, provided at each published component's root.
#[derive(Clone, Copy)]
pub(crate) struct ChangeTick(pub(crate) Signal<u64>);

/// One published presentation: its Dioxus runtime and the tick signal its root created.
struct Presentation {
    dom: GuestDom,
    tick: Rc<Cell<Option<Signal<u64>>>>,
}

#[derive(Clone)]
struct RootProps {
    tick: Rc<Cell<Option<Signal<u64>>>>,
    render: Rc<dyn Fn() -> Element>,
}

fn root(props: RootProps) -> Element {
    let tick = use_hook(|| {
        let signal = Signal::new(0_u64);
        props.tick.set(Some(signal));
        signal
    });
    use_context_provider(|| ChangeTick(tick));
    (props.render)()
}

/// Local tasks an incarnation owns. A task is dropped at its next poll once the incarnation is
/// closing, which cancels whatever host call it was suspended in.
#[derive(Default)]
struct Tasks {
    closing: Cell<bool>,
    live: Cell<usize>,
    /// Wakers of the tasks to rouse at close, and of whoever waits for them to be gone.
    wakers: RefCell<Vec<Waker>>,
    idle: RefCell<Option<Waker>>,
    /// Wakes the component-model task whose executor adopts newly spawned futures: see
    /// [`Tasks::start_doorbell`].
    doorbell: RefCell<Option<Waker>>,
}

/// Stops one task before the incarnation closes: the view that spawned it was dropped.
#[derive(Default)]
pub(crate) struct Cancel {
    cancelled: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

impl Cancel {
    /// Drop the task's future at its next poll, which is now: the task is woken.
    pub(crate) fn cancel(&self) {
        self.cancelled.set(true);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

/// An incarnation-owned task: polls `future` until it ends, is cancelled, or the incarnation
/// closes.
struct Owned {
    future: Option<Pin<Box<dyn Future<Output = ()>>>>,
    tasks: Rc<Tasks>,
    cancel: Option<Rc<Cancel>>,
}

impl Future for Owned {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
        let tasks = Rc::clone(&self.tasks);
        let cancelled = self.cancel.as_ref().is_some_and(|cancel| {
            *cancel.waker.borrow_mut() = Some(cx.waker().clone());
            cancel.cancelled.get()
        });
        let future = self
            .future
            .as_mut()
            .expect("an ended task is not polled again");
        let finished = if tasks.closing.get() || cancelled {
            true
        } else {
            let mut wakers = tasks.wakers.borrow_mut();
            if !wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
                wakers.push(cx.waker().clone());
            }
            drop(wakers);
            future.as_mut().poll(cx).is_ready()
        };
        if !finished {
            return Poll::Pending;
        }
        self.future = None;
        tasks.live.set(tasks.live.get() - 1);
        if tasks.live.get() == 0
            && let Some(waker) = tasks.idle.borrow_mut().take()
        {
            waker.wake();
        }
        Poll::Ready(())
    }
}

impl Tasks {
    fn spawn(
        self: &Rc<Self>,
        future: Pin<Box<dyn Future<Output = ()>>>,
        cancel: Option<Rc<Cancel>>,
    ) {
        self.live.set(self.live.get() + 1);
        wit_bindgen::spawn_local(Owned {
            future: Some(future),
            tasks: Rc::clone(self),
            cancel,
        });
        if let Some(doorbell) = &*self.doorbell.borrow() {
            doorbell.wake_by_ref();
        }
    }

    /// `wit_bindgen::spawn_local` only queues the future on a global list, and the executor of
    /// whichever component-model task is polled next adopts it. Spawning from `initialize` or a
    /// call handler is polled right away, but a spawn from a synchronous entry (`mount`,
    /// `dispatch`, a document event: a view creating a slot while it renders) queues it with
    /// nothing to poll it, so it would wait for an unrelated wake (a feed's long-poll expiring,
    /// half a minute later). The doorbell is a task parked in `initialize`'s scope, which
    /// outlives those entries, whose waker each spawn rings: the executor is polled, adopts the
    /// queue and polls the new task. Started once, from `initialize`; ends with the other tasks
    /// when the incarnation closes.
    fn start_doorbell(self: &Rc<Self>) {
        let tasks = Rc::clone(self);
        self.spawn(
            Box::pin(std::future::poll_fn(move |cx| {
                *tasks.doorbell.borrow_mut() = Some(cx.waker().clone());
                Poll::<()>::Pending
            })),
            None,
        );
    }

    /// Stop every task and resolve once each has dropped its future (and so its host
    /// resources).
    async fn close(&self) {
        self.closing.set(true);
        for waker in std::mem::take(&mut *self.wakers.borrow_mut()) {
            waker.wake();
        }
        std::future::poll_fn(|cx| {
            if self.live.get() == 0 {
                return Poll::Ready(());
            }
            *self.idle.borrow_mut() = Some(cx.waker().clone());
            Poll::Pending
        })
        .await;
    }
}

/// What the manifest declares that guest code needs to check its own calls against.
struct Declared {
    collections: BTreeMap<String, Access>,
    calls: BTreeMap<String, clay_collection::Nsid>,
}

/// One incarnation's guest-side state: the mirror every granted binding routes through, the
/// published presentations and call handlers, and the owned tasks. A wasm store hosts one
/// incarnation, so this is a thread-local; the host serializes every call into it. Public only
/// for [`crate::export_micro!`].
#[doc(hidden)]
pub struct Runtime {
    mirror: DocumentMirror,
    declared: Declared,
    readers: RefCell<BTreeMap<String, Rc<collections::CollectionReader>>>,
    writers: RefCell<BTreeMap<String, Rc<collection_registration::RegistrationWriter>>>,
    presentations: RefCell<BTreeMap<String, Presentation>>,
    /// The publication each mounted surface belongs to, by surface identity.
    surfaces: RefCell<BTreeMap<u64, String>>,
    calls: RefCell<BTreeMap<String, RawHandler>>,
    /// The bank's resource of each publication staged, handed to the host when `initialize`
    /// returns.
    staged: RefCell<Vec<(String, capability_bank::PublishedCapability)>>,
    /// Set by the mirror's notifier and the feeds; [`Self::mutate`] pushes it into the views.
    dirty: Cell<bool>,
    tasks: Rc<Tasks>,
}

thread_local! {
    static RUNTIME: RefCell<Option<Rc<Runtime>>> = const { RefCell::new(None) };
}

/// The guest's `instance-root`: closing it ends the incarnation's tasks and drops everything it
/// holds from the host.
#[doc(hidden)]
pub struct GuestRoot;

impl micro_lifecycle::GuestInstanceRoot for GuestRoot {
    async fn close(&self) -> Result<(), String> {
        Runtime::close().await;
        Ok(())
    }
}

impl Runtime {
    /// The running incarnation's runtime (a wasm store hosts exactly one), for the view
    /// components that spawn tasks on it.
    pub(crate) fn current() -> Rc<Self> {
        RUNTIME.with_borrow(|slot| {
            Rc::clone(
                slot.as_ref()
                    .expect("host drove a guest micro before initializing it"),
            )
        })
    }

    /// The MDK's `initialize`: bind every granted binding, run `activate`, and answer with a
    /// [`GuestRoot`] and the publications `activate` staged. `arguments` are unused: no MDK
    /// micro takes any.
    pub async fn initialize<Activate, Fut>(
        activate: Activate,
        manifest: fn() -> MicroManifest,
        context: micro_lifecycle::InitialContext,
    ) -> Result<micro_lifecycle::Initialized, micro_lifecycle::InitializationError>
    where
        Activate: FnOnce(Context) -> Fut,
        Fut: Future<Output = Result<(), Error>>,
    {
        let micro_lifecycle::InitialContext {
            arguments: _,
            configs,
            collections,
            registrations,
            requirements,
        } = context;
        let manifest = manifest();
        let mirror = DocumentMirror::default();
        let runtime = Rc::new(Self {
            declared: Declared {
                collections: manifest
                    .collection_requests
                    .iter()
                    .map(|request| {
                        (
                            request.binding.clone(),
                            request
                                .access
                                .clone()
                                .expect("a collection request declares its access"),
                        )
                    })
                    .collect(),
                calls: manifest
                    .publications
                    .iter()
                    .filter_map(|publication| {
                        let contract = publication.contract.as_ref()?;
                        (publication.kind == PublicationKind::Call)
                            .then(|| (publication.name.clone(), contract.clone()))
                    })
                    .collect(),
            },
            readers: RefCell::new(
                collections
                    .into_iter()
                    .map(|binding| (binding.name, Rc::new(binding.reader)))
                    .collect(),
            ),
            writers: RefCell::new(
                registrations
                    .into_iter()
                    .map(|binding| (binding.name, Rc::new(binding.writer)))
                    .collect(),
            ),
            mirror,
            presentations: RefCell::default(),
            surfaces: RefCell::default(),
            calls: RefCell::default(),
            staged: RefCell::default(),
            dirty: Cell::new(false),
            tasks: Rc::default(),
        });
        runtime.tasks.start_doorbell();
        runtime.mirror.set_notifier({
            let runtime = Rc::downgrade(&runtime);
            move || {
                if let Some(runtime) = runtime.upgrade() {
                    runtime.dirty.set(true);
                }
            }
        });
        let mut requirements: BTreeMap<_, _> = requirements
            .into_iter()
            .map(|binding| (binding.name, Capability::guest(Rc::new(binding.capability))))
            .collect();
        let shared = Context(Rc::new(Shared {
            configs: configs
                .into_iter()
                .map(|binding| {
                    (
                        binding.name.clone(),
                        Config::guest(runtime.mirror.bind(binding)),
                    )
                })
                .collect(),
            requirements: manifest
                .requires
                .iter()
                .map(|required| {
                    (
                        required.binding.clone(),
                        requirements.remove(&required.binding),
                    )
                })
                .collect(),
            requests: manifest.collection_requests.clone(),
            backend: Backend::Guest(Rc::clone(&runtime)),
        }));
        RUNTIME.with_borrow_mut(|slot| {
            assert!(
                slot.replace(Rc::clone(&runtime)).is_none(),
                "guest micro initialized twice"
            );
        });
        activate(shared)
            .await
            .map_err(|error| micro_lifecycle::InitializationError::Rejected(error.to_string()))?;
        let publications = runtime
            .staged
            .take()
            .into_iter()
            .map(|(name, capability)| micro_lifecycle::NamedPublication { name, capability })
            .collect();
        Ok(micro_lifecycle::Initialized {
            root: micro_lifecycle::InstanceRoot::new(GuestRoot),
            publications,
        })
    }

    /// End the incarnation (ADR 030 §6), with no close hook for author code: cleanup is the
    /// `Drop` of values the micro owns, and closing is dropping them in this order. The host
    /// calls this while the incarnation's dependencies are retained, so a drop may still use
    /// what it holds from them, and it revokes publications and the scope only after this
    /// returns. First the tasks stop and resolve once each has dropped its future, which drops
    /// whatever the task owned (a redeemed proxy, a binding, a lease). Then the views, call
    /// handlers and collection bindings are dropped, and with them what each captured. Clearing
    /// the maps also breaks the cycle through captured contexts (runtime -> view -> context ->
    /// runtime), which is what finally releases the requirement references the contexts hold.
    async fn close() {
        let runtime = Self::current();
        runtime.tasks.close().await;
        runtime.surfaces.take();
        runtime.presentations.take();
        runtime.calls.take();
        runtime.readers.take();
        runtime.writers.take();
        RUNTIME.with_borrow_mut(Option::take);
    }

    /// Run a mutation of a mirror, then push whatever it marked dirty into every presentation.
    fn mutate(&self, change: impl FnOnce()) {
        change();
        if !self.dirty.take() {
            return;
        }
        for presentation in self.presentations.borrow_mut().values_mut() {
            let Some(mut tick) = presentation.tick.get() else {
                continue;
            };
            presentation
                .dom
                .update(move || tick += 1)
                .expect("guest presentation commit failed");
        }
    }

    pub fn changed(snapshot: documents::DocumentSnapshot) {
        let runtime = Self::current();
        runtime.mutate(|| {
            runtime.mirror.changed(snapshot);
        });
    }

    /// A lagged watch is resynchronized on the spot: the mirror is stale and a guest has nothing
    /// better to do with it than pull the current snapshot. Every failure mode is already
    /// reflected in the watch's status (still lagged, or withdrawn), which is what views show.
    pub fn lagged(watch_id: u64) {
        let runtime = Self::current();
        runtime.mutate(|| {
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
        });
    }

    pub fn completed(receipt: documents::OperationReceipt) {
        let runtime = Self::current();
        runtime.mutate(|| {
            runtime.mirror.completed(receipt);
        });
    }

    /// Serve one call: `publication` names the handler `publish_call` registered.
    pub async fn call(publication: String, request: Vec<u8>) -> Result<Vec<u8>, String> {
        // Cloned out so no borrow is held while the handler awaits.
        let handler = Self::current()
            .calls
            .borrow()
            .get(&publication)
            .cloned()
            .ok_or_else(|| format!("this guest micro serves no call {publication:?}"))?;
        handler(request).await.map_err(|error| error.to_string())
    }

    pub fn mount(
        publication: &str,
        placement: &presentation::MountPoint,
    ) -> Result<presentation::Surface, presentation::PresentationError> {
        let runtime = Self::current();
        let surface = runtime
            .presentations
            .borrow_mut()
            .get_mut(publication)
            .unwrap_or_else(|| panic!("host mounted unpublished presentation {publication:?}"))
            .dom
            .mount(placement)?;
        assert!(
            runtime
                .surfaces
                .borrow_mut()
                .insert(surface.identity(), publication.to_owned())
                .is_none(),
            "surface identity reused"
        );
        Ok(surface)
    }

    pub fn unmount(surface: &presentation::Surface) {
        let runtime = Self::current();
        let Some(publication) = runtime.surfaces.borrow_mut().remove(&surface.identity()) else {
            return;
        };
        runtime
            .presentations
            .borrow_mut()
            .get_mut(&publication)
            .expect("a mounted surface's presentation outlives it")
            .dom
            .unmount(surface);
    }

    pub fn dispatch(
        surface: &presentation::Surface,
        event: presentation_events::Event,
    ) -> presentation_events::EventResponse {
        let runtime = Self::current();
        let publication = runtime
            .surfaces
            .borrow()
            .get(&surface.identity())
            .cloned()
            .expect("host dispatched to a surface that is not mounted");
        runtime
            .presentations
            .borrow_mut()
            .get_mut(&publication)
            .expect("a mounted surface's presentation outlives it")
            .dom
            .dispatch(surface, event)
    }

    /// The reader of the collection bound to `binding`, or `None` when configuration left an
    /// optional request unbound. Panics when the manifest declares no such request.
    pub(crate) fn collection_reader(
        &self,
        binding: &str,
    ) -> Option<Rc<collections::CollectionReader>> {
        assert!(
            self.declared.collections.contains_key(binding),
            "the manifest declares no collection request {binding:?}"
        );
        self.readers.borrow().get(binding).cloned()
    }

    /// The writer of the collection bound to `binding`, or `None` when it is unbound. Panics
    /// when the manifest's request for `binding` is read-only or absent.
    pub(crate) fn registration_writer(
        &self,
        binding: &str,
    ) -> Option<Rc<collection_registration::RegistrationWriter>> {
        match self.declared.collections.get(binding) {
            Some(Access::Write) => self.writers.borrow().get(binding).cloned(),
            Some(_) => panic!("collection request {binding:?} is read-only"),
            None => panic!("the manifest declares no collection request {binding:?}"),
        }
    }

    pub(crate) fn publish_component(
        &self,
        name: &str,
        render: Rc<dyn Fn() -> Element>,
    ) -> Result<(), Error> {
        let tick = Rc::default();
        let dom = GuestDom::new_with_props(
            root,
            RootProps {
                tick: Rc::clone(&tick),
                render,
            },
        );
        let published = capability_bank::publish_presentation(name)?;
        assert!(
            self.presentations
                .borrow_mut()
                .insert(name.to_owned(), Presentation { dom, tick })
                .is_none(),
            "presentation {name:?} published twice"
        );
        self.staged.borrow_mut().push((name.to_owned(), published));
        Ok(())
    }

    pub(crate) fn publish_call(&self, name: &str, handler: RawHandler) -> Result<(), Error> {
        let contract = self
            .declared
            .calls
            .get(name)
            .unwrap_or_else(|| panic!("the manifest declares no call publication {name:?}"));
        let published = capability_bank::publish_call(name, contract.as_str())?;
        assert!(
            self.calls
                .borrow_mut()
                .insert(name.to_owned(), handler)
                .is_none(),
            "call {name:?} published twice"
        );
        self.staged.borrow_mut().push((name.to_owned(), published));
        Ok(())
    }

    pub(crate) fn spawn(&self, future: Pin<Box<dyn Future<Output = ()>>>) {
        self.tasks.spawn(future, None);
    }

    /// Run `future` for a view: like [`Self::spawn`], but the returned handle stops it (and so
    /// releases whatever host resources it holds) when the view is dropped.
    pub(crate) fn spawn_for_view(&self, future: Pin<Box<dyn Future<Output = ()>>>) -> Rc<Cancel> {
        let cancel = Rc::default();
        self.tasks.spawn(future, Some(Rc::clone(&cancel)));
        cancel
    }

    /// A view's task changed what the view renders: push a tick into every presentation so its
    /// components re-render. Nothing polls Dioxus tasks in a guest, so a view task reports
    /// through the same dirty flag the mirrors do.
    pub(crate) fn notify_views(&self) {
        self.mutate(|| self.dirty.set(true));
    }
}

/// `datum`'s state in the mirror, or [`Error::Revoked`] once its watch was released or revoked.
pub(crate) fn snapshot<T: DeserializeOwned>(datum: &Datum<T>) -> Result<DatumSnapshot<T>, Error> {
    let snapshot = datum.current();
    match snapshot.status {
        MirrorStatus::Released | MirrorStatus::Revoked => Err(Error::Revoked),
        // A lagged watch is resynchronized as soon as the host says so (`Runtime::lagged`); its
        // retained state is what a view shows meanwhile.
        MirrorStatus::Live | MirrorStatus::Lagged => Ok(DatumSnapshot {
            state: datum.state(&snapshot),
            refreshing: snapshot.refreshing,
            revision: snapshot.revision,
            pending: snapshot.pending.len(),
        }),
    }
}

/// Write one row through the host's registration writer; each attachment names one of this
/// micro's own declared publications, which the host turns into its endpoint.
pub(crate) async fn put(
    writer: &collection_registration::RegistrationWriter,
    key: Vec<u8>,
    basis: WriteBasis,
    body: Vec<u8>,
    attachments: Vec<RowAttachment>,
) -> Result<Stamp, Error> {
    let attachments = attachments
        .into_iter()
        .map(|attachment| {
            collection_registration::RowAttachment::OwnPublication(attachment.into_name())
        })
        .collect();
    Ok(writer
        .put(
            key,
            basis.into(),
            collection_registration::WrittenValue { body, attachments },
        )
        .await?
        .into())
}

pub(crate) async fn delete(
    writer: &collection_registration::RegistrationWriter,
    key: Vec<u8>,
    basis: WriteBasis,
) -> Result<Stamp, Error> {
    Ok(writer.delete(key, basis.into()).await?.into())
}

/// Keep the mirror behind `feed` current from the host's projection of the collection `reader`
/// names: acquire a lease over the whole collection and long-poll it, folding each page in. The
/// host clamps the poll to its own maximum, so a caught-up read comes back empty at that cadence
/// and costs nothing in the views (an empty page changes nothing). Runs until the lease is
/// revoked or the incarnation closes and drops the task.
pub(crate) async fn feed(reader: Rc<collections::CollectionReader>, feed: Feed) {
    loop {
        let lease = match reader.acquire(Vec::new(), None).await {
            Ok(lease) => lease,
            Err(collections::AcquireError::Revoked) => return,
            Err(error) => panic!("a granted reader failed to acquire its collection: {error:?}"),
        };
        feed.mirror.borrow_mut().begin_lease(None);
        loop {
            let ticket = feed.mirror.borrow_mut().begin_read();
            let read = lease
                .read(
                    ticket.committed().map(|cursor| cursor.0.clone()),
                    collections::ReadLimits {
                        max_transactions: None,
                        max_changes: None,
                        poll_timeout_ms: u32::MAX,
                    },
                )
                .await;
            match read {
                Ok(page) => {
                    let page =
                        stored_page(decode_page(page), |value| Stored::Guest(Rc::new(value)));
                    let runtime = Runtime::current();
                    runtime.mutate(|| {
                        let mut mirror = feed.mirror.borrow_mut();
                        let bootstrapped = mirror.bootstrap_complete();
                        let populated = page
                            .transactions
                            .iter()
                            .any(|transaction| !transaction.changes.is_empty());
                        assert!(mirror.apply(&ticket, &page));
                        if populated || mirror.bootstrap_complete() != bootstrapped {
                            drop(mirror);
                            feed.applied();
                            runtime.dirty.set(true);
                        }
                    });
                }
                Err(collections::ReadError::ResetRequired) => {
                    let runtime = Runtime::current();
                    runtime.mutate(|| {
                        feed.mirror.borrow_mut().clear();
                        feed.applied();
                        runtime.dirty.set(true);
                    });
                    break;
                }
                Err(collections::ReadError::Revoked) => return,
                Err(collections::ReadError::SourceUnavailable) => {
                    unimplemented!("registration sources are always available")
                }
            }
        }
    }
}

fn decode_page(page: collections::Page) -> Page<Vec<u8>, Stamped<Option<GuestValue>>> {
    let value = |value: collections::EncodedValue| GuestValue {
        body: value.body,
        capabilities: value.capabilities.into_iter().map(Rc::new).collect(),
    };
    Page {
        transactions: page
            .transactions
            .into_iter()
            .map(|transaction| Transaction {
                changes: transaction
                    .changes
                    .into_iter()
                    .map(|change| match change {
                        collections::Change::Upsert(upsert) => Change::Upsert {
                            key: upsert.key.body,
                            row_version: version(upsert.row_version),
                            value: Stamped {
                                stamp: upsert.stamp.into(),
                                value: upsert.value.map(value),
                            },
                            evidence: match upsert.evidence {
                                collections::RowEvidence::Authoritative => {
                                    RowEvidence::Authoritative
                                }
                                collections::RowEvidence::Pending => RowEvidence::Pending,
                            },
                            write_context: upsert.write_context,
                        },
                        collections::Change::Remove(removal) => Change::Remove {
                            key: removal.key.body,
                            row_version: version(removal.row_version),
                        },
                    })
                    .collect(),
            })
            .collect(),
        through: Cursor(page.through),
        bootstrap_complete: page.bootstrap_complete,
    }
}

fn version(version: collections::Version) -> Version {
    Version::new(version.incarnation, version.sequence)
}

// The authoring vocabulary's view of the host's results.

impl From<documents::DocumentError> for Error {
    fn from(error: documents::DocumentError) -> Self {
        match error {
            documents::DocumentError::Denied => Self::Denied,
            documents::DocumentError::Revoked => Self::Revoked,
            documents::DocumentError::InvalidValue(message) => Self::InvalidValue(message),
            documents::DocumentError::SubscriptionLagged => Self::Lagged,
        }
    }
}

impl From<capability_bank::CapabilityError> for Error {
    fn from(error: capability_bank::CapabilityError) -> Self {
        match error {
            capability_bank::CapabilityError::ContractMismatch => Self::Denied,
            capability_bank::CapabilityError::Revoked => Self::Revoked,
            capability_bank::CapabilityError::LimitExceeded => Self::Limit,
            capability_bank::CapabilityError::Unavailable => Self::Unavailable,
        }
    }
}

impl From<capability_bank::CallError> for Error {
    fn from(error: capability_bank::CallError) -> Self {
        match error {
            capability_bank::CallError::Capability(error) => error.into(),
            capability_bank::CallError::Provider(message) => Self::Rejected(message),
        }
    }
}

impl From<collection_registration::MutationError> for Error {
    fn from(error: collection_registration::MutationError) -> Self {
        use collection_registration::MutationError;
        match error {
            MutationError::Conflict(current) => Self::Conflict(current.map(Into::into)),
            MutationError::Revoked => Self::Revoked,
            MutationError::RowLimit | MutationError::CapabilityLimit => Self::Limit,
            MutationError::KeyTooLarge | MutationError::ValueTooLarge => {
                Self::Rejected("row exceeds the collection's size limits".into())
            }
            MutationError::InvalidAttachment => {
                Self::Rejected("row names an attachment it does not carry".into())
            }
        }
    }
}

impl From<capability_bank::RefStatus> for RefStatus {
    fn from(status: capability_bank::RefStatus) -> Self {
        match status {
            capability_bank::RefStatus::Live => Self::Live,
            capability_bank::RefStatus::Dormant => Self::Dormant,
            capability_bank::RefStatus::Disabled => Self::Disabled,
            capability_bank::RefStatus::Faulted => Self::Faulted,
            capability_bank::RefStatus::Unavailable => Self::Unavailable,
            capability_bank::RefStatus::Revoked => Self::Revoked,
        }
    }
}

impl From<RefStatus> for capability_bank::RefStatus {
    fn from(status: RefStatus) -> Self {
        match status {
            RefStatus::Live => Self::Live,
            RefStatus::Dormant => Self::Dormant,
            RefStatus::Disabled => Self::Disabled,
            RefStatus::Faulted => Self::Faulted,
            RefStatus::Unavailable => Self::Unavailable,
            RefStatus::Revoked => Self::Revoked,
        }
    }
}

impl From<documents::OperationReceipt> for OperationReceipt {
    fn from(receipt: documents::OperationReceipt) -> Self {
        Self {
            id: receipt.id,
            revision: receipt.revision,
            status: match receipt.status {
                documents::OperationStatus::LocallyApplied => OperationStatus::LocallyApplied,
                documents::OperationStatus::Accepted => OperationStatus::Accepted,
                documents::OperationStatus::RolledBack(documents::OperationFailure::Rejected(
                    message,
                )) => OperationStatus::RolledBack(OperationError::Rejected { message }),
                documents::OperationStatus::RolledBack(documents::OperationFailure::Transport(
                    failure,
                )) => OperationStatus::RolledBack(OperationError::Transport {
                    message: failure.message,
                    outcome_unknown: failure.outcome_unknown,
                }),
            },
        }
    }
}
