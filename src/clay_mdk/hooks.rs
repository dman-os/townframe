//! The view-side half of the authoring surface: Dioxus hooks that bind the handles a micro's
//! `activate` captured to reactivity, and the slot components that mount another micro's
//! published component into this micro's tree: in-process *through the installed renderer*, in a
//! guest by emitting `mount-component` at the slot's position. Micro views reach both through
//! this module and so never name a renderer crate (ADR 015 §3 dependency direction).
use crate::Capability;
use crate::Collection;
use crate::Facet;
use crate::FacetSnapshot;
use crate::RefStatus;
use crate::Row;
use crate::context::Mount;
use crate::context::MountInner;
use dioxus::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::rc::Rc;

#[cfg(feature = "in_process")]
use clay_dioxus::ComponentMount;

#[derive(Props, Clone, PartialEq)]
pub struct PresentationSlotProps {
    pub reference: Capability,
    /// Chrome the slot's owner wants whenever there is no presentation to show (still loading, or
    /// unavailable): it is rendered above the placeholder. A mounted presentation brings its own
    /// chrome, so a position that must always offer a way out (the shell's sidebar/back button)
    /// has nothing to offer in the one state where the micro that would place it is absent.
    #[props(default)]
    pub placeholder_chrome: Option<Element>,
}

/// What a slot shows: the presentation it redeemed, or why there is none right now.
enum SlotState {
    Loading,
    /// The failure, and the reference's status when it failed, which says why in words.
    Unavailable(crate::Error, RefStatus),
    Mounted(Mount),
}

/// Follow `reference`'s provider, reporting what the slot should show to `show`, until the
/// slot is dropped. The redemption is asynchronous (a lazy reference activates its provider)
/// and the redeemed proxy is held for exactly as long as it is mounted, so dropping the slot
/// releases the provider (ADR 031 §9).
///
/// A redemption that fails shows its failure, and the next redemption waits for the
/// reference's status to move off the status the failure left it in (a disabled micro enabled,
/// an unconfigured one configured, a faulted one `retry`'d), so a provider that keeps failing is
/// never redeemed in a loop. A mounted presentation whose incarnation ends shows as loading and
/// is redeemed again, which demand-starts the provider's next incarnation. Both waits are woken
/// by the supervisor's status ticks; nothing polls. They are the same two host waits in-process
/// and in a guest (`status-changed`, `revoked`), so the slot is written once.
async fn follow_provider(reference: Capability, mut show: impl FnMut(SlotState)) {
    loop {
        // Read before redeeming: a status change after the failure then still wakes the wait
        // below, where reading it afterwards could miss a provider enabled in between.
        let before = reference.status();
        let failure = match reference.redeem_presentation().await {
            Ok(proxy) => match proxy.component() {
                Ok(component) => {
                    show(SlotState::Mounted(component));
                    proxy.revoked().await;
                    show(SlotState::Loading);
                    continue;
                }
                Err(failure) => failure,
            },
            Err(failure) => failure,
        };
        // Redeeming again before the status moves would only fail the same way.
        show(SlotState::Unavailable(failure, reference.status()));
        reference.status_changed(before).await;
    }
}

/// Mount the presentation a row attaches (or any other [`Capability`] to a presentation),
/// in-process and in a wasm guest alike. The backend decides only how the redeemed component
/// reaches the tree: the installed renderer mounts it in-process, a guest emits
/// `mount-component` at the slot's position.
#[expect(non_snake_case, reason = "Dioxus component naming")]
pub fn PresentationSlot(props: PresentationSlotProps) -> Element {
    let reference = props.reference;
    let chrome = props.placeholder_chrome;
    #[cfg(feature = "guest")]
    if reference.is_guest() {
        // Keyed by the reference: a row that keeps its key but attaches another target gets a
        // fresh task, and dropping the old one releases the previous provider.
        let key = reference.identity();
        return rsx! { GuestSlot { key: "{key}", reference, chrome } };
    }
    #[cfg(feature = "view")]
    return rsx! { InProcessSlot { reference, chrome } };
    #[cfg(not(feature = "view"))]
    unreachable!("without the `view` feature every reference belongs to a guest")
}

fn render_state(state: &SlotState, chrome: &Option<Element>) -> Element {
    match state {
        SlotState::Loading => {
            rsx! { {chrome.clone()} p { role: "status", "Loading section…" } }
        }
        SlotState::Unavailable(failure, status) => {
            let reason = match status.blocked_reason() {
                Some(reason) => reason.to_owned(),
                None => failure.to_string(),
            };
            rsx! { {chrome.clone()} p { role: "alert", "Section unavailable: {reason}." } }
        }
        SlotState::Mounted(mount) => rsx! { MountedSlot { mount: mount.clone() } },
    }
}

#[cfg(feature = "view")]
#[component]
fn InProcessSlot(reference: Capability, chrome: Option<Element>) -> Element {
    let mut state = use_signal(|| SlotState::Loading);
    // Reactive over the prop: dropping the old future drops its redemption.
    use_future(use_reactive!(|(reference,)| async move {
        state.set(SlotState::Loading);
        follow_provider(reference, |next| state.set(next)).await;
    }));
    render_state(&state.read(), &chrome)
}

/// A guest slot's state, shared between its component and the runtime task that follows the
/// provider. Nothing polls Dioxus tasks in a guest, so the task cannot write a signal: it
/// stores the state here and asks the runtime to tick every presentation, which re-renders the
/// component that subscribed to the tick.
#[cfg(feature = "guest")]
#[component]
fn GuestSlot(reference: Capability, chrome: Option<Element>) -> Element {
    let tick = use_context::<crate::guest::ChangeTick>().0;
    let _ = tick();
    let (state, cancel) = use_hook(|| {
        let state = Rc::new(std::cell::RefCell::new(SlotState::Loading));
        let runtime = crate::guest::Runtime::current();
        let cancel = runtime.spawn_for_view(Box::pin({
            let state = Rc::clone(&state);
            let runtime = Rc::clone(&runtime);
            follow_provider(reference, move |next| {
                *state.borrow_mut() = next;
                runtime.notify_views();
            })
        }));
        (state, cancel)
    });
    use_drop(move || cancel.cancel());
    render_state(&state.borrow(), &chrome)
}

#[component]
fn MountedSlot(mount: Mount) -> Element {
    match mount.0 {
        #[cfg(feature = "in_process")]
        MountInner::InProcess(owner, reference) => {
            rsx! { ComponentMount { owner, reference } }
        }
        #[cfg(feature = "guest")]
        MountInner::Guest(ref component) => {
            // A changed component is another element, so the one that is written is never
            // rewritten (see `wire::SLOT_ATTRIBUTE`).
            let key = Rc::as_ptr(component) as usize;
            rsx! { GuestMount { key: "{key}", mount } }
        }
    }
}

/// The element a guest emits for a mounted component: it carries the slot id and nothing else,
/// and the wire writer turns the attribute into `mount-component`. `display: contents` keeps the
/// element itself out of the layout, so the component lays out as if mounted in its place.
#[cfg(feature = "guest")]
#[component]
fn GuestMount(mount: Mount) -> Element {
    // With only the guest backend compiled, `MountInner` has one variant and the pattern cannot
    // fail; with both backends it can, if an in-process mount reached a guest slot.
    #[cfg_attr(
        not(feature = "in_process"),
        expect(irrefutable_let_patterns, reason = "single-backend build")
    )]
    let MountInner::Guest(component) = mount.0 else {
        unreachable!("guest slots mount guest components");
    };
    let slots = use_context::<Rc<crate::guest::Slots>>();
    let id = use_hook(|| slots.hold(component));
    use_drop(move || slots.retire(id));
    let id = i64::try_from(id).expect("slot ids stay within i64");
    rsx! { div { style: "display: contents", "data-clay-slot": id } }
}

/// What tells a hook its source changed. In-process, the source's own stream is awaited by a
/// task in the renderer's executor; in a guest nothing polls Dioxus tasks, so the runtime
/// pushes each change into the presentation's tick signal instead.
#[derive(Clone)]
pub(crate) enum Changes {
    #[cfg(feature = "in_process")]
    Document(tokio::sync::watch::Receiver<clay_docs::DocumentSnapshot>),
    #[cfg(feature = "in_process")]
    Rows(crate::rows::Observer),
    #[cfg(feature = "guest")]
    Pushed,
}

/// Re-render the calling component on every change `changes` reports. The subscription belongs
/// to the calling component's scope, so dropping it stops listening. Which branch runs is fixed
/// by the handle's backend, so a component calls the same hooks on every render.
fn use_changes(source: impl FnOnce() -> Changes) {
    let changes = use_hook(source);
    match changes {
        #[cfg(feature = "in_process")]
        Changes::Document(changes) => {
            let mut tick = use_signal(|| 0_u64);
            use_future(move || {
                let mut changes = changes.clone();
                async move {
                    // The watch ending means the incarnation did.
                    while changes.changed().await.is_ok() {
                        tick += 1;
                    }
                }
            });
            let _ = tick();
        }
        #[cfg(feature = "in_process")]
        Changes::Rows(observer) => {
            let mut tick = use_signal(|| 0_u64);
            use_future(move || {
                let mut observer = observer.clone();
                async move {
                    // The feed task ends with the incarnation, which closes the stream.
                    while observer.changed().await.is_ok() {
                        tick += 1;
                    }
                }
            });
            let _ = tick();
        }
        #[cfg(feature = "guest")]
        Changes::Pushed => {
            let _ = use_context::<crate::guest::ChangeTick>().0();
        }
    }
}

/// Bind a collection mirror to Dioxus reactivity: the returned rows are the mirror's live rows
/// at this render, decoded once per mirror revision, and the calling component re-renders on
/// every change the mirror folds in.
pub fn use_rows<K, V>(collection: &Collection<K, V>) -> Rc<[Row<K, V>]>
where
    K: DeserializeOwned + 'static,
    V: DeserializeOwned + 'static,
{
    use_changes(|| collection.changes());
    collection
        .rows()
        .expect("the collection mirror outlives the view that reads it")
}

/// Bind a configuration facet to Dioxus reactivity: the returned snapshot is current at this
/// render and the calling component re-renders on every change of the facet's document watch.
pub fn use_facet<T>(facet: &Facet<T>) -> FacetSnapshot<T>
where
    T: Clone + DeserializeOwned + Serialize + 'static,
{
    use_changes(|| {
        facet
            .changes()
            .expect("the document watch outlives the view that reads it")
    });
    facet
        .current()
        .expect("the document watch outlives the view that reads it")
}

/// Bind a reference's [`RefStatus`] to Dioxus reactivity without activating its provider: the
/// returned status is current at this render, and the calling component re-renders whenever the
/// supervisor's status ticks move it (a disabled micro enabled, a faulted one retried). A view
/// uses it to show an action whose provider cannot start as unavailable before it is pressed.
/// A reference that changes between renders is followed from then on.
pub fn use_ref_status(reference: &Capability) -> RefStatus {
    let mut status = use_signal(|| reference.status());
    use_resource(use_reactive((reference,), move |(reference,)| async move {
        loop {
            let seen = reference.status();
            if *status.peek() != seen {
                status.set(seen);
            }
            reference.status_changed(seen).await;
        }
    }));
    status()
}

#[cfg(all(test, feature = "in_process"))]
mod tests;
