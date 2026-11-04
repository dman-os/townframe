//! The view-side half of the authoring surface: Dioxus hooks that bind the handles a micro's
//! `activate` captured to reactivity, and (in-process) the slot components that mount another
//! micro's published component into the host tree *through the installed renderer*. Micro views
//! reach both through this module and so never name a renderer crate (ADR 015 §3 dependency
//! direction).
use crate::Collection;
use crate::Facet;
use crate::FacetSnapshot;
use crate::Row;
use dioxus::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::rc::Rc;

#[cfg(feature = "view")]
use crate::Capability;
#[cfg(feature = "view")]
use crate::ComponentRef;
#[cfg(feature = "view")]
use crate::Instance;
#[cfg(feature = "view")]
use clay_dioxus::ComponentMount;

/// The view-side counterpart of [`crate::Context::publish_component`]: mounts a published
/// component into the host tree, resolving the renderer registry from Dioxus context.
#[cfg(feature = "view")]
#[derive(Props, Clone, PartialEq)]
pub struct ComponentSlotProps {
    pub owner: Instance,
    pub reference: ComponentRef,
}

#[cfg(feature = "view")]
#[expect(non_snake_case, reason = "Dioxus component naming")]
pub fn ComponentSlot(props: ComponentSlotProps) -> Element {
    let reference = props.reference.host();
    rsx! { ComponentMount { owner: props.owner, reference } }
}

#[cfg(feature = "view")]
#[derive(Props, Clone, PartialEq)]
pub struct PresentationSlotProps {
    pub owner: Instance,
    pub reference: Capability,
}

/// What a slot shows: the presentation it redeemed, or why there is none right now.
#[cfg(feature = "view")]
enum SlotState {
    Loading,
    Unavailable(crate::Error),
    Mounted(clay_host::ComponentRef),
}

/// Mount the presentation a row attaches. The redemption is asynchronous (a lazy reference
/// activates its provider) and the redeemed proxy is held for exactly as long as this
/// component lives, so unmounting it releases the provider (ADR 031 §9).
///
/// The slot follows its provider rather than redeeming once. A redemption that fails renders
/// its failure, and the next redemption waits for the reference's status to move off the
/// status the failure left it in (a disabled micro enabled, an unconfigured one configured, a
/// faulted one `retry`'d), so a provider that keeps failing is never redeemed in a loop. A
/// mounted presentation whose incarnation ends renders as loading and is redeemed again, which
/// demand-starts the provider's next incarnation. Both waits are woken by the supervisor's
/// status ticks; nothing polls.
#[cfg(feature = "view")]
#[expect(non_snake_case, reason = "Dioxus component naming")]
pub fn PresentationSlot(props: PresentationSlotProps) -> Element {
    let mut state = use_signal(|| SlotState::Loading);
    // Reactive over the prop: a row that keeps its key but attaches another target redeems
    // anew, and dropping the old future drops its redemption, releasing the previous provider.
    let reference = props.reference.clone();
    use_future(use_reactive!(|(reference,)| {
        let reference = reference.clone();
        async move {
            state.set(SlotState::Loading);
            // `None` for a reference to a live publication, which has no restart to follow.
            let mut changes = reference.status_changes();
            loop {
                let failure = match reference.redeem_presentation().await {
                    Ok(proxy) => match proxy.component() {
                        Ok(component) => {
                            state.set(SlotState::Mounted(component));
                            // The proxy is revoked when its incarnation closes, which the
                            // supervisor records as a status change right after.
                            while !proxy.is_revoked() {
                                let Some(changes) = changes.as_mut() else {
                                    return std::future::pending().await;
                                };
                                if changes.changed().await.is_err() {
                                    return;
                                }
                            }
                            state.set(SlotState::Loading);
                            continue;
                        }
                        Err(failure) => crate::Error::from(failure),
                    },
                    Err(failure) => failure,
                };
                // Redeeming again before the status moves would only fail the same way.
                let failed_at = reference.status();
                state.set(SlotState::Unavailable(failure));
                while reference.status() == failed_at {
                    let Some(changes) = changes.as_mut() else {
                        return std::future::pending().await;
                    };
                    if changes.changed().await.is_err() {
                        return;
                    }
                }
            }
        }
    }));
    match &*state.read() {
        SlotState::Loading => rsx! { p { role: "status", "Loading section…" } },
        SlotState::Unavailable(failure) => {
            rsx! { p { role: "alert", "Section unavailable: {failure:?}" } }
        }
        SlotState::Mounted(reference) => {
            rsx! { ComponentMount { owner: props.owner, reference: *reference } }
        }
    }
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

#[cfg(all(test, feature = "in_process"))]
mod tests;
