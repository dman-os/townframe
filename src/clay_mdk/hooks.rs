//! The view-side half of the authoring surface: Dioxus hooks that bind the handles a micro's
//! `activate` captured to reactivity, and the slot components that mount another micro's
//! published component into the host tree *through the installed renderer*. Micro views reach
//! both through this module and so never name a renderer crate (ADR 015 §3 dependency
//! direction).
use crate::Capability;
use crate::Collection;
use crate::ComponentRef;
use crate::Facet;
use crate::FacetSnapshot;
use crate::Instance;
use crate::Row;
use clay_dioxus::ComponentMount;
use dioxus::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::rc::Rc;

/// The view-side counterpart of [`crate::Context::publish_component`]: mounts a published
/// component into the host tree, resolving the renderer registry from Dioxus context.
#[derive(Props, Clone, PartialEq)]
pub struct ComponentSlotProps {
    pub owner: Instance,
    pub reference: ComponentRef,
}

#[expect(non_snake_case, reason = "Dioxus component naming")]
pub fn ComponentSlot(props: ComponentSlotProps) -> Element {
    let reference = props.reference.host();
    rsx! { ComponentMount { owner: props.owner, reference } }
}

#[derive(Props, Clone, PartialEq)]
pub struct PresentationSlotProps {
    pub owner: Instance,
    pub reference: Capability,
}

/// Mount the presentation a row attaches. The redemption is asynchronous (a lazy reference
/// activates its provider) and the redeemed proxy is held for exactly as long as this
/// component lives, so unmounting it releases the provider (ADR 025 §9). A reference that
/// cannot be redeemed renders its failure instead of a mount.
#[expect(non_snake_case, reason = "Dioxus component naming")]
pub fn PresentationSlot(props: PresentationSlotProps) -> Element {
    // Reactive over the prop: a row that keeps its key but attaches another target redeems
    // anew, and dropping the old redemption releases the previous provider.
    let reference = props.reference.clone();
    let redeemed = use_resource(use_reactive!(|(reference,)| {
        let reference = reference.clone();
        async move { reference.redeem_presentation().await }
    }));
    let redeemed = redeemed.read();
    match &*redeemed {
        None => rsx! { p { role: "status", "Loading section…" } },
        Some(Err(failure)) => rsx! { p { role: "alert", "Section unavailable: {failure:?}" } },
        Some(Ok(component)) => match component.component() {
            Ok(reference) => {
                rsx! { ComponentMount { owner: props.owner, reference } }
            }
            Err(failure) => rsx! { p { role: "alert", "Section unavailable: {failure:?}" } },
        },
    }
}

/// Bind a collection mirror to Dioxus reactivity: the returned rows are the mirror's live rows
/// at this render, decoded once per mirror revision, and the calling component re-renders on
/// every change the mirror folds in. The subscription belongs to the calling component's scope,
/// so dropping it stops listening.
pub fn use_rows<K, V>(collection: &Collection<K, V>) -> Rc<[Row<K, V>]>
where
    K: DeserializeOwned + 'static,
    V: DeserializeOwned + 'static,
{
    let mut tick = use_signal(|| 0_u64);
    let changes = use_hook(|| collection.clone());
    use_future(move || {
        let mut changes = changes.clone();
        async move {
            // The feed task ends with the incarnation, which closes the stream.
            while changes.changed().await.is_ok() {
                tick += 1;
            }
        }
    });
    let _ = tick();
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
    let mut tick = use_signal(|| 0_u64);
    let changes = use_hook(|| {
        facet
            .changes()
            .expect("the document watch outlives the view that reads it")
    });
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
    facet
        .current()
        .expect("the document watch outlives the view that reads it")
}

#[cfg(test)]
mod tests;
