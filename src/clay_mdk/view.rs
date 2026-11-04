//! The view-side backend: Dioxus presentation glue over the renderer-agnostic authoring surface.
//! A micro's own view is plain Dioxus; what lives here are the two pieces that must resolve host
//! state *through the installed renderer* — the slot component that mounts a published
//! [`ComponentRef`] into the host tree, and the hook that drives a host collection reader from
//! Dioxus reactivity. Micro views reach both through this module and so never name a renderer
//! crate (ADR 015 §3 dependency direction).
use crate::ComponentRef;
use crate::native::CollectionWatch;
use crate::native::Instance;
use crate::native::Row;
use clay_dioxus::ComponentMount;
use dioxus::prelude::*;
use serde::de::DeserializeOwned;

/// The view-side counterpart of [`crate::native::Context::publish_component`]: mounts a published
/// component into the host tree, resolving the renderer registry from Dioxus context. Backend-owned
/// so micro views never name a renderer crate; the WIT backend gains the matching slot component
/// with the collections waist.
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

/// Bind a collection mirror to Dioxus reactivity: the returned rows are the mirror's live rows
/// at this render, and the calling component re-renders on every change the mirror folds in.
/// The subscription belongs to the calling component's scope, so dropping it stops listening.
pub fn use_rows<K, V>(watch: &CollectionWatch<K, V>) -> Vec<Row<K, V>>
where
    K: DeserializeOwned + 'static,
    V: DeserializeOwned + 'static,
{
    let mut tick = use_signal(|| 0_u64);
    let revisions = use_hook(|| watch.revisions());
    use_future(move || {
        let mut revisions = revisions.clone();
        async move {
            // The sender lives in the incarnation's feed task; it ends when the incarnation does.
            while revisions.changed().await.is_ok() {
                tick += 1;
            }
        }
    });
    let _ = tick();
    watch
        .rows()
        .expect("the collection mirror outlives the view that reads it")
}

#[cfg(test)]
mod tests;
