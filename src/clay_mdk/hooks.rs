//! The view-side backend: Dioxus presentation glue over the renderer-agnostic authoring surface.
//! A micro's own view is plain Dioxus; what lives here are the two pieces that must resolve host
//! state *through the installed renderer* — the slot component that mounts a published
//! [`ComponentRef`] into the host tree, and the hook that drives a host collection reader from
//! Dioxus reactivity. Micro views reach both through this module and so never name a renderer
//! crate (ADR 015 §3 dependency direction).
use crate::ComponentRef;
use crate::native::CapabilityRef;
use crate::native::CollectionWatch;
use crate::native::Instance;
use crate::native::Row;
use clay_dioxus::ComponentMount;
use dioxus::prelude::*;
use serde::de::DeserializeOwned;
use std::rc::Rc;

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

/// A row's attachment reference as a component prop. Two props are equal when the reference
/// stores back as the same attachment, so a re-rendered row that still names the same target
/// does not remount its slot.
#[derive(Clone)]
pub struct AttachedRef(Rc<CapabilityRef>);

impl AttachedRef {
    pub fn new(reference: CapabilityRef) -> Self {
        Self(Rc::new(reference))
    }
}

impl PartialEq for AttachedRef {
    fn eq(&self, other: &Self) -> bool {
        self.0.attachment() == other.0.attachment()
    }
}

#[derive(Props, Clone, PartialEq)]
pub struct PresentationSlotProps {
    pub owner: Instance,
    pub reference: AttachedRef,
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
        let reference = reference.0.share();
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
