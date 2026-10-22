//! The view-side backend: Dioxus presentation glue over the renderer-agnostic authoring surface.
//! A micro's own view is plain Dioxus; what lives here are the two pieces that must resolve host
//! state *through the installed renderer* — the slot component that mounts a published
//! [`ComponentRef`] into the host tree, and the hook that drives a host collection reader from
//! Dioxus reactivity. Micro views reach both through this module and so never name a renderer
//! crate (ADR 015 §3 dependency direction).
use crate::ComponentRef;
use crate::native::CollectionReader;
use crate::native::Instance;
use crate::native::ReadOnly;
use crate::native::View;
use clay_dioxus::ComponentMount;
use dioxus::prelude::*;

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

/// Bind a host collection reader to Dioxus reactivity: the returned view re-renders its component
/// on every committed batch and reports closure once the host withdraws the collection. The
/// reader's advance loop is owned by the calling component's scope, so dropping the view stops
/// polling it.
pub fn use_collection<T: Clone + PartialEq + Send + Sync + 'static>(
    reader: &CollectionReader<T>,
) -> View<u64, T, ReadOnly> {
    let (view, binding, _listener, tick) = use_hook(|| {
        let binding = reader
            .view()
            .expect("collection reader must be live on mount");
        let view = binding.view().clone();
        let tick = Signal::new(0_u64);
        let listener = view.subscribe(move |_event| {
            let mut tick = tick;
            tick += 1;
        });
        (
            view,
            std::rc::Rc::new(std::cell::RefCell::new(Some(binding))),
            std::rc::Rc::new(listener),
            tick,
        )
    });
    use_future(move || {
        let binding = binding.clone();
        async move {
            let Some(mut binding) = binding.borrow_mut().take() else {
                return;
            };
            while binding.advance().await {}
        }
    });
    let _ = tick();
    view
}

#[cfg(test)]
mod tests;
