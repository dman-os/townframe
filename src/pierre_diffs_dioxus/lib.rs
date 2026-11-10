// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/Virtualizer.ts
// (lifecycle: construction, `setup(root, contentContainer)`, `connect(...)` and
// `cleanUp()`, which upstream's callers drive from DOM lifecycle).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the logic is `pierre_diffs::components::virtualizer`; this crate is
// the Dioxus-web layer. `VirtualizerRoot` is the scroll container element that
// builds a `Virtualizer` over a `WebHost`, calls `setup` when it mounts and
// `clean_up` when it unmounts; `use_virtualizer` / `use_document_virtualizer`
// are the same lifecycle without an element (the document is the root);
// `use_virtualizer_instance` returns the `onmounted` handler that connects a
// `SubscribedInstance` to the context virtualizer and disconnects it on drop.
// Upstream's React adapter (`react/Virtualizer.tsx`) is the analogue: a
// component that owns the instance and exposes it through context.

mod host;

use std::cell::RefCell;
use std::rc::Rc;

use dioxus::prelude::*;
pub use host::DomElement;
pub use host::WebHost;
use pierre_diffs::components::virtualizer::InstanceRef;
use pierre_diffs::components::virtualizer::Root;
use pierre_diffs::components::virtualizer::Virtualizer;
use pierre_diffs::components::virtualizer::VirtualizerConfig;
use pierre_diffs::managers::universal_rendering_manager::UniversalRenderingManager;
use wasm_bindgen::JsCast;
use web_sys::HtmlElement;

/// The disposer `connect` returns, once an instance is connected.
type DisconnectSlot = Rc<RefCell<Option<Box<dyn Fn()>>>>;

/// The virtualizer a subtree shares through context.
#[derive(Clone)]
pub struct VirtualizerHandle {
    virtualizer: Rc<Virtualizer<WebHost>>,
}

impl VirtualizerHandle {
    pub fn new(config: VirtualizerConfig) -> Self {
        let host = WebHost::new();
        let virtualizer = Virtualizer::new(
            Rc::clone(&host),
            UniversalRenderingManager::new(Rc::clone(&host)),
            config,
        );
        host.bind(Rc::downgrade(&virtualizer));
        Self { virtualizer }
    }

    pub fn virtualizer(&self) -> &Rc<Virtualizer<WebHost>> {
        &self.virtualizer
    }

    fn setup_element(&self, mounted: &MountedEvent) {
        let element = mounted_html_element(mounted);
        self.virtualizer
            .setup(Root::Element(DomElement::new(element)), None);
    }
}

fn mounted_html_element(mounted: &MountedEvent) -> HtmlElement {
    mounted
        .data()
        .downcast::<web_sys::Element>()
        .expect("onmounted data of the web renderer is a web_sys::Element")
        .clone()
        .dyn_into::<HtmlElement>()
        .expect("the virtualizer root is an HTMLElement")
}

/// Creates the virtualizer once per component and cleans it up on drop.
pub fn use_virtualizer(config: VirtualizerConfig) -> VirtualizerHandle {
    let handle = use_hook(|| VirtualizerHandle::new(config));
    let for_drop = handle.clone();
    use_drop(move || for_drop.virtualizer.clean_up());
    handle
}

/// A virtualizer rooted at the document (page scrolling): `setup(document)`
/// runs once after the first render.
pub fn use_document_virtualizer(config: VirtualizerConfig) -> VirtualizerHandle {
    let handle = use_virtualizer(config);
    let for_setup = handle.clone();
    use_effect(move || for_setup.virtualizer.setup(Root::Document, None));
    handle
}

#[derive(Props, Clone, PartialEq)]
pub struct VirtualizerRootProps {
    #[props(default)]
    pub config: VirtualizerConfig,
    #[props(default)]
    pub class: String,
    /// The root must be a scroll container (`overflow: auto` and a height) for
    /// the window computation to mean anything.
    #[props(default)]
    pub style: String,
    pub children: Element,
}

/// `new Virtualizer(config)` plus `setup(root)` for an element root. The first
/// child element becomes the content container (upstream's
/// `root.firstElementChild` default); descendants connect through
/// `use_virtualizer_instance`.
#[component]
pub fn VirtualizerRoot(props: VirtualizerRootProps) -> Element {
    let handle = use_virtualizer(props.config);
    use_context_provider(|| handle.clone());
    rsx! {
        div {
            class: "{props.class}",
            style: "{props.style}",
            onmounted: move |mounted| handle.setup_element(&mounted),
            {props.children}
        }
    }
}

/// Returns the `onmounted` handler that connects `instance` to the context
/// virtualizer (a `VirtualizerRoot` or `use_document_virtualizer` ancestor)
/// with the mounted element as the container, and disconnects it when the
/// component drops.
pub fn use_virtualizer_instance(instance: InstanceRef) -> Callback<MountedEvent> {
    let handle = use_context::<VirtualizerHandle>();
    let disconnect: DisconnectSlot = use_hook(Rc::default);
    let for_drop = Rc::clone(&disconnect);
    use_drop(move || {
        if let Some(disconnect) = for_drop.borrow_mut().take() {
            disconnect();
        }
    });
    use_callback(move |mounted: MountedEvent| {
        let container = DomElement::new(mounted_html_element(&mounted));
        let disconnect_fn = handle.virtualizer.connect(container, Rc::clone(&instance));
        *disconnect.borrow_mut() = Some(disconnect_fn);
    })
}
