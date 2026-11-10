//! Projection of a retained [`SurfaceTree`] into the host `VirtualDom`, and the static vocabulary
//! (tags, attributes, listeners) that projection can name. Ingress validation asks this module
//! whether a name is projectable, so a batch can never carry a name the renderer cannot emit.
use crate::tree::SurfaceNodeId;
use crate::tree::SurfaceNodeKind;
use crate::tree::SurfaceTree;
use crate::wire::AttributeValue;
use crate::wire::ElementHandles;
use crate::wire::Event;
use crate::wire::NodeId;
use crate::wire::SurfaceEventHandler;
use dioxus::prelude::*;
use dioxus_core::ListenerCallback;
use dioxus_html::PlatformEventData;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;

impl<C: Clone> SurfaceTree<C> {
    /// Project the whole tree. Every node is rebuilt on every call, whatever changed; see
    /// [`SurfaceRoot`] for the projection that rebuilds only what a batch touched.
    pub fn render(
        &self,
        dispatch: &SurfaceEventHandler,
        component: &dyn Fn(&C) -> Element,
        handles: &ElementHandles,
    ) -> Element {
        let root_children = &self.node(SurfaceNodeId::ROOT).children;
        rsx! {
            for &child in root_children {
                {self.render_node(child, dispatch, component, handles)}
            }
        }
    }

    pub(crate) fn render_node(
        &self,
        id: SurfaceNodeId,
        dispatch: &SurfaceEventHandler,
        component: &dyn Fn(&C) -> Element,
        handles: &ElementHandles,
    ) -> Element {
        let node = self.node(id);
        match &node.kind {
            SurfaceNodeKind::Root => unreachable!("root is never a child node"),
            SurfaceNodeKind::Text(text) => rsx! {
                Fragment { key: "{id.1}", "{text}" }
            },
            SurfaceNodeKind::Component(reference) => rsx! {
                Fragment { key: "{id.1}", {component(reference)} }
            },
            SurfaceNodeKind::Element { tag, namespace, .. } => {
                let attrs = self.element_attributes(id, dispatch, handles);
                let children = node
                    .children
                    .iter()
                    .map(|&child| self.render_node(child, dispatch, component, handles));
                render_element_tag(tag, namespace.as_deref(), id.1, attrs, children)
            }
        }
    }

    /// The host attributes and listeners of element `id`. A listener carries the node's
    /// generation, so the host can tell an event for a node that is gone from one for a node
    /// that merely changed (see [`SurfaceTree::event_is_current`]).
    fn element_attributes(
        &self,
        id: SurfaceNodeId,
        dispatch: &SurfaceEventHandler,
        handles: &ElementHandles,
    ) -> Vec<dioxus_core::Attribute> {
        let SurfaceNodeKind::Element {
            attributes,
            listeners,
            element_id,
            ..
        } = &self.node(id).kind
        else {
            unreachable!("element_attributes is called on elements")
        };
        let mut attrs = Vec::with_capacity(attributes.len() + listeners.len());
        for ((name, ns), value) in attributes {
            let (attr_name, attr_ns) =
                static_attribute(name, ns.as_deref()).expect("validated surface attribute");
            let volatile = attr_name == "value" || attr_name == "checked";
            let attr_value = match value {
                AttributeValue::Text(text) => dioxus_core::AttributeValue::Text(text.clone()),
                AttributeValue::Float(float) => dioxus_core::AttributeValue::Float(*float),
                AttributeValue::Int(int) => dioxus_core::AttributeValue::Int(*int),
                AttributeValue::Boolean(bool) => dioxus_core::AttributeValue::Bool(*bool),
                AttributeValue::None => dioxus_core::AttributeValue::None,
            };
            attrs.push(dioxus_core::Attribute::new(
                attr_name, attr_value, attr_ns, volatile,
            ));
        }
        if let Some(guest_element) = *element_id {
            let guest_node_id =
                NodeId::try_from(guest_element.raw()).expect("element id fits NodeId");
            for listener_name in listeners {
                attrs.push(
                    surface_listener_attribute(
                        listener_name,
                        id.1,
                        guest_node_id,
                        dispatch.clone(),
                        handles,
                    )
                    .expect("validated surface listener"),
                );
            }
        }
        attrs
    }
}

/// What every node of one incrementally projected surface shares. Its parts are the same objects
/// from one revision to the next, so equality is identity and costs four pointer comparisons.
pub struct SurfaceProjection<C: 'static> {
    pub tree: Arc<Mutex<SurfaceTree<C>>>,
    pub dispatch: SurfaceEventHandler,
    pub component: Rc<dyn Fn(&C) -> Element>,
    pub handles: ElementHandles,
}

impl<C> Clone for SurfaceProjection<C> {
    fn clone(&self) -> Self {
        Self {
            tree: self.tree.clone(),
            dispatch: self.dispatch.clone(),
            component: self.component.clone(),
            handles: self.handles.clone(),
        }
    }
}

impl<C> PartialEq for SurfaceProjection<C> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.tree, &other.tree)
            && Rc::ptr_eq(&self.dispatch, &other.dispatch)
            && Rc::ptr_eq(&self.component, &other.component)
            && self.handles == other.handles
    }
}

/// The incremental projection of a retained tree: a `VirtualDom` component per node that has
/// element grandchildren, memoized on `(node, subtree stamp)`. A batch moves the stamp of every
/// node it touched and of that node's ancestors, so on the next render Dioxus skips every
/// subtree the batch left alone and rebuilds only the touched ones (plus, per touched parent,
/// the cheap component element of each of its children). Leaf-level elements (a span and its
/// text, say) are built inline by their parent: a component per text node would cost more than
/// it saves. `stamp` is the root's, so the root re-renders only when something changed.
#[component]
pub fn SurfaceRoot<C: Clone + 'static>(projection: SurfaceProjection<C>, stamp: u64) -> Element {
    let _ = stamp;
    let tree = projection.tree.lock().unwrap();
    let root_children = &tree.node(SurfaceNodeId::ROOT).children;
    rsx! {
        for &child in root_children {
            {tree.render_child(child, &projection)}
        }
    }
}

#[component]
fn SurfaceNodeView<C: Clone + 'static>(
    projection: SurfaceProjection<C>,
    id: SurfaceNodeId,
    stamp: u64,
) -> Element {
    let _ = stamp;
    let tree = projection.tree.lock().unwrap();
    tree.render_incremental(id, &projection)
}

impl<C: Clone + 'static> SurfaceTree<C> {
    /// Whether `id` gets its own component: an element with an element child that itself has
    /// children, i.e. one whose subtree is more than a tag around text.
    fn is_boundary(&self, id: SurfaceNodeId) -> bool {
        let node = self.node(id);
        matches!(node.kind, SurfaceNodeKind::Element { .. })
            && node.children.iter().any(|&child| {
                let child = self.node(child);
                matches!(child.kind, SurfaceNodeKind::Element { .. }) && !child.children.is_empty()
            })
    }

    fn render_child(&self, id: SurfaceNodeId, projection: &SurfaceProjection<C>) -> Element {
        if self.is_boundary(id) {
            rsx! {
                SurfaceNodeView::<C> {
                    key: "{id.1}",
                    projection: projection.clone(),
                    id,
                    stamp: self.node(id).subtree,
                }
            }
        } else {
            self.render_incremental(id, projection)
        }
    }

    fn render_incremental(&self, id: SurfaceNodeId, projection: &SurfaceProjection<C>) -> Element {
        let node = self.node(id);
        match &node.kind {
            SurfaceNodeKind::Root => unreachable!("root is never a child node"),
            SurfaceNodeKind::Text(text) => rsx! {
                Fragment { key: "{id.1}", "{text}" }
            },
            SurfaceNodeKind::Component(reference) => rsx! {
                Fragment { key: "{id.1}", {(projection.component)(reference)} }
            },
            SurfaceNodeKind::Element { tag, namespace, .. } => {
                let attrs = self.element_attributes(id, &projection.dispatch, &projection.handles);
                let children = node
                    .children
                    .iter()
                    .map(|&child| self.render_child(child, projection));
                render_element_tag(tag, namespace.as_deref(), id.1, attrs, children)
            }
        }
    }
}

pub(crate) fn static_attribute(
    name: &str,
    namespace: Option<&str>,
) -> Option<(&'static str, Option<&'static str>)> {
    macro_rules! vocabulary {
        ($($ns:pat => $canonical:expr => [$($name:literal),* $(,)?]),* $(,)?) => {
            match (name, namespace) {
                $($(($name, $ns) => Some(($name, $canonical)),)*)*
                _ => None,
            }
        };
    }
    vocabulary!(
        None => None => [
            "id", "class", "style", "title", "role", "tabindex", "hidden", "lang", "dir",
            "accesskey", "contenteditable", "draggable", "spellcheck", "translate",
            "value", "checked", "disabled", "readonly", "required", "multiple", "selected",
            "name", "type", "placeholder", "autocomplete", "autofocus", "inputmode", "pattern",
            "min", "max", "step", "minlength", "maxlength", "size", "rows", "cols", "wrap",
            "for", "form", "action", "method", "enctype", "novalidate", "accept", "accept-charset",
            "href", "target", "rel", "download", "src", "alt", "width", "height", "loading",
            "srcset", "sizes", "crossorigin", "referrerpolicy", "open", "datetime", "start",
            "reversed", "colspan", "rowspan", "scope", "headers", "label", "low", "high",
            "optimum", "data-testid", "data-state", "data-value",
            "aria-label", "aria-labelledby", "aria-describedby", "aria-hidden", "aria-live",
            "aria-atomic", "aria-busy", "aria-controls", "aria-current", "aria-disabled",
            "aria-expanded", "aria-haspopup", "aria-invalid", "aria-modal", "aria-multiline",
            "aria-multiselectable", "aria-orientation", "aria-pressed", "aria-readonly",
            "aria-required", "aria-selected", "aria-checked", "aria-valuemin", "aria-valuemax",
            "aria-valuenow", "aria-valuetext", "aria-level", "aria-posinset", "aria-setsize",
            "viewBox", "preserveAspectRatio", "d", "fill", "fill-rule", "fill-opacity", "stroke",
            "stroke-width", "stroke-linecap", "stroke-linejoin", "stroke-dasharray",
            "stroke-dashoffset", "stroke-opacity", "opacity", "transform", "cx", "cy", "r",
            "rx", "ry", "x", "y", "x1", "y1", "x2", "y2", "points", "xmlns", "clip-path"
        ],
        Some("style") => Some("style") => [
            "display", "position", "top", "right", "bottom", "left", "inset", "z-index",
            "width", "height", "min-width", "max-width", "min-height", "max-height",
            "margin", "margin-top", "margin-right", "margin-bottom", "margin-left",
            "padding", "padding-top", "padding-right", "padding-bottom", "padding-left",
            "gap", "row-gap", "column-gap", "flex", "flex-direction", "flex-wrap", "flex-grow",
            "flex-shrink", "flex-basis", "align-items", "align-self", "justify-content",
            "grid-template-columns", "grid-template-rows", "grid-column", "grid-row",
            "overflow", "overflow-x", "overflow-y", "color", "background", "background-color",
            "border", "border-color", "border-width", "border-style", "border-radius",
            "font", "font-family", "font-size", "font-weight", "font-style", "line-height",
            "text-align", "text-decoration", "text-overflow", "white-space", "word-break",
            "cursor", "pointer-events", "user-select", "opacity", "visibility", "box-sizing",
            "box-shadow", "transform", "transform-origin", "transition", "object-fit"
        ],
        Some("http://www.w3.org/1999/xlink") => Some("http://www.w3.org/1999/xlink") => ["href"]
    )
}

macro_rules! expand_surface_event_listener {
    (
        enum Event {
            $(
                #[convert = $converter:ident]
                #[events = [
                    $(
                        $( #[$attr:meta] )*
                        $name:ident => $raw:ident,
                    )*
                ]]
                $(#[raw = [$($raw_only:ident),* $(,)?]])?
                $group:ident($data:ident),
            )*
        }
    ) => {
        pub(crate) fn supported_listener(name: &str) -> bool {
            match name {
                $($(stringify!($raw) => true,)*)*
                _ => false,
            }
        }

        fn surface_listener_attribute(
            raw_name: &str,
            revision: u64,
            element: NodeId,
            dispatch: SurfaceEventHandler,
            handles: &ElementHandles,
        ) -> Option<dioxus_core::Attribute> {
            match raw_name {
                $(
                    $(
                        stringify!($raw) => {
                            let event_name = stringify!($raw);
                            let handles = handles.clone();
                            let listener = ListenerCallback::new(
                                move |evt: dioxus_core::Event<PlatformEventData>| {
                                    crate::probe::mark("listener-in");
                                    let platform_rc = evt.data();
                                    let data_json = expand_surface_event_listener!(
                                        @serialize $group, $data, platform_rc.as_ref(),
                                        element, &handles
                                    );
                                    crate::probe::mark("encoded");
                                    let response = dispatch(Event {
                                        revision,
                                        element,
                                        name: event_name.to_owned(),
                                        bubbles: evt.propagates(),
                                        data_json,
                                    });
                                    if response.prevent_default {
                                        evt.prevent_default();
                                    }
                                    if response.stop_propagation {
                                        evt.stop_propagation();
                                    }
                                },
                            );
                            return Some(dioxus_core::Attribute::new(
                                stringify!($name),
                                dioxus_core::AttributeValue::Listener(listener.erase()),
                                None,
                                false,
                            ));
                        }
                    )*
                )*
                _ => None,
            }
        }
    };
    // A mounted element's host handle is what element commands act on; the guest is told only
    // that the element exists (its payload is `null`).
    (@serialize Mounted, $data:ident, $platform:expr, $element:expr, $handles:expr) => {{
        let platform: &PlatformEventData = $platform;
        $handles.insert($element, dioxus_html::MountedData::from(platform));
        "null".to_owned()
    }};
    (@serialize $group:ident, $data:ident, $platform:expr, $element:expr, $handles:expr) => {{
        let _ = (&$element, &$handles);
        pastey::paste! {
            let platform: &PlatformEventData = $platform;
            if let Some(serialized) = platform.downcast::<dioxus_html::[<Serialized $data>]>() {
                serde_json::to_string(serialized).expect("event data serializes to JSON")
            } else {
                let converted = dioxus_html::$data::from(platform);
                serde_json::to_string(&converted).expect("event data serializes to JSON")
            }
        }
    }};
}

dioxus_html::with_html_event_groups!(expand_surface_event_listener);

macro_rules! surface_tags {
    ($($namespace:pat => [$($tag:ident),* $(,)?]),* $(,)?) => {
        pub(crate) fn supported_tag(tag: &str, namespace: Option<&str>) -> bool {
            match (tag, namespace) {
                $($((stringify!($tag), $namespace) => true,)*)*
                _ => false,
            }
        }

        fn render_element_tag(
            tag: &str,
            namespace: Option<&str>,
            key: u64,
            attrs: Vec<dioxus_core::Attribute>,
            children: impl Iterator<Item = Element>,
        ) -> Element {
            match (tag, namespace) {
                $($((stringify!($tag), $namespace) => rsx! {
                    $tag {
                        key: "{key}",
                        ..attrs,
                        {children}
                    }
                },)*)*
                _ => unreachable!("validated surface element"),
            }
        }
    };
}

surface_tags!(
    None => [
        div, section, article, aside, header, footer, main, nav, h1, h2, h3, h4, h5, h6, p, span,
        pre, code, blockquote, strong, em, small, b, i, u, s, mark, kbd, sub, sup, br, hr, ul, ol,
        li, dl, dt, dd, table, thead, tbody, tfoot, tr, th, td, caption, form, label, input,
        textarea, button, select, option, optgroup, fieldset, legend, details, summary, dialog, a,
        img, figure, figcaption, progress, meter, output, time, data
    ],
    Some("http://www.w3.org/2000/svg") => [
        svg, g, path, circle, rect, line, polyline, polygon, defs
    ]
);

#[cfg(test)]
mod tests;
