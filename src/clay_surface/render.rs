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
use dioxus_core::Template;
use dioxus_core_template::RuntimeTemplateBuilder;
use dioxus_html::PlatformEventData;

/// The host-drawn box around every guest presentation. `contain: layout paint` makes the box the
/// containing block for absolutely and fixed positioned descendants (so a guest `position: fixed`
/// from `class` cannot reach the viewport) and clips painting to the box; `overflow: clip` repeats
/// the clip for renderers without `contain` paint support. The flex column with 100% size keeps
/// the guest root filling its slot as it did before the wrapper existed.
const PRESENTATION_BOX_STYLE: &str = "display:flex;flex-direction:column;flex:1 1 auto;width:100%;height:100%;min-width:0;min-height:0;position:relative;contain:layout paint;overflow:clip";

impl<C: Clone> SurfaceTree<C> {
    /// Projects the tree inside the presentation box. `nested` marks a presentation mounted by
    /// another guest: its box also ends event bubbling, so a parent presentation's listeners never
    /// see an event that began inside the child. A top-level presentation has no guest ancestor
    /// to protect and leaves bubbling to the host.
    pub fn render(
        &self,
        dispatch: &SurfaceEventHandler,
        component: &dyn Fn(&C) -> Element,
        handles: &ElementHandles,
        nested: bool,
    ) -> Element {
        let root_children = &self.node(SurfaceNodeId::ROOT).children;
        let barrier = if nested {
            presentation_barrier()
        } else {
            Vec::new()
        };
        rsx! {
            // The guest's sanitized stylesheet comes first, so every rule of the mount precedes
            // the mount's content. A renderer that gives the mount a shadow root (see
            // `MountedSurface`) scopes it there; otherwise it is a light-DOM `<style>`.
            if !self.stylesheet.is_empty() {
                style { "{self.stylesheet}" }
            }
            div {
                "data-clay-presentation": "",
                style: PRESENTATION_BOX_STYLE,
                ..barrier,
                for &child in root_children {
                    {self.render_node(child, dispatch, component, handles)}
                }
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
            SurfaceNodeKind::Element {
                tag,
                namespace,
                attributes,
                listeners,
                element_id,
            } => {
                let mut attrs = Vec::with_capacity(attributes.len() + listeners.len());
                for ((name, ns), value) in attributes {
                    let (attr_name, attr_ns) =
                        static_attribute(&DATA_ATTRIBUTES, name, ns.as_deref())
                            .expect("validated surface attribute")
                            .expect("validated surface attribute");
                    let volatile = attr_name == "value" || attr_name == "checked";
                    let attr_value = match value {
                        AttributeValue::Text(text) => {
                            dioxus_core::AttributeValue::Text(text.clone())
                        }
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
                                self.revision,
                                guest_node_id,
                                dispatch.clone(),
                                handles,
                            )
                            .expect("validated surface listener"),
                        );
                    }
                }
                let children = node
                    .children
                    .iter()
                    .map(|&child| self.render_node(child, dispatch, component, handles));
                let key = id.1;
                render_element_tag(tag, namespace.as_deref(), key, attrs, children)
            }
        }
    }
}

/// Maximum number of distinct `data-*` names the process-wide interner will hold.
const DATA_ATTRIBUTE_NAME_LIMIT: usize = 4096;

/// Maximum number of distinct custom element names the process-wide interner will hold. Each one
/// also leaks a one-element template, so the bound is smaller than the attribute one.
const CUSTOM_ELEMENT_NAME_LIMIT: usize = 512;

/// Longest custom element name admitted, in bytes. With [`CUSTOM_ELEMENT_NAME_LIMIT`] it bounds
/// the host memory a guest can pin through element names.
pub(crate) const MAX_CUSTOM_ELEMENT_NAME_BYTES: usize = 64;

/// The process-wide interner behind [`static_attribute`]'s open `data-*` names.
pub(crate) static DATA_ATTRIBUTES: NameInterner<()> = NameInterner::new(DATA_ATTRIBUTE_NAME_LIMIT);

/// The process-wide interner behind open custom element names; the value kept with each name is
/// the leaked template that renders it.
pub(crate) static CUSTOM_ELEMENTS: NameInterner<Template> =
    NameInterner::new(CUSTOM_ELEMENT_NAME_LIMIT);

/// The interner already holds its limit of distinct names and the requested one is new.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NamesExhausted;

/// Dioxus attribute and tag names are `&'static str`, so each distinct guest-chosen name is
/// leaked once and deduplicated here, together with whatever `V` the renderer derives from it
/// once. The distinct-name bound is what keeps a guest from growing host memory without limit by
/// minting names; a repeated name costs nothing.
pub(crate) struct NameInterner<V> {
    limit: usize,
    names: std::sync::Mutex<std::collections::BTreeMap<&'static str, V>>,
}

impl<V: Copy> NameInterner<V> {
    pub(crate) const fn new(limit: usize) -> Self {
        Self {
            limit,
            names: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        }
    }

    /// `name` must already be well formed for the vocabulary this interner serves. `make` runs
    /// only the first time a name is interned and receives the leaked name.
    pub(crate) fn intern(
        &self,
        name: &str,
        make: impl FnOnce(&'static str) -> V,
    ) -> Result<(&'static str, V), NamesExhausted> {
        let mut names = self.names.lock().expect("interner lock is never poisoned");
        if let Some((&interned, &value)) = names.get_key_value(name) {
            return Ok((interned, value));
        }
        if names.len() >= self.limit {
            return Err(NamesExhausted);
        }
        let interned: &'static str = Box::leak(name.to_owned().into_boxed_str());
        let value = make(interned);
        names.insert(interned, value);
        Ok((interned, value))
    }
}

/// `data-` followed by one or more of `[a-z0-9-._]`: the HTML custom data attribute form,
/// without namespace colons, uppercase, or whitespace.
pub(crate) fn is_data_attribute_name(name: &str) -> bool {
    name.strip_prefix("data-").is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix
                .bytes()
                .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_'))
    })
}

/// Resolves a wire attribute to the names Dioxus needs. `Ok(None)` is an unsupported name;
/// `Err` means a well-formed new `data-*` name would exceed the interner's bound.
pub(crate) fn static_attribute(
    data_names: &NameInterner<()>,
    name: &str,
    namespace: Option<&str>,
) -> Result<Option<(&'static str, Option<&'static str>)>, NamesExhausted> {
    macro_rules! vocabulary {
        ($($ns:pat => $canonical:expr => [$($name:literal),* $(,)?]),* $(,)?) => {
            match (name, namespace) {
                $($(($name, $ns) => Some(($name, $canonical)),)*)*
                _ => None,
            }
        };
    }
    if namespace.is_none() && is_data_attribute_name(name) {
        return data_names
            .intern(name, |_| ())
            .map(|(name, ())| Some((name, None)));
    }
    Ok(vocabulary!(
        None => None => [
            "id", "class", "title", "role", "tabindex", "hidden", "lang", "dir",
            "contenteditable", "draggable", "spellcheck", "translate",
            "value", "checked", "disabled", "readonly", "required", "multiple", "selected",
            "name", "type", "placeholder", "autocomplete", "inputmode", "pattern",
            "min", "max", "step", "minlength", "maxlength", "size", "rows", "cols", "wrap",
            "for", "novalidate", "accept", "href", "rel", "alt", "width", "height", "loading",
            "open", "datetime", "start",
            "reversed", "colspan", "rowspan", "scope", "headers", "label", "low", "high",
            "optimum", "media",
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
            "overflow", "overflow-x", "overflow-y", "overflow-anchor", "color", "background",
            "background-color",
            "border", "border-color", "border-width", "border-style", "border-radius",
            "font", "font-family", "font-size", "font-weight", "font-style", "line-height",
            "text-align", "text-decoration", "text-overflow", "white-space", "word-break",
            "cursor", "pointer-events", "touch-action", "user-select", "opacity", "visibility", "box-sizing",
            "box-shadow", "transform", "transform-origin", "transition", "object-fit"
        ],
        Some("http://www.w3.org/1999/xlink") => Some("http://www.w3.org/1999/xlink") => ["href"]
    ))
}

const SVG_NAMESPACE: &str = "http://www.w3.org/2000/svg";
const XLINK_NAMESPACE: &str = "http://www.w3.org/1999/xlink";

/// The value half of the attribute vocabulary: [`static_attribute`] decides which names a guest
/// may set, this decides which values those names may carry. Every value that can make the host
/// request, navigate to, or autofill from something the guest names is refused here, so a batch
/// carrying one faults the presentation exactly as an unknown name does. Names that exist only to
/// carry such values (`src`, `srcset`, `action`, `target`, `download`, raw `style`, ...) are not in
/// the vocabulary at all.
///
/// A removal (`None`, `false`) is always admitted. The checks are single byte-scans over the
/// value with a fixed token list; nothing here allocates.
pub(crate) fn attribute_value_policy(
    tag: &str,
    element_namespace: Option<&str>,
    name: &str,
    namespace: Option<&str>,
    value: &AttributeValue,
) -> Result<(), &'static str> {
    let text = match value {
        AttributeValue::None | AttributeValue::Boolean(false) => return Ok(()),
        AttributeValue::Text(text) => text.as_str(),
        AttributeValue::Float(_) | AttributeValue::Int(_) | AttributeValue::Boolean(true) => {
            // Numbers and booleans cannot carry a URL, but Dioxus stringifies them into
            // attributes whose meaning is a scheme or an autofill token.
            return match (name, namespace) {
                ("href" | "autocomplete", None | Some(XLINK_NAMESPACE)) => {
                    Err("non-text value for a restricted attribute")
                }
                _ => Ok(()),
            };
        }
    };
    match (name, namespace) {
        // An SVG `<use>` instantiates the element its `href` names, and an external reference
        // makes the renderer fetch a document, so it takes only a same-document fragment (`#id`).
        // `xlink:href` is the legacy spelling of the same attribute and takes the same rule.
        ("href", None | Some(XLINK_NAMESPACE))
            if tag == "use" && element_namespace == Some(SVG_NAMESPACE) =>
        {
            if text.len() < 2 || !text.starts_with('#') {
                return Err("svg use href must be a same-document fragment");
            }
        }
        // `xlink:href` exists for `<use>` only; on any other element it would be an unchecked
        // second spelling of `href`.
        ("href", Some(XLINK_NAMESPACE)) => return Err("xlink:href is only for svg use"),
        // Links carry an absolute http, https or mailto target and nothing else. The prefix test
        // runs at byte 0 (browsers strip leading C0 controls and spaces before reading the
        // scheme) and control characters anywhere are refused (tab and newline are stripped from
        // inside a scheme, so `java\tscript:` would otherwise pass for a different scheme).
        ("href", None) => {
            let scheme_ok = ["http://", "https://", "mailto:"].iter().any(|scheme| {
                text.as_bytes()
                    .get(..scheme.len())
                    .is_some_and(|head| head.eq_ignore_ascii_case(scheme.as_bytes()))
            });
            if !scheme_ok || text.bytes().any(|byte| byte.is_ascii_control()) {
                return Err("href must be an http, https or mailto url");
            }
        }
        // Any autofill token other than `off` invites a password manager to fill a guest field.
        ("autocomplete", None) => {
            if text != "off" {
                return Err("autocomplete must be off");
            }
        }
        // SVG paint and clip references accept `url()`, which fetches.
        ("fill" | "stroke" | "clip-path", None) => {
            if css_value_is_forbidden(text, &["url("]) {
                return Err("svg paint value carries a url");
            }
        }
        (_, Some("style")) => {
            if css_value_is_forbidden(text, CSS_FETCHING_TOKENS) {
                return Err("style value can fetch or escape its box");
            }
            // Containment traps absolute and fixed boxes on the hosts that support it; the
            // value is also refused so a host without containment support is not the only
            // line of defence.
            if name == "position" && css_value_is_forbidden(text, &["fixed", "sticky"]) {
                return Err("style position escapes its box");
            }
        }
        _ => {}
    }
    Ok(())
}

/// Functions and at-rules that make a style declaration fetch (`url()` and the image functions
/// that wrap it), read another attribute (`attr()`), or run script in legacy engines.
const CSS_FETCHING_TOKENS: &[&str] = &[
    "url(",
    "image-set(",
    "image(",
    "src(",
    "element(",
    "cross-fade(",
    "paint(",
    "attr(",
    "expression",
    "@import",
];

/// One pass over `value` looking for any of `tokens` ASCII-case-insensitively. Backslashes (CSS
/// escapes can spell `url(` without the bytes), comments, and control characters are forbidden
/// too, because each can hide a token from this byte scan while the CSS parser still sees it.
fn css_value_is_forbidden(value: &str, tokens: &[&str]) -> bool {
    let bytes = value.as_bytes();
    bytes.iter().enumerate().any(|(index, &byte)| {
        byte == b'\\'
            || byte.is_ascii_control()
            || (byte == b'/' && bytes.get(index + 1) == Some(&b'*'))
            || tokens.iter().any(|token| {
                bytes[index..]
                    .get(..token.len())
                    .is_some_and(|head| head.eq_ignore_ascii_case(token.as_bytes()))
            })
    })
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

        /// A listener for every bubbling event name that calls `stop_propagation`. Spread on the
        /// host element wrapping a child presentation, it ends bubbling at that boundary, so no
        /// listener of an ancestor presentation receives an event that began inside the child.
        /// `mounted` is excluded: it is per-element and never bubbles.
        pub fn presentation_barrier() -> Vec<dioxus_core::Attribute> {
            let mut barrier = Vec::new();
            $($(
                expand_surface_event_listener!(@barrier $group, barrier, $name);
            )*)*
            barrier
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
                                    let platform_rc = evt.data();
                                    let data_json = expand_surface_event_listener!(
                                        @serialize $group, $data, platform_rc.as_ref(), element, &handles
                                    );
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
    (@barrier Mounted, $barrier:ident, $name:ident) => {};
    (@barrier $group:ident, $barrier:ident, $name:ident) => {
        $barrier.push(dioxus_core::Attribute::new(
            stringify!($name),
            dioxus_core::AttributeValue::Listener(
                ListenerCallback::new(|evt: dioxus_core::Event<PlatformEventData>| {
                    evt.stop_propagation();
                })
                .erase(),
            ),
            None,
            false,
        ));
    };
    // A mounted element's host handle is what element commands act on; the guest is told only
    // that the element exists (its payload is `null`) and builds its own `MountedData` that
    // sends commands back over the surface.
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

/// The valid autonomous custom element names of the HTML Standard, narrowed to ASCII: a lowercase
/// ASCII letter, then `[a-z0-9._-]`, containing a hyphen, at most
/// [`MAX_CUSTOM_ELEMENT_NAME_BYTES`] long, and not one of the names the standard reserves for
/// SVG and MathML elements (`annotation-xml`, `color-profile`, `font-face`, `font-face-*`,
/// `missing-glyph`). The standard's non-ASCII "PCEN" characters are refused. The name is only a
/// tag the host renders; with no script on the host there is no definition for it to upgrade to.
pub(crate) fn is_custom_element_name(name: &str) -> bool {
    const RESERVED: &[&str] = &[
        "annotation-xml",
        "color-profile",
        "font-face",
        "missing-glyph",
    ];
    name.len() <= MAX_CUSTOM_ELEMENT_NAME_BYTES
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && name.contains('-')
        && name
            .bytes()
            .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_'))
        && !RESERVED.contains(&name)
        && !name.starts_with("font-face-")
}

/// Interns `name` as a custom element tag, leaking its template on first use. `name` must satisfy
/// [`is_custom_element_name`].
pub(crate) fn intern_custom_element(
    interner: &NameInterner<Template>,
    name: &str,
) -> Result<&'static str, NamesExhausted> {
    interner
        .intern(name, |tag| {
            // The shape every static tag's template has: the element with one slot for the spread
            // attributes and one for its children.
            let mut builder = RuntimeTemplateBuilder::default();
            builder.open_element(tag, None);
            builder.dynamic_attr();
            builder.dynamic_node(false);
            builder.close_element();
            builder.finish()
        })
        .map(|(tag, _)| tag)
}

fn render_custom_element(
    tag: &str,
    key: u64,
    attrs: Vec<dioxus_core::Attribute>,
    children: impl Iterator<Item = Element>,
) -> Element {
    let template = CUSTOM_ELEMENTS
        .names
        .lock()
        .expect("interner lock is never poisoned")
        .get(tag)
        .copied()
        .expect("validated custom element name is interned");
    Ok(VNode::new(
        template,
        dioxus_core::DynamicValues::from_parts(
            Some(key.to_string()),
            Box::new([dioxus_core::DynamicNode::make_node(children)]),
            Box::new([attrs.into_boxed_slice()]),
        ),
    ))
}

macro_rules! tag_name {
    ($tag:ident) => {
        stringify!($tag)
    };
    ($tag:ident, $name:literal) => {
        $name
    };
}

macro_rules! surface_tags {
    ($($namespace:pat => [$($tag:ident $(=> $name:literal)?),* $(,)?]),* $(,)?) => {
        pub(crate) fn supported_tag(tag: &str, namespace: Option<&str>) -> bool {
            match (tag, namespace) {
                $($((tag_name!($tag $(, $name)?), $namespace) => true,)*)*
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
                $($((tag_name!($tag $(, $name)?), $namespace) => rsx! {
                    $tag {
                        key: "{key}",
                        ..attrs,
                        {children}
                    }
                },)*)*
                (custom, None) if is_custom_element_name(custom) => {
                    render_custom_element(custom, key, attrs, children)
                }
                _ => unreachable!("validated surface element"),
            }
        }
    };
}

// `style` is admitted with its text sanitized at ingress (see `SurfaceTree::sanitize_style_text`);
// `use` takes only same-document `href`s (see `validate_use_href`). `slot` and `bdi` carry no
// behaviour without a shadow-root host and a script. None of these fetches or runs anything.
surface_tags!(
    None => [
        div, section, article, aside, header, footer, main, nav, h1, h2, h3, h4, h5, h6, p, span,
        pre, code, blockquote, strong, em, small, b, i, u, s, mark, kbd, sub, sup, br, hr, ul, ol,
        li, dl, dt, dd, table, thead, tbody, tfoot, tr, th, td, caption, form, label, input,
        textarea, button, select, option, optgroup, fieldset, legend, details, summary, dialog, a,
        img, figure, figcaption, progress, meter, output, time, data, style, slot, bdi
    ],
    Some("http://www.w3.org/2000/svg") => [
        svg, g, path, circle, rect, line, polyline, polygon, defs, symbol, r#use => "use"
    ]
);

#[cfg(test)]
mod tests;
