//! The renderer: a `pierre_diffs::document::Document` subtree to Dioxus elements.
//!
//! One pass over the model, no virtual tree of its own. Every node becomes an element keyed by
//! its `NodeKey` (Dioxus then diffs children by identity, so a window of rows that shifts moves
//! nodes instead of rewriting them). Three things in the model are not plain elements:
//!
//! * A shadow host renders with `data-dioxus-shadow`. Dioxus's patched interpreter attaches a
//!   real shadow root to such an element and parents its children there; on an unpatched
//!   interpreter the attribute is inert and the same children stay in the light DOM. Either way
//!   the first child is Pierre's stylesheet, because the port leaves the core CSS to the
//!   embedder (upstream's `diffs-container` adopts it).
//! * The host's light children (annotation wrappers, header slots) are not rendered where the
//!   model keeps them. A `<slot name="x">` in the shadow tree renders the host's children whose
//!   `slot` attribute is `x`, as fallback content of the slot element, which shows them whether
//!   or not the slot is live. The model never holds Dioxus elements.
//! * A `SlotContent` placeholder (see `slot.rs`) renders as the caller's Dioxus `Element`.
//!
//! A code view adds a fourth: its container, spacer and slab render as `SlabNode` components, not
//! as plain elements. The slab's per-frame writes are not `Document` edits but a `Signal` of the
//! host (`host/code_view.rs`), and a component is the unit Dioxus re-runs when a signal it reads
//! changes: a node's `style` is patched by that component alone, while its `children` (the
//! shells) are the very element the parent render made, which Dioxus finds unchanged and skips.

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::LazyLock;

use dioxus::prelude::*;
use pierre_diffs::document::Document;
use pierre_diffs::document::NodeKey;
use pierre_diffs::js_collections::PtrEq;
use pierre_diffs::utils::dom::css_wrappers::wrap_core_css;

use crate::events::Dispatch;
use crate::events::Pointers;
use crate::host::DioxusHost;
use crate::host::SlabRole;
use crate::host::SlabStyle;
use crate::slot::SlotTable;

/// The core stylesheet in the layer order the port's theme CSS expects, built once.
static CORE_CSS: LazyLock<String> = LazyLock::new(|| wrap_core_css(""));

thread_local! {
    /// Attribute names, leaked once each: `Attribute::name` is `&'static str`, and the set is
    /// the renderer's vocabulary (Pierre's `data-*` names, a few ARIA names), not user data.
    static ATTRIBUTE_NAMES: RefCell<HashSet<&'static str>> = RefCell::new(HashSet::new());
}

fn attribute_name(name: &str) -> &'static str {
    ATTRIBUTE_NAMES.with_borrow_mut(|names| match names.get(name) {
        Some(interned) => interned,
        None => {
            let interned: &'static str = Box::leak(name.to_owned().into_boxed_str());
            names.insert(interned);
            interned
        }
    })
}

pub(crate) struct Renderer<'a> {
    pub(crate) document: &'a Document,
    pub(crate) slots: &'a SlotTable,
    /// The view's event wiring: a node the host registered for resize, visibility or scroll
    /// renders with that listener.
    pub(crate) dispatch: &'a Rc<Dispatch>,
    /// The pointer handlers of the nodes the interaction manager listens through; a view with
    /// no interaction has none.
    pub(crate) pointers: Option<&'a Rc<Pointers>>,
    /// A code view's slab signal, which its three wrapper nodes render their `style` from.
    pub(crate) slab: Option<Signal<SlabStyle>>,
}

impl Renderer<'_> {
    /// `node` is the shadow host Pierre renders into (the `diffs-container`).
    pub(crate) fn render(&self, node: NodeKey) -> Element {
        self.node(node, None)
    }

    // `host` is the shadow host whose shadow tree `key` sits in: where a `<slot>` finds the
    // light children to show.
    fn node(&self, key: NodeKey, host: Option<NodeKey>) -> Element {
        if let Some(content) = self.slots.content(key) {
            return content;
        }
        let document = self.document;
        let key_id = key.as_u64();
        if let Some(style) = self.slab
            && let Some(role) = self
                .dispatch
                .host
                .code_view()
                .and_then(|code_view| code_view.slab_role(key))
        {
            let children: Vec<Element> = document
                .child_nodes(key)
                .into_iter()
                .map(|child| self.node(child, host))
                .collect();
            return rsx! {
                SlabNode {
                    key: "{key_id}",
                    host: PtrEq::new(Rc::clone(&self.dispatch.host)),
                    dispatch: PtrEq::new(Rc::clone(self.dispatch)),
                    node: key,
                    role,
                    style,
                    children: rsx! { {children.into_iter()} },
                }
            };
        }
        let tag = document.tag_name(key);
        // Inside a shadow tree a `<style>` is already in its root's sheet attribute.
        if host.is_some() && tag.eq_ignore_ascii_case("style") {
            // Keyed like every sibling, or the keyed diff of the children refuses the list.
            return rsx! {
                Fragment { key: "{key_id}" }
            };
        }
        if tag.is_empty() {
            let text = document.text_content(key);
            return rsx! {
                Fragment { key: "{key_id}", "{text}" }
            };
        }
        let tag = tag.to_ascii_lowercase();
        let mut attributes: Vec<Attribute> = document
            .attributes(key)
            .into_iter()
            .map(|(name, value)| Attribute::new(attribute_name(&name), value, None, false))
            .collect();
        attributes.extend(self.dispatch.listeners(key));
        if let Some(pointers) = self.pointers {
            attributes.extend(pointers.listeners(key));
        }
        let children: Vec<Element> = if let Some(shadow_root) = document.shadow_root(key) {
            attributes.push(Attribute::new("data-dioxus-shadow", "", None, false));
            // The shadow root's stylesheet (Pierre's core CSS, then the text of every `<style>`
            // Pierre put anywhere in this shadow tree, such as its theme) rides on the host as an
            // attribute, which the interpreter adopts as a constructable sheet; those `<style>`
            // elements are not rendered (see `node`). A `<style>` element would be refused by a
            // CSP without `'unsafe-inline'` in `style-src-elem` (the web shell's), while an
            // attribute's text is data. A style's position in the tree does not change what it
            // styles: it applies to the whole shadow tree either way.
            let mut sheet = CORE_CSS.to_string();
            let mut pending = document.child_nodes(shadow_root);
            while let Some(child) = pending.pop() {
                // Pierre's document names HTML elements in upper case (`STYLE`).
                if document.tag_name(child).eq_ignore_ascii_case("style") {
                    sheet.push('\n');
                    sheet.push_str(&document.text_content(child));
                } else if document.shadow_root(child).is_none() {
                    // A nested shadow host's tree is its own: its styles go on its own sheet.
                    pending.extend(document.child_nodes(child));
                }
            }
            attributes.push(Attribute::new(
                "data-dioxus-shadow-sheet",
                sheet,
                None,
                false,
            ));
            let content = document.child_nodes(shadow_root);
            content
                .into_iter()
                .map(|child| self.node(child, Some(key)))
                .collect()
        } else if tag == "slot" {
            let name = document.get_attribute(key, "name").unwrap_or_default();
            host.into_iter()
                .flat_map(|host| document.child_nodes(host))
                .filter(|child| {
                    !document.tag_name(child).is_empty()
                        && document.get_attribute(child, "slot").unwrap_or_default() == name
                })
                .map(|child| self.node(child, host))
                .collect()
        } else {
            document
                .child_nodes(key)
                .into_iter()
                .map(|child| self.node(child, host))
                .collect()
        };
        element(&tag, key, attributes, children)
    }
}

// Dioxus has no element whose tag is a runtime value, so the tags Pierre emits are listed.
// A tag outside the list is a port change this renderer has not caught up with.
fn element(tag: &str, key: NodeKey, attributes: Vec<Attribute>, children: Vec<Element>) -> Element {
    let key_id = key.as_u64();
    macro_rules! tags {
        ($($name:literal => $element:ident),* $(,)?) => {
            match tag {
                $($name => rsx! {
                    $element { key: "{key_id}", ..attributes, {children.into_iter()} }
                },)*
                other => panic!("pierre_diffs_dioxus cannot render <{other}>: add it to `tags!`"),
            }
        };
    }
    if tag == "diffs-container" {
        // A custom element: no `dioxus_elements` ident carries a hyphen, but rsx accepts it
        // spelled out.
        return rsx! {
            diffs-container { key: "{key_id}", ..attributes, {children.into_iter()} }
        };
    }
    tags! {
        "div" => div,
        "span" => span,
        "bdi" => bdi,
        "header" => header,
        "small" => small,
        "p" => p,
        "b" => b,
        "a" => a,
        "pre" => pre,
        "code" => code,
        "style" => style,
        "slot" => slot,
        "button" => button,
        "svg" => svg,
        "symbol" => symbol,
        "path" => path,
        "use" => r#use,
    }
}

#[derive(Props, Clone, PartialEq)]
struct SlabNodeProps {
    host: PtrEq<DioxusHost>,
    dispatch: PtrEq<Dispatch>,
    node: NodeKey,
    role: SlabRole,
    style: Signal<SlabStyle>,
    children: Element,
}

/// One of a code view's container, spacer and slab: the node's attributes from the `Document`,
/// with the declarations the host's slab signal owns appended to its `style`. This component is
/// the only reader of the signal, so a scrolled frame re-runs these three and nothing else.
#[component]
fn SlabNode(props: SlabNodeProps) -> Element {
    let slab: SlabStyle = *props.style.read();
    let document = props.host.0.document();
    let mut attributes: Vec<Attribute> = Vec::new();
    let mut style = String::new();
    for (name, value) in document.attributes(props.node) {
        if name == "style" {
            style = value;
        } else {
            attributes.push(Attribute::new(attribute_name(&name), value, None, false));
        }
    }
    // Later declarations win, and the `Document` holds none of the signal's properties.
    style.push_str(&slab.declarations(props.role));
    attributes.push(Attribute::new("style", style, None, false));
    attributes.extend(props.dispatch.0.listeners(props.node));
    rsx! {
        div { ..attributes, {props.children.clone()} }
    }
}
