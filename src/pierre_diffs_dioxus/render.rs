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

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::LazyLock;

use dioxus::prelude::*;
use pierre_diffs::document::Document;
use pierre_diffs::document::NodeKey;
use pierre_diffs::utils::dom::css_wrappers::wrap_core_css;

use crate::events::Dispatch;
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
    /// renders with that listener. A static view has none.
    pub(crate) dispatch: Option<&'a Rc<Dispatch>>,
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
        let tag = document.tag_name(key);
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
        if let Some(dispatch) = self.dispatch {
            attributes.extend(dispatch.listeners(key));
        }
        let children: Vec<Element> = if let Some(shadow_root) = document.shadow_root(key) {
            attributes.push(Attribute::new("data-dioxus-shadow", "", None, false));
            // Dioxus rejects a literal key, hence the interpolation.
            let stylesheet_key = "pierre-core-css";
            let stylesheet = rsx! {
                style { key: "{stylesheet_key}", {CORE_CSS.as_str()} }
            };
            std::iter::once(stylesheet)
                .chain(
                    document
                        .child_nodes(shadow_root)
                        .into_iter()
                        .map(|child| self.node(child, Some(key))),
                )
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
