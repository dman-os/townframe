//! The code view half of the host: what `CodeView` asks of its platform beyond a virtualized
//! diff's reads (`CodeViewHost`), over the caches and tasks the rest of `DioxusHost` already
//! has. It is a child of `host.rs` because it reads the host's private state.
//!
//! * The viewer's own `ResizeObserver` and the root's five listeners are the renderer's, as
//!   everywhere in this crate: registering a node records it (`shell_observed`,
//!   `shell_listeners`), and the renderer attaches `onresize`, or the scroll, wheel, touch,
//!   pointer and key handlers, to exactly that node (`events.rs`).
//! * The slab's three per-frame writes (the container's height, the spacer's height, the slab's
//!   `top` and `bottom`) are not `Document` edits: they set one `Signal<SlabStyle>` that the
//!   three wrapper elements of the view render their `style` from (`render.rs`, `SlabNode`), so a
//!   scrolled frame patches three attributes and rebuilds no shell. The port reads those style
//!   properties back nowhere, which is what allows the override.
//! * Focus. Standard Dioxus cannot ask which element has focus, so each shell reports its own
//!   `focusin`/`focusout` (`events.rs`) and `active_element` answers the shell that holds
//!   focus: the viewer asks "is focus inside this shell" (`renderedItemOwnsFocus`), which that
//!   answers exactly. `focus_root` is `MountedData::set_focus`, which has no `preventScroll`.
//! * `random`: the slab's jitter only needs to look random, and std has no generator (and no
//!   entropy a wasm guest can reach), so it is an xorshift sequence.
//! * `now` and the sleeps are the embedder's (`Runtime`): std's `Instant` does not exist on
//!   `wasm32-unknown-unknown`.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::time::Duration;

use dioxus::prelude::*;
use pierre_diffs::components::code_view::CodeViewHost;
use pierre_diffs::components::code_view::SlabNodes;
use pierre_diffs::document::Document;
use pierre_diffs::document::NodeKey;
use pierre_diffs::utils::dom::platform::PlatformInfo;

use crate::code_view::ScrollMode;
use crate::scroll_root::ScrollRoot;

use super::DioxusHost;
use super::Scroller;
use super::SleepFuture;

/// What only the platform knows and standard Dioxus cannot ask: the embedder passes it in and
/// it is read fresh on every call, so a monitor switch or a zoom is seen.
#[derive(Clone, PartialEq)]
pub struct Environment {
    /// `window.devicePixelRatio`: scroll positions are rounded to a device pixel.
    pub device_pixel_ratio: f64,
    /// `prefers-reduced-motion: reduce`: a smooth scroll jumps instead.
    pub prefers_reduced_motion: bool,
    /// The user agent and platform strings, which choose the scroll rebase tuning (Firefox) and
    /// the mobile Safari workarounds.
    pub platform: PlatformInfo,
}

impl Default for Environment {
    fn default() -> Self {
        Self {
            device_pixel_ratio: 1.0,
            prefers_reduced_motion: false,
            platform: PlatformInfo::default(),
        }
    }
}

/// The three nodes of the slab's scaffolding, which the view renders as wrapper elements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SlabRole {
    /// Holds the spacer and the slab; its height is the whole scroll range.
    Container,
    /// Pushes the slab down to where the rendered window starts.
    Spacer,
    /// The sticky slab the shells render in.
    Slab,
}

/// What the three wrappers' `style` attributes take from the host, besides what the `Document`
/// holds for them. `None` is a property the viewer has not written (or removed).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct SlabStyle {
    pub(crate) container_height: Option<f64>,
    pub(crate) spacer_height: Option<f64>,
    pub(crate) top: Option<f64>,
    pub(crate) bottom: Option<f64>,
    /// Bumped when a `Document` edit lands on one of the three nodes (the pointer-events and
    /// overflow writes of a scroll burst), so the wrappers read their `Document` attributes
    /// again without the shell list being rebuilt.
    pub(crate) revision: u64,
}

impl SlabStyle {
    /// The declarations of `role`'s node that the signal owns, to follow the `Document`'s.
    pub(crate) fn declarations(&self, role: SlabRole) -> String {
        let mut css = String::new();
        let mut write = |name: &str, value: Option<f64>| {
            if let Some(value) = value {
                // `${value}px`, with JavaScript's rendering of negative zero.
                let value = if value == 0.0 { 0.0 } else { value };
                css.push_str(&format!("{name}:{value}px;"));
            }
        };
        match role {
            SlabRole::Container => write("height", self.container_height),
            SlabRole::Spacer => write("height", self.spacer_height),
            SlabRole::Slab => {
                write("top", self.top);
                write("bottom", self.bottom);
            }
        }
        css
    }
}

pub(crate) struct CodeViewHostState {
    pub(crate) slab: Signal<SlabStyle>,
    nodes: Cell<Option<SlabNodes>>,
    environment: RefCell<Environment>,
    pub(super) now: Rc<dyn Fn() -> f64>,
    random: Cell<u64>,
    /// The nodes the viewer's resize observer watches: the slab, the root, the header and footer
    /// hosts.
    shell_observed: RefCell<HashSet<NodeKey>>,
    /// The root, while it carries the viewer's scroll, wheel, touch, pointer and key listeners.
    shell_listeners: Cell<Option<NodeKey>>,
    /// The shell that holds focus (see the module docs).
    focused: Cell<Option<NodeKey>>,
    /// The embedder's scroller, when the viewer lays out in it instead of a scroller of its own.
    page: Option<ScrollRoot>,
}

impl CodeViewHostState {
    /// Tells the host which nodes are the slab's scaffolding, once the viewer has built them.
    pub(crate) fn set_slab_nodes(&self, nodes: SlabNodes) {
        self.nodes.set(Some(nodes));
    }

    pub(crate) fn set_environment(&self, environment: Environment) {
        *self.environment.borrow_mut() = environment;
    }

    /// Whether `node` is one of the three wrappers, and which.
    pub(crate) fn slab_role(&self, node: NodeKey) -> Option<SlabRole> {
        let nodes = self.nodes.get()?;
        [
            (nodes.container, SlabRole::Container),
            (nodes.spacer, SlabRole::Spacer),
            (nodes.slab, SlabRole::Slab),
        ]
        .into_iter()
        .find_map(|(wrapper, role)| (wrapper == node).then_some(role))
    }

    /// The embedder's scroller, in `ScrollMode::Embedder`.
    pub(crate) fn page(&self) -> Option<&ScrollRoot> {
        self.page.as_ref()
    }

    /// Whether `page` is the scroller this viewer was mounted with.
    pub(crate) fn is_page(&self, page: &ScrollRoot) -> bool {
        self.page.as_ref() == Some(page)
    }

    /// Whether the viewer's resize observer watches `node`.
    pub(crate) fn observes_shell(&self, node: NodeKey) -> bool {
        self.shell_observed.borrow().contains(&node)
    }

    /// A shell: a child of the slab, the node an item renders into.
    pub(crate) fn is_shell(&self, document: &Document, node: NodeKey) -> bool {
        self.nodes
            .get()
            .is_some_and(|nodes| document.parent(node) == Some(nodes.slab))
    }

    /// Whether `node` is the root carrying the viewer's listeners.
    pub(crate) fn wants_shell_listeners(&self, node: NodeKey) -> bool {
        self.shell_listeners.get() == Some(node)
    }

    /// A shell took or lost focus.
    pub(crate) fn record_focus(&self, shell: NodeKey, focused: bool) {
        if focused {
            self.focused.set(Some(shell));
        } else if self.focused.get() == Some(shell) {
            self.focused.set(None);
        }
    }

    /// A `Document` edit landed on a wrapper: the wrappers read their attributes again.
    pub(crate) fn touch_slab(&self) {
        let mut slab = self.slab;
        slab.write().revision += 1;
    }

    /// Sets the slab signal's fields through `edit`, notifying the wrappers only when one
    /// changed.
    fn write_slab(&self, edit: impl FnOnce(&mut SlabStyle)) {
        let mut slab = self.slab;
        let current = *slab.peek();
        let mut next = current;
        edit(&mut next);
        if next != current {
            slab.set(next);
        }
    }

    pub(super) fn prune(&self, document: &Document) {
        self.shell_observed
            .borrow_mut()
            .retain(|node| document.exists(*node));
        if self
            .focused
            .get()
            .is_some_and(|node| !document.exists(node))
        {
            self.focused.set(None);
        }
    }
}

impl DioxusHost {
    /// A code view host: `root` is what the viewer sets up (the viewer builds
    /// `container > [spacer, slab]` in it). In `ScrollMode::Own` it is a scroll container of the
    /// given CSS height. In `ScrollMode::Embedder` it is a plain block in the embedder's scroller:
    /// its geometry is the scroller's (see `scroll_root.rs`), so it is no scroll container and
    /// no tab stop, and the root's own `onscroll` and `onmounted` are not the viewer's.
    pub(crate) fn new_code_view(
        scroll: &ScrollMode,
        sleep: Rc<dyn Fn(Duration) -> SleepFuture>,
        now: Rc<dyn Fn() -> f64>,
        environment: Environment,
        slab: Signal<SlabStyle>,
        line_height: f64,
    ) -> Rc<Self> {
        let document = Document::new();
        let root = document.create_element("div");
        match scroll {
            ScrollMode::Own { height } => {
                document.set_attribute(
                    root,
                    "style",
                    &format!("overflow-y:auto;height:{height};"),
                );
                // A scroll container the keyboard can reach (the viewer would make it a
                // script-only tab stop): without one its arrow and page keys do nothing.
                document.set_attribute(root, "tabindex", "0");
            }
            // A root that scrolls (`overflow-y:auto`) also clips its inline axis; this one does
            // not scroll, so wide rows would widen the embedder's scroller, the document
            // included. `clip` crops at the root's edge without making a scroll container, which
            // the sticky slab would otherwise stick to instead of the embedder's scroller. The
            // rows scroll inside their own columns (`[data-code]` is `overflow: scroll clip`).
            ScrollMode::Embedder(_) => {
                document.set_attribute(root, "style", "overflow-x:clip;");
            }
        }
        Self::build(
            document,
            None,
            root,
            Some(Scroller {
                root,
                content: None,
                sleep,
            }),
            line_height,
            Some(CodeViewHostState {
                slab,
                nodes: Cell::new(None),
                environment: RefCell::new(environment),
                now,
                random: Cell::new(0x9E37_79B9_7F4A_7C15),
                shell_observed: RefCell::default(),
                shell_listeners: Cell::new(None),
                focused: Cell::new(None),
                page: match scroll {
                    ScrollMode::Own { .. } => None,
                    ScrollMode::Embedder(page) => Some(page.clone()),
                },
            }),
        )
    }

    /// What a code view host keeps beyond a diff host's state; `None` on a diff host.
    pub(crate) fn code_view(&self) -> Option<&CodeViewHostState> {
        self.code_view.as_ref()
    }

    fn code(&self) -> &CodeViewHostState {
        self.code_view
            .as_ref()
            .expect("only a code view host answers the viewer's reads")
    }

    /// The root the viewer scrolls.
    pub(crate) fn root(&self) -> NodeKey {
        self.scroller_root()
    }

    /// Whether `node` is the viewer's root in the embedder's scroller: it is not what scrolls,
    /// so the scroll and size events of the node are not the viewer's (the handle forwards the
    /// scroller's).
    pub(crate) fn is_page_root(&self, node: NodeKey) -> bool {
        self.code_view
            .as_ref()
            .is_some_and(|code_view| code_view.page().is_some())
            && node == self.scroller_root()
    }

    /// How far the viewer's top is below the top of the scroller's content: scroll writes
    /// addressed to the viewer's root go to the scroller shifted by it. 0 in an own scroller.
    pub(crate) fn page_offset(&self) -> f64 {
        self.code_view
            .as_ref()
            .and_then(|code_view| code_view.page())
            .map_or(0.0, ScrollRoot::offset)
    }
}

impl CodeViewHost for DioxusHost {
    fn device_pixel_ratio(&self) -> f64 {
        self.code().environment.borrow().device_pixel_ratio
    }

    fn prefers_reduced_motion(&self) -> bool {
        self.code().environment.borrow().prefers_reduced_motion
    }

    fn platform_info(&self) -> PlatformInfo {
        self.code().environment.borrow().platform.clone()
    }

    fn random(&self) -> f64 {
        let code_view = self.code();
        let mut state = code_view.random.get();
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        code_view.random.set(state);
        let scrambled = state.wrapping_mul(0x2545_F491_4F6C_DD1D);
        // The top 53 bits make a double in [0, 1).
        (scrambled >> 11) as f64 / (1_u64 << 53) as f64
    }

    fn now(&self) -> f64 {
        (self.code().now)()
    }

    fn create_shell_observer(&self) {}

    fn shell_observe(&self, element: &NodeKey) {
        self.code().shell_observed.borrow_mut().insert(*element);
    }

    fn shell_unobserve(&self, element: &NodeKey) {
        self.code().shell_observed.borrow_mut().remove(element);
    }

    fn shell_disconnect(&self) {
        self.code().shell_observed.borrow_mut().clear();
    }

    /// The root's `scroll` is the scroll registration of the scroller (no side), as the
    /// virtualizer's is; the other four are the renderer's (`wants_shell_listeners`).
    fn add_shell_listeners(&self, root: &NodeKey) {
        self.code().shell_listeners.set(Some(*root));
        self.registrations.borrow_mut().scroll.insert(*root, None);
    }

    fn remove_shell_listeners(&self, root: &NodeKey) {
        self.code().shell_listeners.set(None);
        self.registrations.borrow_mut().scroll.remove(root);
    }

    fn active_element(&self) -> Option<NodeKey> {
        self.code().focused.get()
    }

    /// Focus inside a shell is already `active_element` (a shell's `focusin` reports focus in its
    /// shadow tree too).
    fn shadow_active_element(&self, _element: &NodeKey) -> Option<NodeKey> {
        None
    }

    fn focus_root(&self, root: &NodeKey) {
        let mounted = self.mounted_of(*root);
        spawn(async move {
            mounted
                .set_focus(true)
                .await
                .expect("the platform focuses a mounted element");
        });
    }

    fn write_slab_spacer(&self, _spacer: NodeKey, height: Option<f64>) {
        self.code().write_slab(|style| style.spacer_height = height);
    }

    fn write_slab_sticky(&self, _slab: NodeKey, top: f64, bottom: f64) {
        self.code().write_slab(|style| {
            style.top = Some(top);
            style.bottom = Some(bottom);
        });
    }

    fn write_container_height(&self, _container: NodeKey, height: f64) {
        self.code()
            .write_slab(|style| style.container_height = Some(height));
    }
}
