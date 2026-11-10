// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/Virtualizer.ts
// (the DOM-facing half: observer construction, listener registration and the
// reads of `scrollTop`, `getBoundingClientRect`, `scrollTo`, ...).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the class body is `pierre_diffs::components::virtualizer`; this file
// is `WebHost`, its `VirtualizerHost` over web-sys. `new ResizeObserver(cb)`,
// `new IntersectionObserver(cb, init)` and the `addEventListener` calls happen
// here and their callbacks forward to the `Virtualizer::handle_*` methods.
// Elements are `DomElement`s: a JavaScript object has no `Eq`/`Hash`, so each
// element carries an id stored in an expando property the first time it is
// seen and equality is that id. The `IntersectionObserver` init is built as a
// plain object because `root` may be a `Document`, which the typed web-sys
// setter cannot express. A non-HTMLElement observer target panics (upstream
// throws).

use std::cell::Cell;
use std::cell::RefCell;
use std::hash::Hash;
use std::hash::Hasher;
use std::rc::Rc;
use std::rc::Weak;

use js_sys::Array;
use js_sys::Function;
use js_sys::Object;
use js_sys::Reflect;
use pierre_diffs::components::virtualizer::AnchorLine;
use pierre_diffs::components::virtualizer::IntersectionEntry;
use pierre_diffs::components::virtualizer::IntersectionObserverInit;
use pierre_diffs::components::virtualizer::ResizeEntry;
use pierre_diffs::components::virtualizer::Root;
use pierre_diffs::components::virtualizer::ScrollBehavior;
use pierre_diffs::components::virtualizer::Virtualizer;
use pierre_diffs::components::virtualizer::VirtualizerHost;
use pierre_diffs::managers::universal_rendering_manager::AnimationFrames;
use pierre_diffs::managers::universal_rendering_manager::FrameId;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen::closure::Closure;
use web_sys::AddEventListenerOptions;
use web_sys::HtmlElement;
use web_sys::IntersectionObserver;
use web_sys::IntersectionObserverEntry;
use web_sys::ResizeObserver;
use web_sys::ResizeObserverEntry;
use web_sys::ScrollToOptions;

const ID_PROPERTY: &str = "__pierreDiffsElementId";

thread_local! {
    static NEXT_ELEMENT_ID: Cell<u32> = const { Cell::new(0) };
}

/// An `HTMLElement` with the identity the virtualizer's maps need.
#[derive(Clone)]
pub struct DomElement {
    id: u32,
    element: HtmlElement,
}

impl DomElement {
    pub fn new(element: HtmlElement) -> Self {
        let key = JsValue::from_str(ID_PROPERTY);
        let id = match Reflect::get(&element, &key)
            .expect("reading an expando property")
            .as_f64()
        {
            // ids are written below as `u32`
            Some(id) => id as u32,
            None => {
                let id = NEXT_ELEMENT_ID.with(|next| next.replace(next.get() + 1));
                Reflect::set(&element, &key, &JsValue::from_f64(f64::from(id)))
                    .expect("writing an expando property");
                id
            }
        };
        Self { id, element }
    }

    pub fn element(&self) -> &HtmlElement {
        &self.element
    }
}

impl PartialEq for DomElement {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for DomElement {}

impl Hash for DomElement {
    fn hash<State: Hasher>(&self, state: &mut State) {
        self.id.hash(state);
    }
}

struct Observers {
    resize: ResizeObserver,
    intersection: IntersectionObserver,
    // Kept alive for as long as the observers can call them.
    _resize_callback: Closure<dyn FnMut(Array)>,
    _intersection_callback: Closure<dyn FnMut(Array)>,
}

#[derive(Default)]
struct Listeners {
    window_scroll: Option<Closure<dyn FnMut()>>,
    window_resize: Option<Closure<dyn FnMut()>>,
    element_scroll: Option<(HtmlElement, Closure<dyn FnMut()>)>,
}

pub struct WebHost {
    window: web_sys::Window,
    document: web_sys::Document,
    virtualizer: RefCell<Weak<Virtualizer<WebHost>>>,
    observers: RefCell<Option<Observers>>,
    listeners: RefCell<Listeners>,
}

impl WebHost {
    pub fn new() -> Rc<Self> {
        let window = web_sys::window().expect("a browser window");
        let document = window.document().expect("a document");
        Rc::new(Self {
            window,
            document,
            virtualizer: RefCell::new(Weak::new()),
            observers: RefCell::new(None),
            listeners: RefCell::new(Listeners::default()),
        })
    }

    /// Points the event callbacks at the virtualizer built over this host.
    pub fn bind(&self, virtualizer: Weak<Virtualizer<WebHost>>) {
        *self.virtualizer.borrow_mut() = virtualizer;
    }

    fn forwarding(&self, handle: fn(&Virtualizer<WebHost>)) -> Closure<dyn FnMut()> {
        let virtualizer = self.virtualizer.borrow().clone();
        Closure::new(move || {
            if let Some(virtualizer) = virtualizer.upgrade() {
                handle(&virtualizer);
            }
        })
    }

    fn passive() -> AddEventListenerOptions {
        let options = AddEventListenerOptions::new();
        options.set_passive(true);
        options
    }

    fn html_element(target: web_sys::Element) -> DomElement {
        DomElement::new(
            target
                .dyn_into::<HtmlElement>()
                .expect("Virtualizer: target not an HTMLElement"),
        )
    }

    fn scroll_to_options(top: f64, behavior: ScrollBehavior) -> ScrollToOptions {
        let options = ScrollToOptions::new();
        options.set_top(top);
        options.set_behavior(match behavior {
            ScrollBehavior::Auto => web_sys::ScrollBehavior::Auto,
            ScrollBehavior::Instant => web_sys::ScrollBehavior::Instant,
            ScrollBehavior::Smooth => web_sys::ScrollBehavior::Smooth,
        });
        options
    }
}

impl AnimationFrames for WebHost {
    fn request_animation_frame(&self, callback: Box<dyn FnOnce(f64)>) -> FrameId {
        let callback = Closure::once_into_js(callback);
        let frame_id = self
            .window
            .request_animation_frame(callback.unchecked_ref())
            .expect("requestAnimationFrame");
        // Ids are positive 32-bit integers.
        frame_id as FrameId
    }

    fn cancel_animation_frame(&self, frame_id: FrameId) {
        self.window
            .cancel_animation_frame(frame_id as i32)
            .expect("cancelAnimationFrame");
    }
}

impl VirtualizerHost for WebHost {
    type Element = DomElement;

    fn window_scroll_y(&self) -> f64 {
        self.window.scroll_y().expect("window.scrollY")
    }

    fn window_inner_height(&self) -> f64 {
        self.window
            .inner_height()
            .expect("window.innerHeight")
            .as_f64()
            .expect("innerHeight is a number")
    }

    fn document_scroll_height(&self) -> f64 {
        f64::from(
            self.document
                .document_element()
                .expect("documentElement")
                .scroll_height(),
        )
    }

    fn element_scroll_top(&self, element: &DomElement) -> f64 {
        f64::from(element.element.scroll_top())
    }

    fn element_scroll_height(&self, element: &DomElement) -> f64 {
        f64::from(element.element.scroll_height())
    }

    fn element_bounding_height(&self, element: &DomElement) -> f64 {
        element.element.get_bounding_client_rect().height()
    }

    fn element_bounding_top(&self, element: &DomElement) -> f64 {
        element.element.get_bounding_client_rect().top()
    }

    fn element_offset_height(&self, element: &DomElement) -> f64 {
        f64::from(element.element.offset_height())
    }

    fn first_element_child(&self, element: &DomElement) -> Option<DomElement> {
        element
            .element
            .first_element_child()
            .and_then(|child| child.dyn_into::<HtmlElement>().ok())
            .map(DomElement::new)
    }

    fn window_scroll_to(&self, top: f64, behavior: ScrollBehavior) {
        self.window
            .scroll_to_with_scroll_to_options(&Self::scroll_to_options(top, behavior));
    }

    fn element_scroll_to(&self, element: &DomElement, top: f64, behavior: ScrollBehavior) {
        element
            .element
            .scroll_to_with_scroll_to_options(&Self::scroll_to_options(top, behavior));
    }

    fn anchor_lines(
        &self,
        file_element: &DomElement,
    ) -> impl Iterator<Item = AnchorLine<DomElement>> {
        let lines = file_element.element.shadow_root().map(|shadow_root| {
            shadow_root
                .query_selector_all("[data-line][data-line-index]")
                .expect("a constant selector is valid")
        });
        let length = lines.as_ref().map_or(0, web_sys::NodeList::length);
        (0..length).filter_map(move |index| {
            let element = lines.as_ref()?.get(index)?.dyn_into::<HtmlElement>().ok()?;
            let line_index = element.get_attribute("data-line-index")?;
            Some(AnchorLine {
                line_index,
                element: DomElement::new(element),
            })
        })
    }

    fn find_anchor_line(&self, file_element: &DomElement, line_index: &str) -> Option<DomElement> {
        file_element
            .element
            .shadow_root()?
            .query_selector(&format!("[data-line][data-line-index=\"{line_index}\"]"))
            .expect("the selector is valid")?
            .dyn_into::<HtmlElement>()
            .ok()
            .map(DomElement::new)
    }

    fn create_observers(&self, intersection: IntersectionObserverInit<DomElement>) {
        let resize_virtualizer = self.virtualizer.borrow().clone();
        let resize_callback = Closure::<dyn FnMut(Array)>::new(move |entries: Array| {
            let Some(virtualizer) = resize_virtualizer.upgrade() else {
                return;
            };
            let entries: Vec<ResizeEntry<DomElement>> = entries
                .iter()
                .map(|entry| {
                    let entry: ResizeObserverEntry = entry.unchecked_into();
                    let block_size = entry
                        .border_box_size()
                        .get(0)
                        .unchecked_into::<web_sys::ResizeObserverSize>()
                        .block_size();
                    ResizeEntry {
                        target: WebHost::html_element(entry.target()),
                        block_size,
                    }
                })
                .collect();
            virtualizer.handle_container_resize(&entries);
        });
        let resize = ResizeObserver::new(resize_callback.as_ref().unchecked_ref())
            .expect("new ResizeObserver");

        let intersection_virtualizer = self.virtualizer.borrow().clone();
        let intersection_callback = Closure::<dyn FnMut(Array)>::new(move |entries: Array| {
            let Some(virtualizer) = intersection_virtualizer.upgrade() else {
                return;
            };
            let entries: Vec<IntersectionEntry<DomElement>> = entries
                .iter()
                .map(|entry| {
                    let entry: IntersectionObserverEntry = entry.unchecked_into();
                    IntersectionEntry {
                        target: WebHost::html_element(entry.target()),
                        is_intersecting: entry.is_intersecting(),
                    }
                })
                .collect();
            virtualizer.handle_intersection_change(&entries);
        });
        let init = Object::new();
        let root: JsValue = match &intersection.root {
            Root::Document => self.document.clone().into(),
            Root::Element(element) => element.element.clone().into(),
        };
        Reflect::set(&init, &"root".into(), &root).expect("init.root");
        Reflect::set(
            &init,
            &"threshold".into(),
            &intersection
                .threshold
                .iter()
                .map(|threshold| JsValue::from_f64(*threshold))
                .collect::<Array>(),
        )
        .expect("init.threshold");
        let margin = intersection.root_margin;
        Reflect::set(
            &init,
            &"rootMargin".into(),
            &JsValue::from_str(&format!("{margin}px 0px {margin}px 0px")),
        )
        .expect("init.rootMargin");
        let intersection_observer = IntersectionObserver::new_with_options(
            intersection_callback.as_ref().unchecked_ref(),
            init.unchecked_ref(),
        )
        .expect("new IntersectionObserver");

        *self.observers.borrow_mut() = Some(Observers {
            resize,
            intersection: intersection_observer,
            _resize_callback: resize_callback,
            _intersection_callback: intersection_callback,
        });
    }

    fn disconnect_observers(&self) {
        if let Some(observers) = self.observers.borrow_mut().take() {
            observers.resize.disconnect();
            observers.intersection.disconnect();
        }
    }

    fn observe_resize(&self, element: &DomElement) {
        if let Some(observers) = &*self.observers.borrow() {
            observers.resize.observe(&element.element);
        }
    }

    fn observe_document_resize(&self) {
        if let Some(observers) = &*self.observers.borrow() {
            observers
                .resize
                .observe(&self.document.document_element().expect("documentElement"));
        }
    }

    fn observe_intersection(&self, element: &DomElement) {
        if let Some(observers) = &*self.observers.borrow() {
            observers.intersection.observe(&element.element);
        }
    }

    fn unobserve_intersection(&self, element: &DomElement) {
        if let Some(observers) = &*self.observers.borrow() {
            observers.intersection.unobserve(&element.element);
        }
    }

    fn add_window_listeners(&self) {
        let scroll = self.forwarding(Virtualizer::handle_window_scroll);
        let resize = self.forwarding(Virtualizer::handle_window_resize);
        let options = Self::passive();
        self.window
            .add_event_listener_with_callback_and_add_event_listener_options(
                "scroll",
                scroll.as_ref().unchecked_ref::<Function>(),
                &options,
            )
            .expect("window scroll listener");
        self.window
            .add_event_listener_with_callback_and_add_event_listener_options(
                "resize",
                resize.as_ref().unchecked_ref::<Function>(),
                &options,
            )
            .expect("window resize listener");
        let mut listeners = self.listeners.borrow_mut();
        listeners.window_scroll = Some(scroll);
        listeners.window_resize = Some(resize);
    }

    fn add_element_scroll_listener(&self, root: &DomElement) {
        let scroll = self.forwarding(Virtualizer::handle_element_scroll);
        root.element
            .add_event_listener_with_callback_and_add_event_listener_options(
                "scroll",
                scroll.as_ref().unchecked_ref::<Function>(),
                &Self::passive(),
            )
            .expect("element scroll listener");
        self.listeners.borrow_mut().element_scroll = Some((root.element.clone(), scroll));
    }

    fn remove_listeners(&self, _root: Option<&Root<DomElement>>) {
        let mut listeners = self.listeners.borrow_mut();
        if let Some((element, scroll)) = listeners.element_scroll.take() {
            element
                .remove_event_listener_with_callback("scroll", scroll.as_ref().unchecked_ref())
                .expect("remove element scroll listener");
        }
        if let Some(scroll) = listeners.window_scroll.take() {
            self.window
                .remove_event_listener_with_callback("scroll", scroll.as_ref().unchecked_ref())
                .expect("remove window scroll listener");
        }
        if let Some(resize) = listeners.window_resize.take() {
            self.window
                .remove_event_listener_with_callback("resize", resize.as_ref().unchecked_ref())
                .expect("remove window resize listener");
        }
    }

    fn console_log(&self, message: &str, virtualizer_id: &str, size_change: f64, new_size: f64) {
        let payload = Object::new();
        Reflect::set(
            &payload,
            &"sizeChange".into(),
            &JsValue::from_f64(size_change),
        )
        .expect("payload.sizeChange");
        Reflect::set(&payload, &"newSize".into(), &JsValue::from_f64(new_size))
            .expect("payload.newSize");
        web_sys::console::log_3(
            &JsValue::from_str(message),
            &JsValue::from_str(virtualizer_id),
            &payload,
        );
    }
}
