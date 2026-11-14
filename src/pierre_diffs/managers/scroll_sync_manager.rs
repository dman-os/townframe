// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/managers/ScrollSyncManager.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the DOM reads, listeners and timers go through `ScrollSyncHost` (listeners are registered per `ScrollSide` and call back the public `handle_deletions_scroll` / `handle_additions_scroll`; `setTimeout`/`clearTimeout` take a `Box<dyn FnOnce()>` and an `i64` id with upstream's `-1` as "none"). `pre.children ?? []` and `element instanceof HTMLElement` are the host handing over HTML children only; `'deletions' in element.dataset` is `dataset_has`. The scroll handlers are methods because the host owns the event wiring; the class is built as an `Rc` so the timeout callbacks can hold a weak reference, as the arrow functions hold `this`.

use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;
use std::rc::Weak;

use crate::document::Document;
use crate::document::NodeKey;

pub type TimeoutId = i64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScrollSide {
    Deletions,
    Additions,
}

pub trait ScrollSyncHost: 'static {
    fn document(&self) -> &Document;
    /// `element.addEventListener('scroll', handler, { passive: true })`
    fn add_scroll_listener(&self, element: NodeKey, side: ScrollSide);
    fn remove_scroll_listener(&self, element: NodeKey, side: ScrollSide);
    fn scroll_left(&self, element: NodeKey) -> f64;
    /// `element.scrollTo({ left })`; `left` is `undefined` when absent.
    fn scroll_to_left(&self, element: NodeKey, left: Option<f64>);
    fn set_timeout(&self, callback: Box<dyn FnOnce()>, milliseconds: u32) -> TimeoutId;
    fn clear_timeout(&self, timeout_id: TimeoutId);
}

pub struct ScrollSyncManager<Host: ScrollSyncHost> {
    host: Rc<Host>,
    weak_self: Weak<Self>,
    is_deletions_scrolling: Cell<bool>,
    is_additions_scrolling: Cell<bool>,
    timeout_id: Cell<TimeoutId>,
    code_deletions: RefCell<Option<NodeKey>>,
    code_additions: RefCell<Option<NodeKey>>,
    enabled: Cell<bool>,
}

impl<Host: ScrollSyncHost> ScrollSyncManager<Host> {
    pub fn new(host: Rc<Host>) -> Rc<Self> {
        Rc::new_cyclic(|weak_self| Self {
            host,
            weak_self: Weak::clone(weak_self),
            is_deletions_scrolling: Cell::new(false),
            is_additions_scrolling: Cell::new(false),
            timeout_id: Cell::new(-1),
            code_deletions: RefCell::new(None),
            code_additions: RefCell::new(None),
            enabled: Cell::new(false),
        })
    }

    pub fn is_deletions_scrolling(&self) -> bool {
        self.is_deletions_scrolling.get()
    }

    pub fn is_additions_scrolling(&self) -> bool {
        self.is_additions_scrolling.get()
    }

    pub fn clean_up(&self) {
        if !self.enabled.get() {
            return;
        }
        if let Some(code_deletions) = &*self.code_deletions.borrow() {
            self.host
                .remove_scroll_listener(*code_deletions, ScrollSide::Deletions);
        }
        if let Some(code_additions) = &*self.code_additions.borrow() {
            self.host
                .remove_scroll_listener(*code_additions, ScrollSide::Additions);
        }
        self.host.clear_timeout(self.timeout_id.get());
        *self.code_deletions.borrow_mut() = None;
        *self.code_additions.borrow_mut() = None;
        self.enabled.set(false);
    }

    pub fn setup(
        &self,
        pre: &NodeKey,
        code_deletions: Option<NodeKey>,
        code_additions: Option<NodeKey>,
    ) {
        let (mut code_deletions, mut code_additions) = (code_deletions, code_additions);
        // If no code elements were provided, lets try to find them in
        // the pre element
        if code_deletions.is_none() || code_additions.is_none() {
            for element in self.host.document().children(pre) {
                if self
                    .host
                    .document()
                    .get_attribute(element, &format!("data-{}", "deletions"))
                    .is_some()
                {
                    code_deletions = Some(element);
                } else if self
                    .host
                    .document()
                    .get_attribute(element, &format!("data-{}", "additions"))
                    .is_some()
                {
                    code_additions = Some(element);
                }
            }
        }
        let (Some(code_deletions), Some(code_additions)) = (code_deletions, code_additions) else {
            self.clean_up();
            return;
        };

        if self.code_deletions.borrow().as_ref() != Some(&code_deletions) {
            if let Some(previous) = &*self.code_deletions.borrow() {
                self.host
                    .remove_scroll_listener(*previous, ScrollSide::Deletions);
            }
            *self.code_deletions.borrow_mut() = Some(code_deletions);
            self.host
                .add_scroll_listener(code_deletions, ScrollSide::Deletions);
        }
        if self.code_additions.borrow().as_ref() != Some(&code_additions) {
            if let Some(previous) = &*self.code_additions.borrow() {
                self.host
                    .remove_scroll_listener(*previous, ScrollSide::Additions);
            }
            *self.code_additions.borrow_mut() = Some(code_additions);
            self.host
                .add_scroll_listener(code_additions, ScrollSide::Additions);
        }
        self.enabled.set(true);
    }

    pub fn handle_deletions_scroll(&self) {
        if self.is_additions_scrolling.get() {
            return;
        }
        self.is_deletions_scrolling.set(true);
        self.host.clear_timeout(self.timeout_id.get());
        let manager = Weak::clone(&self.weak_self);
        self.timeout_id.set(self.host.set_timeout(
            Box::new(move || {
                if let Some(manager) = manager.upgrade() {
                    manager.is_deletions_scrolling.set(false);
                }
            }),
            300,
        ));
        if let Some(code_additions) = &*self.code_additions.borrow() {
            let left = self
                .code_deletions
                .borrow()
                .as_ref()
                .map(|code_deletions| self.host.scroll_left(*code_deletions));
            self.host.scroll_to_left(*code_additions, left);
        }
    }

    pub fn handle_additions_scroll(&self) {
        if self.is_deletions_scrolling.get() {
            return;
        }
        self.is_additions_scrolling.set(true);
        self.host.clear_timeout(self.timeout_id.get());
        let manager = Weak::clone(&self.weak_self);
        self.timeout_id.set(self.host.set_timeout(
            Box::new(move || {
                if let Some(manager) = manager.upgrade() {
                    manager.is_additions_scrolling.set(false);
                }
            }),
            300,
        ));
        if let Some(code_deletions) = &*self.code_deletions.borrow() {
            let left = self
                .code_additions
                .borrow()
                .as_ref()
                .map(|code_additions| self.host.scroll_left(*code_additions));
            self.host.scroll_to_left(*code_deletions, left);
        }
    }
}
