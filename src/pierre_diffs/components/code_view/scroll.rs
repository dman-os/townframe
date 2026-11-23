// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/CodeView.ts
// (C3: the scroll model. `ScrollRebaseConfig` and its two constants, 651-698;
// the paged scroll math `getMaxScrollTopForHeight` ... `getPagedLayoutTop`,
// 2977-3112; the scroll targets `getStickyHeaderOffset` ... `getRangeScrollPosition`,
// 3113-3372; the spring `computeTargetScrollTopForFrame`, `computeSpringStep`,
// `advanceScrollAnimation`, 3373-3465; the anchors `getScrollAnchorViewportTop`,
// `getScrollAnchor`, `resolveAnchoredScrollTop`, `isPendingTargetSettled`,
// 4035-4202, 4252-4258; the cached reads `getScrollTop`, `getHeight`,
// 4260-4290, and `capturePendingLayoutAnchor`, 1878).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes:
// * `root.scrollTop` and `root.getBoundingClientRect().height` are the
//   `VirtualizerHost` reads `element_scroll_top` and `element_bounding_height`
//   of `State::root`; `isFirefox()` at field initialisation is
//   `is_firefox(&host.platform_info())` at construction.
// * A line target's undefined `side` is `additions`, the default parameter of
//   `getLinePosition` (the Rust method takes the side by value).
// * `console.warn` for an unknown item or an unresolvable line or range
//   returns `None` without the log (dropped.psv).
// * `resolveAlignedScrollPosition`'s five positional arguments are
//   `AlignedScrollPosition` (upstream's own `REVIEW` asks for a named
//   interface object), and `nearest` is excluded from the pending align by
//   `PendingAlign`, so `normalizeScrollTarget` converts the target's
//   `ScrollAlign` (`pending_align`).
// * `getScrollAnchor` clones an item's `Rc` out of the state before it calls
//   into the instance (`getNumericScrollAnchor` may call back into the viewer),
//   and `availableItems.get(id) === item` is `Rc::ptr_eq`.

use std::collections::HashMap;
use std::rc::Rc;

use super::CodeView;
use super::CodeViewHost;
use super::state::ItemAnchor;
use super::state::ItemRef;
use super::state::LineAnchor;
use super::state::PendingAlign;
use super::state::PendingItemTarget;
use super::state::PendingLineTarget;
use super::state::PendingRangeTarget;
use super::state::PendingScrollTarget;
use super::state::ScrollAnchor;
use super::state::ScrollToAnimation;
use crate::components::file_diff::RenderedContent;
use crate::components::virtualized_file_diff::window::LinePosition;
use crate::types::view::AnnotationSide;
use crate::types::view::CodeViewRangeScrollTarget;
use crate::types::view::CodeViewScrollBehavior;
use crate::types::view::CodeViewScrollTarget;
use crate::types::view::ScrollAlign;
use crate::utils::round_to_device_pixel::round_to_device_pixel;

/// `ScrollRebaseConfig`. Browsers cap how tall an element can be, so very
/// large content scrolls inside a fixed-height container instead. When the
/// scroll position gets close to either end of that container, the content
/// shifts and the scroll position jumps back toward the middle, so there is
/// always room to keep scrolling.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScrollRebaseConfig {
    /// Height of the scroll container, in CSS pixels.
    pub container_height: f64,
    /// Scrolling up past this point jumps back down to `target_bottom`.
    pub trigger_top: f64,
    /// Where a jump lands after scrolling down past `threshold`.
    pub target_top: f64,
    /// Where a jump lands after scrolling up past `trigger_top`.
    pub target_bottom: f64,
    /// Scrolling down past this point jumps back up to `target_top`.
    pub threshold: f64,
}

impl ScrollRebaseConfig {
    /// `createScrollRebaseConfig`
    const fn new(container_height: f64, trigger_top: f64, target_top: f64) -> Self {
        Self {
            container_height,
            trigger_top,
            target_top,
            target_bottom: container_height - target_top,
            threshold: container_height - trigger_top,
        }
    }
}

/// `DEFAULT_SCROLL_REBASE`
pub const DEFAULT_SCROLL_REBASE: ScrollRebaseConfig =
    ScrollRebaseConfig::new(12_000_000.0, 1_000_000.0, 2_000_000.0);

/// `FIREFOX_SCROLL_REBASE`. Firefox loses precision on very large positions:
/// past about 16.7 million device pixels, some letters render a pixel lower
/// than their neighbors. A smaller container keeps positions under that limit,
/// even on high-density or zoomed-in screens.
pub const FIREFOX_SCROLL_REBASE: ScrollRebaseConfig =
    ScrollRebaseConfig::new(4_194_304.0, 500_000.0, 1_000_000.0);

/// `PagedScrollPosition`
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PagedScrollPosition {
    pub paged_scroll_top: f64,
    pub scroll_page_offset: f64,
}

/// `SpringStepResult`
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpringStepResult {
    pub position: f64,
    pub velocity: f64,
}

/// `resolveAlignedScrollPosition`'s arguments, named.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AlignedScrollPosition {
    /// Item-space (0 is the first item's top).
    pub target_top: f64,
    pub target_height: f64,
    /// `None` is `start`, the default.
    pub align: Option<PendingAlign>,
    pub offset: f64,
    pub sticky_offset: f64,
}

/// `{ top: number; height: number }` of `getScrollTargetRect`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScrollTargetRect {
    pub top: f64,
    pub height: f64,
}

/// `target.align` of a target that is not `nearest`.
fn pending_align(align: Option<ScrollAlign>) -> Option<PendingAlign> {
    match align {
        None => None,
        Some(ScrollAlign::Start) => Some(PendingAlign::Start),
        Some(ScrollAlign::Center) => Some(PendingAlign::Center),
        Some(ScrollAlign::End) => Some(PendingAlign::End),
        Some(ScrollAlign::Nearest) => unreachable!("nearest is resolved before it is pending"),
    }
}

impl<Host, Rendered, LAnnotation> CodeView<Host, Rendered, LAnnotation>
where
    Host: CodeViewHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    /// `roundToDevicePixel(value)`
    pub(crate) fn round_to_device_pixel(&self, value: f64) -> f64 {
        round_to_device_pixel(value, self.host.device_pixel_ratio())
    }

    /// `getScrollTop()`: the cached logical scroll position, read from the
    /// root (plus the page offset) only while it is dirty.
    pub fn get_scroll_top(&self) -> f64 {
        let root = {
            let mut state = self.state.borrow_mut();
            if !state.scroll_dirty {
                return state.scroll_top;
            }
            state.scroll_dirty = false;
            state.root
        };
        let root_scroll_top = root.map_or(0.0, |root| self.host.element_scroll_top(&root));
        // Not inline: the borrow would live through `clamp_scroll_top`, which
        // writes the cached height.
        let scroll_page_offset = self.state.borrow().scroll_page_offset;
        let scroll_top = self.clamp_scroll_top(root_scroll_top + scroll_page_offset);
        self.state.borrow_mut().scroll_top = scroll_top;
        scroll_top
    }

    /// `getHeight()`: the cached viewport height, read from the root only
    /// while it is dirty.
    pub fn get_height(&self) -> f64 {
        let root = {
            let mut state = self.state.borrow_mut();
            if !state.height_dirty {
                return state.height;
            }
            state.height_dirty = false;
            state.root
        };
        let height = root.map_or(0.0, |root| self.host.element_bounding_height(&root));
        self.state.borrow_mut().height = height;
        height
    }

    /// `getMaxScrollTopForHeight(scrollHeight)`: the header/footer hosts live
    /// in `root` outside `container`, so they add to the real scrollable range
    /// on top of the items + padding.
    pub(crate) fn get_max_scroll_top_for_height(&self, scroll_height: f64) -> f64 {
        let layout = self.get_layout();
        let (header, footer) = {
            let state = self.state.borrow();
            (state.header.height, state.footer.height)
        };
        (layout.padding_top + header + scroll_height + footer + layout.padding_bottom
            - self.get_height())
        .max(0.0)
    }

    /// `getMaxScrollTop()`
    pub(crate) fn get_max_scroll_top(&self) -> f64 {
        self.get_max_scroll_top_for_height(self.get_scroll_height())
    }

    /// `shouldRebaseScroll()`
    pub(crate) fn should_rebase_scroll(&self) -> bool {
        self.get_max_scroll_top() > self.state.borrow().scroll_rebase_config.threshold
    }

    /// `getPagedScrollHeight()`
    pub(crate) fn get_paged_scroll_height(&self) -> f64 {
        if self.should_rebase_scroll() {
            self.get_scroll_height()
                .min(self.state.borrow().scroll_rebase_config.container_height)
        } else {
            self.get_scroll_height()
        }
    }

    /// `getMaxPagedScrollTop()`
    pub(crate) fn get_max_paged_scroll_top(&self) -> f64 {
        self.get_max_scroll_top_for_height(self.get_paged_scroll_height())
    }

    /// `clampPagedScrollTop(value)`
    pub(crate) fn clamp_paged_scroll_top(&self, value: f64) -> f64 {
        value.min(self.get_max_paged_scroll_top()).max(0.0)
    }

    /// `clampScrollTop(value)`: clamps a logical scroll position to the
    /// min/max allowable scroll range based on the full computed content
    /// height.
    pub(crate) fn clamp_scroll_top(&self, value: f64) -> f64 {
        value.min(self.get_max_scroll_top()).max(0.0)
    }

    /// `getMaxScrollPageOffset()`
    pub(crate) fn get_max_scroll_page_offset(&self) -> f64 {
        (self.get_max_scroll_top() - self.get_max_paged_scroll_top()).max(0.0)
    }

    /// `clampScrollPageOffset(value)`
    pub(crate) fn clamp_scroll_page_offset(&self, value: f64) -> f64 {
        value.min(self.get_max_scroll_page_offset()).max(0.0)
    }

    /// `resolveScrollPageWindow(scrollTop, preferredPagedScrollTop)`
    pub(crate) fn resolve_scroll_page_window(
        &self,
        scroll_top: f64,
        preferred_paged_scroll_top: f64,
    ) -> PagedScrollPosition {
        let mut paged_scroll_top =
            self.round_to_device_pixel(self.clamp_paged_scroll_top(preferred_paged_scroll_top));
        let mut scroll_page_offset = self.clamp_scroll_page_offset(scroll_top - paged_scroll_top);

        paged_scroll_top = self
            .round_to_device_pixel(self.clamp_paged_scroll_top(scroll_top - scroll_page_offset));
        scroll_page_offset = self.clamp_scroll_page_offset(scroll_top - paged_scroll_top);
        PagedScrollPosition {
            paged_scroll_top,
            scroll_page_offset,
        }
    }

    /// `resolvePagedScrollPosition(logicalScrollTop)`: how a logical scrollTop
    /// maps onto the reusable paged scroll window without mutating the
    /// current page offset.
    pub(crate) fn resolve_paged_scroll_position(
        &self,
        logical_scroll_top: f64,
    ) -> PagedScrollPosition {
        if !self.should_rebase_scroll() {
            return PagedScrollPosition {
                paged_scroll_top: self.clamp_paged_scroll_top(logical_scroll_top),
                scroll_page_offset: 0.0,
            };
        }

        let (scroll_page_offset, config) = {
            let state = self.state.borrow();
            (state.scroll_page_offset, state.scroll_rebase_config)
        };
        let current_page_offset = self.clamp_scroll_page_offset(scroll_page_offset);

        let paged_scroll_top = logical_scroll_top - current_page_offset;
        let paged_max_scroll_top = self.get_max_paged_scroll_top();
        let max_rebase_offset = self.get_max_scroll_page_offset();
        let should_move_down =
            paged_scroll_top > config.threshold && current_page_offset < max_rebase_offset;
        let should_move_up = paged_scroll_top < config.trigger_top && current_page_offset > 0.0;

        if paged_scroll_top < 0.0
            || paged_scroll_top > paged_max_scroll_top
            || should_move_down
            || should_move_up
        {
            return self.resolve_scroll_page_window(
                logical_scroll_top,
                if should_move_up {
                    config.target_bottom.min(paged_max_scroll_top)
                } else {
                    config.target_top
                },
            );
        }

        PagedScrollPosition {
            paged_scroll_top: self
                .round_to_device_pixel(self.clamp_paged_scroll_top(paged_scroll_top)),
            scroll_page_offset: current_page_offset,
        }
    }

    /// `needsScrollPageUpdate(logicalScrollTop)`
    pub(crate) fn needs_scroll_page_update(&self, logical_scroll_top: f64) -> bool {
        let rounded_scroll_top =
            self.round_to_device_pixel(self.clamp_scroll_top(logical_scroll_top));
        self.resolve_paged_scroll_position(rounded_scroll_top)
            .scroll_page_offset
            != self.state.borrow().scroll_page_offset
    }

    /// `getPagedLayoutTop(logicalTop)`
    pub(crate) fn get_paged_layout_top(&self, logical_top: f64) -> f64 {
        if !self.should_rebase_scroll() {
            return logical_top;
        }
        (logical_top - self.state.borrow().scroll_page_offset).max(0.0)
    }

    /// `getStickyHeaderOffset()`
    pub(crate) fn get_sticky_header_offset(&self) -> f64 {
        let state = self.state.borrow();
        if state.options.sticky_headers == Some(true)
            && state.options.diff.base.base.disable_file_header != Some(true)
        {
            state.item_metrics_cache.diff_header_height
        } else {
            0.0
        }
    }

    /// `getLineScrollPosition(item, target)`
    fn get_line_scroll_position(
        item: &ItemRef<Host, Rendered, LAnnotation>,
        line_number: usize,
        side: Option<crate::types::view::SelectionSide>,
    ) -> Option<LinePosition> {
        let instance = Rc::clone(item.borrow().instance());
        instance.get_line_position(line_number, side.unwrap_or(AnnotationSide::Additions))
    }

    /// `getRangeScrollPosition(item, target)`
    fn get_range_scroll_position(
        item: &ItemRef<Host, Rendered, LAnnotation>,
        target: &CodeViewRangeScrollTarget,
    ) -> Option<LinePosition> {
        let range = &target.range;
        let start_position = Self::get_line_scroll_position(item, range.start, range.side)?;
        let end_position =
            Self::get_line_scroll_position(item, range.end, range.end_side.or(range.side))?;

        let start_top = start_position.top;
        let start_bottom = start_top + start_position.height;
        let end_top = end_position.top;
        let end_bottom = end_top + end_position.height;
        let top = start_top.min(end_top);
        let bottom = start_bottom.max(end_bottom);
        Some(LinePosition {
            top,
            height: bottom - top,
        })
    }

    /// `getScrollTargetRect(target)`: `None` for an unknown item or an
    /// unresolvable line or range (upstream warns).
    pub(crate) fn get_scroll_target_rect(
        &self,
        target: &CodeViewScrollTarget,
    ) -> Option<ScrollTargetRect> {
        let id = match target {
            CodeViewScrollTarget::Position(_) => {
                unreachable!("getScrollTargetRect takes an item, line or range target")
            }
            CodeViewScrollTarget::Line(target) => &target.id,
            CodeViewScrollTarget::Range(target) => &target.id,
            CodeViewScrollTarget::Item(target) => &target.id,
        };
        let item = self.item_by_id(id)?;
        let item_top = item.borrow().top;
        match target {
            CodeViewScrollTarget::Position(_) => unreachable!("handled above"),
            CodeViewScrollTarget::Item(_) => Some(ScrollTargetRect {
                top: item_top,
                height: item.borrow().height,
            }),
            CodeViewScrollTarget::Range(target) => {
                let position = Self::get_range_scroll_position(&item, target)?;
                Some(ScrollTargetRect {
                    top: item_top + position.top,
                    height: position.height,
                })
            }
            CodeViewScrollTarget::Line(target) => {
                let position =
                    Self::get_line_scroll_position(&item, target.line_number, target.side)?;
                Some(ScrollTargetRect {
                    top: item_top + position.top,
                    height: position.height,
                })
            }
        }
    }

    /// `{ ...target, align }` as a pending target.
    fn pending_target(
        target: &CodeViewScrollTarget,
        align: Option<PendingAlign>,
    ) -> PendingScrollTarget {
        match target {
            CodeViewScrollTarget::Position(target) => PendingScrollTarget::Position(target.clone()),
            CodeViewScrollTarget::Line(target) => PendingScrollTarget::Line(PendingLineTarget {
                target: target.clone(),
                align,
            }),
            CodeViewScrollTarget::Range(target) => PendingScrollTarget::Range(PendingRangeTarget {
                target: target.clone(),
                align,
            }),
            CodeViewScrollTarget::Item(target) => PendingScrollTarget::Item(PendingItemTarget {
                target: target.clone(),
                align,
            }),
        }
    }

    /// `normalizeScrollTarget(target)`: a `nearest` target becomes `start` or
    /// `end`, or `None` when it is already in view.
    pub(crate) fn normalize_scroll_target(
        &self,
        target: &CodeViewScrollTarget,
    ) -> Option<PendingScrollTarget> {
        let (align, offset) = match target {
            CodeViewScrollTarget::Position(_) => return Some(Self::pending_target(target, None)),
            CodeViewScrollTarget::Line(target) => (target.align, target.offset),
            CodeViewScrollTarget::Range(target) => (target.align, target.offset),
            CodeViewScrollTarget::Item(target) => (target.align, target.offset),
        };
        if align != Some(ScrollAlign::Nearest) {
            return Some(Self::pending_target(target, pending_align(align)));
        }

        let rect = self.get_scroll_target_rect(target)?;

        // Determine a stable scrollTo target for `nearest` alignment. This is
        // to ensure that we don't experience any scroll bouncing
        let offset = offset.unwrap_or(0.0);
        let target_top = self.get_item_top_offset() + rect.top;
        let target_bottom = target_top + rect.height;
        let current_top = self.get_scroll_top();
        let visible_top = current_top
            + if matches!(
                target,
                CodeViewScrollTarget::Line(_) | CodeViewScrollTarget::Range(_)
            ) {
                self.get_sticky_header_offset()
            } else {
                0.0
            };
        let visible_bottom = current_top + self.get_height();

        // If the item is spanning beyond the full viewport, do nothing as
        // it's already in view
        if target_top - offset <= visible_top && target_bottom + offset >= visible_bottom {
            return None;
        }

        // Let's use the top as the target
        if target_top - offset < visible_top {
            return Some(Self::pending_target(target, Some(PendingAlign::Start)));
        }

        // Let's use the end as the target
        if target_bottom + offset > visible_bottom {
            return Some(Self::pending_target(target, Some(PendingAlign::End)));
        }

        // The element is already in view, nothing to do.
        None
    }

    /// `resolveScrollTargetTop(target)`: a target's scroll position, `None`
    /// when no target can be resolved for whatever reason.
    pub(crate) fn resolve_scroll_target_top(&self, target: &PendingScrollTarget) -> Option<f64> {
        if let PendingScrollTarget::Position(target) = target {
            let clamped_position = self.clamp_scroll_top(target.position);
            return Some(if clamped_position != target.position {
                // If our position was clamped, we we shouldn't apply the sticky offset
                clamped_position
            } else {
                self.clamp_scroll_top(target.position - self.get_sticky_header_offset())
            });
        }

        let id = target.id().expect("a non-position target names an item");
        let item = self.item_by_id(id)?;
        let item_top = item.borrow().top;
        match target {
            PendingScrollTarget::Position(_) => unreachable!("handled above"),
            PendingScrollTarget::Item(target) => {
                let item_height = item.borrow().height;
                Some(self.clamp_scroll_top(self.resolve_aligned_scroll_position(
                    AlignedScrollPosition {
                        target_top: item_top,
                        target_height: item_height,
                        align: target.align,
                        offset: target.target.offset.unwrap_or(0.0),
                        sticky_offset: 0.0,
                    },
                )))
            }
            PendingScrollTarget::Range(target) => {
                let position = Self::get_range_scroll_position(&item, &target.target)?;
                Some(self.clamp_scroll_top(self.resolve_aligned_scroll_position(
                    AlignedScrollPosition {
                        target_top: item_top + position.top,
                        target_height: position.height,
                        align: target.align,
                        offset: target.target.offset.unwrap_or(0.0),
                        sticky_offset: self.get_sticky_header_offset(),
                    },
                )))
            }
            PendingScrollTarget::Line(target) => {
                let position = Self::get_line_scroll_position(
                    &item,
                    target.target.line_number,
                    target.target.side,
                )?;
                Some(self.clamp_scroll_top(self.resolve_aligned_scroll_position(
                    AlignedScrollPosition {
                        target_top: item_top + position.top,
                        target_height: position.height,
                        align: target.align,
                        offset: target.target.offset.unwrap_or(0.0),
                        sticky_offset: self.get_sticky_header_offset(),
                    },
                )))
            }
        }
    }

    /// `resolveAlignedScrollPosition(...)`: given an existing scroll target
    /// (top and height), the correct scroll position to target based on the
    /// desired alignment, offset and sticky offset.
    pub(crate) fn resolve_aligned_scroll_position(&self, position: AlignedScrollPosition) -> f64 {
        let AlignedScrollPosition {
            target_top,
            target_height,
            align,
            offset,
            sticky_offset,
        } = position;
        // targetTop is item-space (0 = first item's top); shift it into
        // absolute scroll coordinates. getItemTopOffset includes the header
        // height, so scrolling to an item/line lands correctly when a header
        // is present.
        let target_top = target_top + self.get_item_top_offset();
        let viewport_height = self.get_height();
        // If the item + offset is bigger than the viewport, we'll fall back to
        // 'start'
        if align == Some(PendingAlign::Center) && target_height + offset < viewport_height {
            return target_top - (viewport_height - target_height) / 2.0 + offset;
        }
        if align == Some(PendingAlign::End) {
            return target_top - (viewport_height - target_height) + offset;
        }
        // 'start', the default
        target_top - sticky_offset - offset
    }

    /// `resolveEffectiveScrollBehavior(target, destination)`
    pub(crate) fn resolve_effective_scroll_behavior(
        &self,
        target: &PendingScrollTarget,
        destination: f64,
    ) -> CodeViewScrollBehavior {
        let behavior = match target {
            PendingScrollTarget::Position(target) => target.behavior,
            PendingScrollTarget::Line(target) => target.target.behavior,
            PendingScrollTarget::Range(target) => target.target.behavior,
            PendingScrollTarget::Item(target) => target.target.behavior,
        };
        if self.host.prefers_reduced_motion() {
            return CodeViewScrollBehavior::Instant;
        }

        if behavior != Some(CodeViewScrollBehavior::SmoothAuto) {
            return behavior.unwrap_or(CodeViewScrollBehavior::Instant);
        }

        if (destination - self.get_scroll_top()).abs() <= self.get_height() * 10.0 {
            CodeViewScrollBehavior::Smooth
        } else {
            CodeViewScrollBehavior::Instant
        }
    }

    /// `computeTargetScrollTopForFrame(scrollTop, frameTimestamp)`: the target
    /// scroll position for the current frame. Without a pending target it is
    /// the current position; with one it is the destination, or, during a
    /// smooth scroll, the next spring position.
    pub(crate) fn compute_target_scroll_top_for_frame(
        &self,
        scroll_top: f64,
        frame_timestamp: f64,
    ) -> f64 {
        let (pending, animation) = {
            let state = self.state.borrow();
            (state.pending_scroll_target.clone(), state.scroll_animation)
        };
        let Some(pending) = pending else {
            return scroll_top;
        };
        let Some(destination) = self.resolve_scroll_target_top(&pending) else {
            return scroll_top;
        };
        match animation {
            None => destination,
            Some(animation) => {
                self.compute_spring_step(&animation, destination, frame_timestamp)
                    .position
            }
        }
    }

    /// `computeSpringStep(animation, destination, frameTimestamp)`: a
    /// closed-form critically damped ODE step. Stable at any dt (Euler would
    /// blow up once ω·dt ≳ 1), so this survives big frame gaps (tab-wake,
    /// offscreen frames) and resize-driven ticks that fire outside the normal
    /// frame cadence.
    pub(crate) fn compute_spring_step(
        &self,
        animation: &ScrollToAnimation,
        destination: f64,
        frame_timestamp: f64,
    ) -> SpringStepResult {
        let dt = (frame_timestamp - animation.last_timestamp).max(0.0);
        let omega = self
            .state
            .borrow()
            .options
            .smooth_scroll_settings_or_default()
            .omega;
        let decay = (-omega * dt).exp();
        let displacement = animation.position - destination;
        let spring_coeff = animation.velocity + omega * displacement;
        let position = destination + (displacement + spring_coeff * dt) * decay;
        let velocity = (spring_coeff * (1.0 - omega * dt) - omega * displacement) * decay;
        SpringStepResult { position, velocity }
    }

    /// `advanceScrollAnimation(frameTimestamp, anchorDelta)`: for the pending
    /// scroll target, updates any in flight smooth scroll animation and
    /// returns the target scrollTop to move towards. Resolves the animation
    /// based on frame time and adopts any necessary scroll anchoring
    /// corrections.
    pub(crate) fn advance_scroll_animation(
        &self,
        frame_timestamp: f64,
        anchor_delta: f64,
    ) -> Option<f64> {
        let pending = self.state.borrow().pending_scroll_target.clone()?;
        let Some(destination) = self.resolve_scroll_target_top(&pending) else {
            let mut state = self.state.borrow_mut();
            state.pending_scroll_target = None;
            state.scroll_animation = None;
            return None;
        };
        let animation = self.state.borrow().scroll_animation;
        let Some(mut animation) = animation else {
            return Some(destination);
        };

        animation.position += anchor_delta;

        let SpringStepResult { position, velocity } =
            self.compute_spring_step(&animation, destination, frame_timestamp);
        animation.last_timestamp = frame_timestamp;
        animation.position = position;
        animation.velocity = velocity;

        let settings = self
            .state
            .borrow()
            .options
            .smooth_scroll_settings_or_default();
        let mut state = self.state.borrow_mut();
        if (destination - position).abs() <= settings.position_epsilon
            && velocity.abs() <= settings.velocity_epsilon
        {
            state.scroll_animation = None;
            return Some(destination);
        }
        state.scroll_animation = Some(animation);
        Some(position)
    }

    /// `getScrollAnchorViewportTop(absoluteItemTop, scrollTop)`: the scrollTop
    /// accounting for the sticky header if enabled and necessary.
    pub(crate) fn get_scroll_anchor_viewport_top(
        &self,
        absolute_item_top: f64,
        scroll_top: f64,
    ) -> f64 {
        if absolute_item_top < scroll_top {
            scroll_top + self.get_sticky_header_offset()
        } else {
            scroll_top
        }
    }

    /// `getScrollAnchor(scrollTop, availableItems = this.idToItem)`: a scroll
    /// anchor based on the built-in metrics of the rendered files/diffs. A
    /// scroll anchor represents the first fully visible element (the first
    /// file or line whose top is fully in the viewport).
    pub(crate) fn get_scroll_anchor(
        &self,
        scroll_top: f64,
        available_items: Option<&HashMap<String, ItemRef<Host, Rendered, LAnnotation>>>,
    ) -> Option<ScrollAnchor> {
        let is_available = |id: &str, item: &ItemRef<Host, Rendered, LAnnotation>| {
            let found = match available_items {
                Some(available) => available.get(id).cloned(),
                None => self.item_by_id(id),
            };
            found.is_some_and(|found| Rc::ptr_eq(&found, item))
        };
        let mut skipped_item: Option<ItemRef<Host, Rendered, LAnnotation>> = None;

        let pending_layout_anchor = self.state.borrow().pending_layout_anchor.clone();
        if let Some(pending_layout_anchor) = pending_layout_anchor {
            let pending_item = self.item_by_id(pending_layout_anchor.id());
            if let Some(pending_item) = pending_item {
                if is_available(pending_layout_anchor.id(), &pending_item) {
                    return Some(pending_layout_anchor);
                }
                skipped_item = Some(pending_item);
            }
        }

        // We shouldn't scroll anchor when at the top, this way if a custom
        // header gets asynchronously added it won't be hidden when added. Also
        // like, logically it doesn't make sense to anchor at the top of the
        // document, you probably want to see stuff added at the top...
        if scroll_top <= 0.0 {
            return None;
        }

        let render_state = self.state.borrow().render_state;
        if render_state.first_index == -1 || render_state.last_index == -1 {
            return None;
        }

        let viewport_height = self.get_height();
        // If we have no previoius frame, we shouldn't scroll anchor
        if render_state.sticky_top == -1.0 || render_state.sticky_bottom == -1.0 {
            return None;
        }

        for index in render_state.first_index..=render_state.last_index {
            let item = self.state.borrow().items.get(index as usize).cloned();
            let Some(item) = item else {
                continue;
            };

            let (id, top, height) = {
                let item = item.borrow();
                (item.id().to_owned(), item.top, item.height)
            };
            let absolute_item_top = self.get_item_top_offset() + top;
            let absolute_item_bottom = absolute_item_top + height;
            // Skip items entirely above the viewport since we can't see it
            if absolute_item_bottom <= scroll_top {
                continue;
            }
            // If the item starts below the viewport bottom we are done searching.
            if absolute_item_top >= scroll_top + viewport_height {
                break;
            }

            if !is_available(&id, &item) {
                skipped_item.get_or_insert(item);
                continue;
            }

            if absolute_item_top >= scroll_top {
                return Some(ScrollAnchor::Item(ItemAnchor {
                    id,
                    viewport_offset: absolute_item_top - scroll_top,
                }));
            }

            // First attempt to grab a the first fully visible line
            let anchor_viewport_top =
                self.get_scroll_anchor_viewport_top(absolute_item_top, scroll_top);
            let local_viewport_top = anchor_viewport_top - absolute_item_top;
            let instance = Rc::clone(item.borrow().instance());
            if let Some(line_anchor) = instance.get_numeric_scroll_anchor(local_viewport_top) {
                let absolute_line_top = absolute_item_top + line_anchor.top;
                return Some(ScrollAnchor::Line(LineAnchor {
                    id,
                    line_number: line_anchor.line_number,
                    side: line_anchor.side,
                    viewport_offset: absolute_line_top - scroll_top,
                }));
            }
        }

        // If we couldn't find an anchored item and we have a skipped item,
        // lets attempt to anchor to the top of the item that came after it
        if let Some(skipped_item) = skipped_item {
            let start = skipped_item.borrow().index + 1;
            let items = self.state.borrow().items.clone();
            for candidate in items.iter().skip(start) {
                let id = candidate.borrow().id().to_owned();
                if is_available(&id, candidate) {
                    return Some(ScrollAnchor::Item(ItemAnchor {
                        id,
                        viewport_offset: 0.0,
                    }));
                }
            }
        }

        // I don't think we'll ever make it this far...
        None
    }

    /// `resolveAnchoredScrollTop(anchor)`: given a scroll anchor, a newly
    /// updated (and clamped) scroll position that keeps the anchored element
    /// in place, or `None` when none can be resolved.
    pub(crate) fn resolve_anchored_scroll_top(&self, anchor: &ScrollAnchor) -> Option<f64> {
        let item = self.item_by_id(anchor.id())?;
        let item_top_offset = self.get_item_top_offset();
        let item_top = item.borrow().top;
        match anchor {
            ScrollAnchor::Item(anchor) => {
                let absolute_item_top = item_top_offset + item_top;
                Some(self.clamp_scroll_top(absolute_item_top - anchor.viewport_offset))
            }
            ScrollAnchor::Line(anchor) => {
                let line_position =
                    Self::get_line_scroll_position(&item, anchor.line_number, anchor.side)?;
                let absolute_line_top = item_top_offset + item_top + line_position.top;
                Some(self.clamp_scroll_top(absolute_line_top - anchor.viewport_offset))
            }
        }
    }

    /// `isPendingTargetSettled(target)`: whether a pending programmatic scroll
    /// has reached its destination and should be cleared.
    pub(crate) fn is_pending_target_settled(&self, target: &PendingScrollTarget) -> bool {
        let Some(top) = self.resolve_scroll_target_top(target) else {
            return true;
        };
        self.round_to_device_pixel(self.get_scroll_top()) == self.round_to_device_pixel(top)
    }
}
