//! Port of `packages/react/src/utils/useSwipeDismiss.ts` (Base UI v1.9.0, MIT, Copyright (c) 2019
//! Material-UI SAS; the licence text is in `swipe.rs`).
//!
//! The hook is a state machine over refs; [`SwipeDismiss`] holds the same state under the same
//! names and its methods are the hook's inner functions. The DOM is replaced by inputs (the
//! element's size and computed transform at press time) and outputs ([`Progress`] is what
//! `onProgress` would be called with, [`SwipeEnd`] is what `onDismiss` or the snap-back would
//! do). `setSwiping`'s `onSwipingChange` and `setCurrentSwipeDirection`'s re-render are read back
//! through [`SwipeDismiss::is_swiping`] and [`SwipeDismiss::swipe_direction`].

use super::Point;
use super::Size;
use super::clamp;

/// `useSwipeDismiss.ts:12`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SwipeDirection {
    Up,
    Down,
    Left,
    Right,
}

impl SwipeDirection {
    fn is_horizontal(self) -> bool {
        matches!(self, Self::Left | Self::Right)
    }
}

/// `useSwipeDismiss.ts:32`.
pub const DEFAULT_SWIPE_THRESHOLD: f64 = 40.0;
/// `useSwipeDismiss.ts:33`.
pub const REVERSE_CANCEL_THRESHOLD: f64 = 10.0;
/// `useSwipeDismiss.ts:34`.
pub const MIN_VELOCITY_DURATION_MS: f64 = 50.0;
/// `useSwipeDismiss.ts:35-36`: under one frame at 240 Hz, so it only damps near-simultaneous
/// samples.
pub const MIN_RELEASE_VELOCITY_DURATION_MS: f64 = 4.0;
/// `useSwipeDismiss.ts:37`.
pub const MAX_RELEASE_VELOCITY_AGE_MS: f64 = 80.0;
/// `useSwipeDismiss.ts:38`.
pub const MIN_VELOCITY_SAMPLE_DISTANCE: f64 = 1.0;

/// `useSwipeDismiss.ts:41-54`.
pub fn get_displacement(direction: SwipeDirection, delta_x: f64, delta_y: f64) -> f64 {
    match direction {
        SwipeDirection::Up => -delta_y,
        SwipeDirection::Down => delta_y,
        SwipeDirection::Left => -delta_x,
        SwipeDirection::Right => delta_x,
    }
}

/// `useSwipeDismiss.ts:56-58`: a timestamp is usable when it is finite and positive (synthetic
/// events report `0`).
pub fn get_valid_timestamp(time_stamp: f64) -> Option<f64> {
    (time_stamp.is_finite() && time_stamp > 0.0).then_some(time_stamp)
}

/// The element's computed 2D translation and scale (`utils/getElementTransform.ts:10-33`, the
/// parse of the `matrix(..)` string is the caller's).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Transform {
    pub x: f64,
    pub y: f64,
    pub scale: f64,
}

impl Default for Transform {
    /// `useSwipeDismiss.ts:134`: `{ x: 0, y: 0, scale: 1 }`.
    fn default() -> Self {
        Self {
            x: 0.0,
            y: 0.0,
            scale: 1.0,
        }
    }
}

/// What the hook reads off `elementRef.current` when a swipe starts
/// (`useSwipeDismiss.ts:405-410`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ElementMetrics {
    pub size: Size,
    pub transform: Transform,
}

/// `useSwipeDismiss.ts:26-30` (`SwipeProgressDetailsInternal`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ProgressDetails {
    pub delta_x: f64,
    pub delta_y: f64,
    pub direction: Option<SwipeDirection>,
}

/// An `onProgress(progress, details)` call (`useSwipeDismiss.ts:210`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Progress {
    pub progress: f64,
    pub details: Option<ProgressDetails>,
}

/// The `swipeThreshold` option (`useSwipeDismiss.ts:1053-1054`): px, or a function of the
/// element and the direction. The element is its [`Size`] here.
#[derive(Clone, Copy, Debug)]
pub enum SwipeThreshold {
    Fixed(f64),
    PerDirection(fn(Size, SwipeDirection) -> f64),
}

/// The details `onRelease` receives (`useSwipeDismiss.ts:1091-1099`, minus the DOM `event`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Release {
    pub direction: Option<SwipeDirection>,
    pub delta_x: f64,
    pub delta_y: f64,
    pub velocity_x: f64,
    pub velocity_y: f64,
    pub release_velocity_x: f64,
    pub release_velocity_y: f64,
}

/// What `handleEnd` ends in (`useSwipeDismiss.ts:716-827`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SwipeEnd {
    /// `useSwipeDismiss.ts:731-735`: the gesture never activated.
    NotSwiping,
    /// `onDismiss(event, { direction })` (`:816-820`).
    Dismissed(SwipeDirection),
    /// The element goes back to its initial transform (`:792-798`, `:821-826`).
    SnappedBack,
}

/// The result of a move (`handleMoveCore`, `useSwipeDismiss.ts:562-714`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Moved {
    /// `onProgress`, unless `updateSwipeProgress` deduped it away.
    pub progress: Option<Progress>,
    /// Whether the drag offset changed, which is when upstream rewrites the drag styles
    /// (`:683-689`).
    pub offset_changed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct DragSample {
    x: f64,
    y: f64,
    time: f64,
}

/// `useSwipeDismiss.ts:249-285` and the hook's refs (`:131-156`), one gesture at a time.
pub struct SwipeDismiss {
    directions: Vec<SwipeDirection>,
    primary_direction: Option<SwipeDirection>,
    swipe_threshold_default: f64,
    swipe_threshold_prop: Option<SwipeThreshold>,
    track_drag: bool,
    allow_left: bool,
    allow_right: bool,
    allow_up: bool,
    allow_down: bool,
    has_horizontal: bool,
    has_vertical: bool,

    current_swipe_direction: Option<SwipeDirection>,
    drag_dismissed: bool,
    is_swiping: bool,

    drag_start_pos: Point,
    drag_offset: Point,
    last_move_pos: Option<Point>,
    initial_transform: Transform,
    intended_swipe_direction: Option<SwipeDirection>,
    max_swipe_displacement: f64,
    cancelled_swipe: bool,
    swipe_cancel_baseline: Point,
    is_first_pointer_move: bool,
    element: Option<Size>,
    swipe_progress: f64,
    swipe_threshold: f64,
    swipe_threshold_function: Option<fn(Size, SwipeDirection) -> f64>,
    swipe_start_time: Option<f64>,
    last_drag_sample: Option<DragSample>,
    has_stationary_sample: bool,
    last_drag_velocity: Point,
    last_progress_details: Option<ProgressDetails>,
}

impl SwipeDismiss {
    /// `useSwipeDismiss.ts:88-130`. `track_drag` defaults to `true` upstream (`:104`);
    /// the swipe area passes `false`. `enabled` is the caller's: a disabled hook is a hook whose
    /// methods are not called.
    pub fn new(
        directions: &[SwipeDirection],
        swipe_threshold: Option<SwipeThreshold>,
        track_drag: bool,
    ) -> Self {
        let swipe_threshold_default = match swipe_threshold {
            Some(SwipeThreshold::Fixed(value)) => value.max(0.0),
            _ => DEFAULT_SWIPE_THRESHOLD,
        };
        let allow_left = directions.contains(&SwipeDirection::Left);
        let allow_right = directions.contains(&SwipeDirection::Right);
        let allow_up = directions.contains(&SwipeDirection::Up);
        let allow_down = directions.contains(&SwipeDirection::Down);
        Self {
            directions: directions.to_vec(),
            primary_direction: match directions {
                [only] => Some(*only),
                _ => None,
            },
            swipe_threshold_default,
            swipe_threshold_prop: swipe_threshold,
            track_drag,
            allow_left,
            allow_right,
            allow_up,
            allow_down,
            has_horizontal: allow_left || allow_right,
            has_vertical: allow_up || allow_down,
            current_swipe_direction: None,
            drag_dismissed: false,
            is_swiping: false,
            drag_start_pos: Point::default(),
            drag_offset: Point::default(),
            last_move_pos: None,
            initial_transform: Transform::default(),
            intended_swipe_direction: None,
            max_swipe_displacement: 0.0,
            cancelled_swipe: false,
            swipe_cancel_baseline: Point::default(),
            is_first_pointer_move: false,
            element: None,
            swipe_progress: 0.0,
            swipe_threshold: swipe_threshold_default,
            swipe_threshold_function: None,
            swipe_start_time: None,
            last_drag_sample: None,
            has_stationary_sample: false,
            last_drag_velocity: Point::default(),
            last_progress_details: None,
        }
    }

    /// `swiping` of the hook's return value (`:1023`).
    pub fn is_swiping(&self) -> bool {
        self.is_swiping
    }

    /// `swipeDirection` of the hook's return value (`:1024`).
    pub fn swipe_direction(&self) -> Option<SwipeDirection> {
        self.current_swipe_direction
    }

    /// `dragDismissed` of the hook's return value (`:1025`).
    pub fn drag_dismissed(&self) -> bool {
        self.drag_dismissed
    }

    /// The `dragOffsetRef` the drag styles are built from (`:236-243`, `:969-1003`).
    pub fn drag_offset(&self) -> Point {
        self.drag_offset
    }

    /// The transform captured when the swipe started (`initialTransformRef`).
    pub fn initial_transform(&self) -> Transform {
        self.initial_transform
    }

    /// `useSwipeDismiss.ts:168-182`.
    fn resolve_swipe_threshold(&mut self, direction: Option<SwipeDirection>) {
        let Some(direction) = direction else {
            return;
        };
        let (Some(element), Some(threshold_function)) =
            (self.element, self.swipe_threshold_function)
        else {
            return;
        };
        self.swipe_threshold = threshold_function(element, direction).max(0.0);
    }

    /// `useSwipeDismiss.ts:184-212`. Returns the `onProgress` call, if upstream would make one.
    fn update_swipe_progress(
        &mut self,
        progress: f64,
        details: Option<ProgressDetails>,
    ) -> Option<Progress> {
        let next_progress = if progress.is_finite() {
            clamp(progress, 0.0, 1.0)
        } else {
            0.0
        };
        let progress_changed = next_progress != self.swipe_progress;
        let details_changed = details.is_some_and(|details| {
            self.last_progress_details
                .is_none_or(|last| last != details)
        });
        if !progress_changed && !details_changed {
            return None;
        }
        self.swipe_progress = next_progress;
        if details.is_some() {
            self.last_progress_details = details;
        } else if progress_changed {
            self.last_progress_details = None;
        }
        Some(Progress {
            progress: next_progress,
            details,
        })
    }

    /// `useSwipeDismiss.ts:249-285`.
    fn record_drag_sample(&mut self, offset: Point, time_stamp: Option<f64>) {
        let Some(time_stamp) = time_stamp else {
            return;
        };
        if let Some(last_sample) = self.last_drag_sample
            && time_stamp > last_sample.time
        {
            // Some Android devices emit one effectively stationary move immediately before
            // release. Keep the last moving sample and its timestamp. Repeated stationary moves
            // still clear the velocity, and a release after a hold still expires the last moving
            // sample.
            let stationary = (offset.x - last_sample.x).abs() < MIN_VELOCITY_SAMPLE_DISTANCE
                && (offset.y - last_sample.y).abs() < MIN_VELOCITY_SAMPLE_DISTANCE;
            let skip_sample = stationary && !self.has_stationary_sample;
            self.has_stationary_sample = stationary;
            if skip_sample {
                return;
            }
            let duration_ms = (time_stamp - last_sample.time).max(MIN_RELEASE_VELOCITY_DURATION_MS);
            self.last_drag_velocity = Point {
                x: (offset.x - last_sample.x) / duration_ms,
                y: (offset.y - last_sample.y) / duration_ms,
            };
        }
        self.last_drag_sample = Some(DragSample {
            x: offset.x,
            y: offset.y,
            time: time_stamp,
        });
    }

    /// `reset` (`useSwipeDismiss.ts:278-306`). Returns the `onProgress(0)` it may fire.
    pub fn reset(&mut self) -> Option<Progress> {
        self.current_swipe_direction = None;
        self.is_swiping = false;
        self.drag_dismissed = false;
        let progress = self.update_swipe_progress(0.0, None);

        self.swipe_threshold = self.swipe_threshold_default;
        self.swipe_threshold_function = None;
        self.drag_start_pos = Point::default();
        self.drag_offset = Point::default();
        self.initial_transform = Transform::default();
        self.intended_swipe_direction = None;
        self.max_swipe_displacement = 0.0;
        self.cancelled_swipe = false;
        self.swipe_cancel_baseline = Point::default();
        self.is_first_pointer_move = false;
        self.last_move_pos = None;
        self.element = None;
        self.swipe_start_time = None;
        self.last_drag_sample = None;
        self.has_stationary_sample = false;
        self.last_drag_velocity = Point::default();
        self.last_progress_details = None;
        progress
    }

    /// The state half of `startSwipeAtPosition` (`useSwipeDismiss.ts:353-426`), lines `:389-423`.
    /// The checks that precede them (a scrollable target under a touch, an interactive element,
    /// a scrollable ancestor, `canStart`) read the DOM and stay with the caller, as do the
    /// pointer capture and `onSwipeStart`. `element` is `None` when the hook has no element
    /// (`:404`).
    pub fn start_swipe_at_position(
        &mut self,
        position: Point,
        time_stamp: f64,
        element: Option<ElementMetrics>,
    ) -> Option<Progress> {
        self.cancelled_swipe = false;
        self.intended_swipe_direction = None;
        self.max_swipe_displacement = 0.0;

        self.drag_start_pos = position;
        self.swipe_start_time = get_valid_timestamp(time_stamp);
        self.last_drag_sample = None;
        self.has_stationary_sample = false;
        self.last_drag_velocity = Point::default();
        self.swipe_cancel_baseline = position;
        self.last_move_pos = Some(position);
        self.swipe_threshold = self.swipe_threshold_default;
        self.swipe_threshold_function = match self.swipe_threshold_prop {
            Some(SwipeThreshold::PerDirection(function)) => Some(function),
            _ => None,
        };

        self.element = element.map(|element| element.size);
        if let Some(element) = element {
            self.resolve_swipe_threshold(self.primary_direction);
            self.initial_transform = element.transform;
            self.drag_offset = Point {
                x: element.transform.x,
                y: element.transform.y,
            };
            self.record_drag_sample(self.drag_offset, self.swipe_start_time);
        }

        self.is_swiping = true;
        self.is_first_pointer_move = true;
        self.update_swipe_progress(0.0, None)
    }

    /// `cancelSwipeInteraction` (`useSwipeDismiss.ts:439-467`), state half: a non-primary mouse
    /// button took over the gesture. Returns the `onProgress` it fires. Upstream fires `onCancel`
    /// only when a swipe was active, which is what [`Self::is_swiping`] said before this call
    /// (the progress can be deduped away while the cancel still happened).
    pub fn cancel_swipe_interaction(&mut self) -> Option<Progress> {
        self.last_move_pos = None;
        if !self.is_swiping {
            return None;
        }
        self.is_swiping = false;
        self.drag_offset = Point {
            x: self.initial_transform.x,
            y: self.initial_transform.y,
        };
        self.current_swipe_direction = None;
        self.update_swipe_progress(
            0.0,
            Some(ProgressDetails {
                delta_x: 0.0,
                delta_y: 0.0,
                direction: None,
            }),
        )
    }

    /// `applyDirectionalDamping` (`useSwipeDismiss.ts:469-482`): square-root damping on every
    /// axis or side that is not a swipe direction.
    fn apply_directional_damping(&self, delta_x: f64, delta_y: f64) -> Point {
        let exponent = |value: f64| super::sign(value) * value.abs().powf(0.5);
        let damp_axis = |delta: f64, allow_negative: bool, allow_positive: bool| {
            if (!allow_negative && delta < 0.0) || (!allow_positive && delta > 0.0) {
                exponent(delta)
            } else {
                delta
            }
        };
        Point {
            x: if self.has_horizontal {
                damp_axis(delta_x, self.allow_left, self.allow_right)
            } else {
                exponent(delta_x)
            },
            y: if self.has_vertical {
                damp_axis(delta_y, self.allow_up, self.allow_down)
            } else {
                exponent(delta_y)
            },
        }
    }

    /// `canSwipeFromScrollEdgeOnPendingMove` (`useSwipeDismiss.ts:484-522`): whether a touch that
    /// began on a scroll container may become a swipe, given the container's scroll metrics.
    /// `Some(false)` keeps the container scrolling, `Some(true)` hands the gesture to the swipe,
    /// `None` is undecided.
    pub fn can_swipe_from_scroll_edge_on_pending_move(
        &self,
        scroll: ScrollMetrics,
        delta_x: f64,
        delta_y: f64,
    ) -> Option<bool> {
        // Swiping toward the axis start edge (down/right) is allowed when scrolled to the start;
        // toward the end edge (up/left) when scrolled to the end.
        let can_swipe_on_axis = |delta: f64,
                                 scroll_offset: f64,
                                 max_scroll_offset: f64,
                                 allow_toward_start: bool,
                                 allow_toward_end: bool| {
            (delta > 0.0 && scroll_offset <= 0.0 && allow_toward_start)
                || (delta < 0.0 && scroll_offset >= max_scroll_offset.max(0.0) && allow_toward_end)
        };
        if self.has_vertical && delta_y != 0.0 {
            return Some(can_swipe_on_axis(
                delta_y,
                scroll.scroll_top,
                scroll.scroll_height - scroll.client_height,
                self.allow_down,
                self.allow_up,
            ));
        }
        if self.has_horizontal && delta_x != 0.0 {
            return Some(can_swipe_on_axis(
                delta_x,
                scroll.scroll_left,
                scroll.scroll_width - scroll.client_width,
                self.allow_right,
                self.allow_left,
            ));
        }
        None
    }

    /// `handleMove` (`useSwipeDismiss.ts:829-964`) from the movement computation (`:941-948`),
    /// then `handleMoveCore` (`:562-714`). The pending-start dance before it (`canStart`, scroll
    /// edges, the `buttons` bookkeeping of mouse pointers) is the caller's, and so are the text
    /// selection and touch-scroll checks at the top of `handleMoveCore`.
    pub fn handle_move(&mut self, position: Point, time_stamp: f64) -> Moved {
        let movement = match self.last_move_pos {
            None => Point::default(),
            Some(previous) => Point {
                x: position.x - previous.x,
                y: position.y - previous.y,
            },
        };
        self.last_move_pos = Some(position);
        self.handle_move_core(time_stamp, position, movement)
    }

    /// `useSwipeDismiss.ts:562-714`.
    fn handle_move_core(&mut self, time_stamp: f64, position: Point, movement: Point) -> Moved {
        if !self.is_swiping {
            return Moved::default();
        }

        if self.is_first_pointer_move {
            self.is_first_pointer_move = false;
            // Reset the drag origin to the first move's position to absorb the gap between the
            // press and the first move event, notably on iOS touch, where the first `touchmove`
            // arrives already offset from the `touchstart`, which would otherwise make the
            // dragged element jump. This only matters when an element follows the pointer; when
            // `trackDrag` is false (the swipe area, which only opens the drawer) keep the
            // original press position so a quick flick still registers: on a low-refresh-rate
            // display the whole travel can land in this single first move, and discarding it
            // would drop the gesture.
            if self.track_drag {
                self.drag_start_pos = position;
                if let Some(move_time) = get_valid_timestamp(time_stamp) {
                    self.swipe_start_time = Some(move_time);
                }
                self.last_drag_sample = None;
                self.has_stationary_sample = false;
            }
        }

        let client_x = position.x;
        let client_y = position.y;

        if (movement.y < 0.0 && client_y > self.swipe_cancel_baseline.y)
            || (movement.y > 0.0 && client_y < self.swipe_cancel_baseline.y)
        {
            self.swipe_cancel_baseline.y = client_y;
        }
        if (movement.x < 0.0 && client_x > self.swipe_cancel_baseline.x)
            || (movement.x > 0.0 && client_x < self.swipe_cancel_baseline.x)
        {
            self.swipe_cancel_baseline.x = client_x;
        }

        let delta_x = client_x - self.drag_start_pos.x;
        let delta_y = client_y - self.drag_start_pos.y;
        let cancel_delta_y = client_y - self.swipe_cancel_baseline.y;
        let cancel_delta_x = client_x - self.swipe_cancel_baseline.x;

        match self.intended_swipe_direction {
            None => {
                let candidate = if delta_x.abs() >= delta_y.abs() {
                    if delta_x > 0.0 {
                        SwipeDirection::Right
                    } else {
                        SwipeDirection::Left
                    }
                } else if delta_y > 0.0 {
                    SwipeDirection::Down
                } else {
                    SwipeDirection::Up
                };
                let is_allowed = (candidate == SwipeDirection::Left && self.allow_left)
                    || (candidate == SwipeDirection::Right && self.allow_right)
                    || (candidate == SwipeDirection::Up && self.allow_up)
                    || (candidate == SwipeDirection::Down && self.allow_down);
                if is_allowed {
                    self.intended_swipe_direction = Some(candidate);
                    self.max_swipe_displacement = get_displacement(candidate, delta_x, delta_y);
                    self.current_swipe_direction = Some(candidate);
                    self.resolve_swipe_threshold(Some(candidate));
                }
            }
            Some(direction) => {
                let current_displacement =
                    get_displacement(direction, cancel_delta_x, cancel_delta_y);
                if current_displacement > self.swipe_threshold {
                    self.cancelled_swipe = false;
                    self.current_swipe_direction = Some(direction);
                } else if !(self.allow_left && self.allow_right)
                    && !(self.allow_up && self.allow_down)
                    && self.max_swipe_displacement - current_displacement
                        >= REVERSE_CANCEL_THRESHOLD
                {
                    // Mark that a change-of-mind has occurred.
                    self.cancelled_swipe = true;
                }
            }
        }

        let damped_delta = self.apply_directional_damping(delta_x, delta_y);
        let mut new_offset_x = self.initial_transform.x;
        let mut new_offset_y = self.initial_transform.y;
        if self.has_horizontal {
            new_offset_x += damped_delta.x;
        }
        if self.has_vertical {
            new_offset_y += damped_delta.y;
        }

        // Only rewrite drag styles when the drag offset actually changed (see upstream's comment
        // at `:676-682`: a move that leaves the offset alone must not reinstate the raw styles).
        let previous_offset = self.drag_offset;
        let offset_changed = new_offset_x != previous_offset.x || new_offset_y != previous_offset.y;

        self.drag_offset = Point {
            x: new_offset_x,
            y: new_offset_y,
        };
        self.record_drag_sample(self.drag_offset, get_valid_timestamp(time_stamp));
        let drag_delta_x = new_offset_x - self.initial_transform.x;
        let drag_delta_y = new_offset_y - self.initial_transform.y;
        let progress_details = ProgressDetails {
            delta_x: drag_delta_x,
            delta_y: drag_delta_y,
            direction: self.intended_swipe_direction,
        };

        let mut progress = 0.0;
        let progress_direction = self.primary_direction.or(self.intended_swipe_direction);
        if let Some(progress_direction) = progress_direction {
            let element_size = self.element.unwrap_or_default();
            let size = if progress_direction.is_horizontal() {
                element_size.width
            } else {
                element_size.height
            };
            let scale =
                if self.initial_transform.scale == 0.0 || self.initial_transform.scale.is_nan() {
                    1.0
                } else {
                    self.initial_transform.scale
                };
            let progress_displacement =
                get_displacement(progress_direction, drag_delta_x, drag_delta_y);
            if size > 0.0 && scale > 0.0 && progress_displacement > 0.0 {
                progress = progress_displacement / (size * scale);
            }
        }

        Moved {
            progress: self.update_swipe_progress(progress, Some(progress_details)),
            offset_changed,
        }
    }

    /// `handleEnd` (`useSwipeDismiss.ts:716-827`). `on_release` is the hook's `onRelease`
    /// option: `Some(true)`/`Some(false)` overrides the default dismissal, `None` keeps it
    /// (`:790`). Pointer capture release and the style writes are the caller's.
    pub fn handle_end(
        &mut self,
        time_stamp: f64,
        on_release: impl FnOnce(&Release) -> Option<bool>,
    ) -> (SwipeEnd, Option<Progress>) {
        let resolved_drag_offset = self.drag_offset;
        let resolved_initial_transform = self.initial_transform;
        let release_delta_x = resolved_drag_offset.x - resolved_initial_transform.x;
        let release_delta_y = resolved_drag_offset.y - resolved_initial_transform.y;
        let progress_details = ProgressDetails {
            delta_x: release_delta_x,
            delta_y: release_delta_y,
            direction: self.intended_swipe_direction,
        };

        self.last_move_pos = None;
        if !self.is_swiping {
            return (
                SwipeEnd::NotSwiping,
                self.update_swipe_progress(0.0, Some(progress_details)),
            );
        }

        self.is_swiping = false;

        let delta_x = release_delta_x;
        let delta_y = release_delta_y;
        let start_time = self.swipe_start_time;
        let end_time = get_valid_timestamp(time_stamp);
        let duration_ms = match (start_time, end_time) {
            (Some(start), Some(end)) if end > start => end - start,
            _ => 0.0,
        };
        let velocity_duration_ms = if duration_ms > 0.0 {
            duration_ms.max(MIN_VELOCITY_DURATION_MS)
        } else {
            0.0
        };
        let velocity_x = if velocity_duration_ms > 0.0 {
            delta_x / velocity_duration_ms
        } else {
            0.0
        };
        let velocity_y = if velocity_duration_ms > 0.0 {
            delta_y / velocity_duration_ms
        } else {
            0.0
        };
        let mut release_velocity_x = self.last_drag_velocity.x;
        let mut release_velocity_y = self.last_drag_velocity.y;
        if let (Some(last_sample), Some(end_time)) = (self.last_drag_sample, end_time)
            && end_time >= last_sample.time
        {
            let age_ms = end_time - last_sample.time;
            if age_ms <= MAX_RELEASE_VELOCITY_AGE_MS {
                let sample_duration_ms = age_ms.max(MIN_RELEASE_VELOCITY_DURATION_MS);
                let delta_from_last_sample_x = resolved_drag_offset.x - last_sample.x;
                let delta_from_last_sample_y = resolved_drag_offset.y - last_sample.y;
                let sample_velocity_x = delta_from_last_sample_x / sample_duration_ms;
                let sample_velocity_y = delta_from_last_sample_y / sample_duration_ms;
                if delta_from_last_sample_x.abs() >= MIN_VELOCITY_SAMPLE_DISTANCE {
                    release_velocity_x = sample_velocity_x;
                }
                if delta_from_last_sample_y.abs() >= MIN_VELOCITY_SAMPLE_DISTANCE {
                    release_velocity_y = sample_velocity_y;
                }
            } else {
                release_velocity_x = 0.0;
                release_velocity_y = 0.0;
            }
        }

        let release_decision = on_release(&Release {
            direction: self.intended_swipe_direction,
            delta_x,
            delta_y,
            velocity_x,
            velocity_y,
            release_velocity_x,
            release_velocity_y,
        });

        if self.cancelled_swipe && release_decision.is_none() {
            self.drag_offset = Point {
                x: resolved_initial_transform.x,
                y: resolved_initial_transform.y,
            };
            self.current_swipe_direction = None;
            return (
                SwipeEnd::SnappedBack,
                self.update_swipe_progress(0.0, Some(progress_details)),
            );
        }

        let mut should_close = false;
        let mut dismiss_direction = None;
        if let Some(decision) = release_decision {
            should_close = decision;
            dismiss_direction = self.intended_swipe_direction.or(self.primary_direction);
        } else {
            for direction in &self.directions {
                if get_displacement(*direction, delta_x, delta_y) > self.swipe_threshold {
                    should_close = true;
                    dismiss_direction = Some(*direction);
                    break;
                }
            }
        }

        if let (true, Some(direction)) = (should_close, dismiss_direction) {
            self.current_swipe_direction = Some(direction);
            self.drag_dismissed = true;
            (SwipeEnd::Dismissed(direction), None)
        } else {
            self.drag_offset = Point {
                x: resolved_initial_transform.x,
                y: resolved_initial_transform.y,
            };
            self.current_swipe_direction = None;
            (
                SwipeEnd::SnappedBack,
                self.update_swipe_progress(0.0, Some(progress_details)),
            )
        }
    }
}

/// The scroll container metrics `canSwipeFromScrollEdgeOnPendingMove` reads
/// (`useSwipeDismiss.ts:501-518`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ScrollMetrics {
    pub scroll_top: f64,
    pub scroll_height: f64,
    pub client_height: f64,
    pub scroll_left: f64,
    pub scroll_width: f64,
    pub client_width: f64,
}

#[cfg(test)]
mod tests;
