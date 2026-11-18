//! The host's width class: the one geometric fact components are given instead of measuring.
//!
//! Neither renderer lets a component ask the DOM how wide it is (Blitz panics when
//! `get_client_rect` runs inside an event handler or `onmounted`, and a guest has no DOM at all),
//! so the host, which owns the window, classifies its width and passes the class down as a prop
//! (`KIT-040`). The breakpoints are defined here and nowhere else.

/// How much horizontal room the host has, in the Material window-size-class steps.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum WidthClass {
    /// A phone held upright: one panel at a time.
    Compact,
    /// A large phone sideways or a small tablet: panels side by side are possible but tight.
    Medium,
    /// A desktop window or a tablet: panels side by side.
    Expanded,
}

impl WidthClass {
    /// Narrowest logical width, in px, that is `Medium`.
    pub const MEDIUM_MIN_PX: f64 = 600.0;
    /// Narrowest logical width, in px, that is `Expanded`.
    pub const EXPANDED_MIN_PX: f64 = 840.0;

    /// Classify a logical (CSS px, scale-factor-divided) width. Hosts call this with the window
    /// width from `winit`/`tao`/`window.innerWidth`.
    pub fn from_px(width: f64) -> Self {
        assert!(
            width.is_finite() && width >= 0.0,
            "a width must be finite and non-negative, got {width}"
        );
        if width >= Self::EXPANDED_MIN_PX {
            Self::Expanded
        } else if width >= Self::MEDIUM_MIN_PX {
            Self::Medium
        } else {
            Self::Compact
        }
    }

    /// Whether two panels (a list and its details) fit side by side. This is the single rule
    /// every adaptive component derives its arrangement from.
    pub fn fits_side_by_side(self) -> bool {
        self != Self::Compact
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_at_the_breakpoints() {
        assert_eq!(WidthClass::from_px(0.0), WidthClass::Compact);
        assert_eq!(WidthClass::from_px(599.9), WidthClass::Compact);
        assert_eq!(WidthClass::from_px(600.0), WidthClass::Medium);
        assert_eq!(WidthClass::from_px(839.9), WidthClass::Medium);
        assert_eq!(WidthClass::from_px(840.0), WidthClass::Expanded);
    }

    #[test]
    #[should_panic(expected = "finite and non-negative")]
    fn rejects_a_nan_width() {
        WidthClass::from_px(f64::NAN);
    }

    #[test]
    fn only_compact_stacks() {
        assert!(!WidthClass::Compact.fits_side_by_side());
        assert!(WidthClass::Medium.fits_side_by_side());
        assert!(WidthClass::Expanded.fits_side_by_side());
    }
}
