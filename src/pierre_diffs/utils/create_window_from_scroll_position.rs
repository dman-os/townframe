// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/createWindowFromScrollPosition.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: optional props default as upstream (`fitPerfectly = false`,
// `fitPerfectlyOverscroll = 0`) via `WindowFromScrollPositionProps::new`.

use crate::types::layout::VirtualWindowSpecs;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WindowFromScrollPositionProps {
    pub scroll_top: f64,
    pub height: f64,
    pub scroll_height: f64,
    pub fit_perfectly: bool,
    pub fit_perfectly_overscroll: f64,
    pub overscroll_size: f64,
}

impl WindowFromScrollPositionProps {
    pub fn new(scroll_top: f64, height: f64, scroll_height: f64, overscroll_size: f64) -> Self {
        Self {
            scroll_top,
            height,
            scroll_height,
            fit_perfectly: false,
            fit_perfectly_overscroll: 0.0,
            overscroll_size,
        }
    }
}

pub fn create_window_from_scroll_position(
    props: WindowFromScrollPositionProps,
) -> VirtualWindowSpecs {
    let WindowFromScrollPositionProps {
        scroll_top,
        height,
        scroll_height,
        fit_perfectly,
        fit_perfectly_overscroll,
        overscroll_size,
    } = props;
    let window_height = height + overscroll_size * 2.0;
    let effective_height = if fit_perfectly {
        height + fit_perfectly_overscroll * 2.0
    } else {
        window_height
    };
    let scroll_height = scroll_height.max(effective_height);

    if window_height >= scroll_height || fit_perfectly {
        let top = (scroll_top - fit_perfectly_overscroll).max(0.0);
        let bottom = (scroll_top + effective_height).min(scroll_height);
        return VirtualWindowSpecs {
            top,
            bottom: bottom.max(top),
        };
    }

    let scroll_center = scroll_top + height / 2.0;
    let mut top = scroll_center - window_height / 2.0;
    let mut bottom = top + window_height;
    if top < 0.0 {
        top = 0.0;
    }
    if bottom > scroll_height {
        bottom = scroll_height;
    }
    top = top.max(0.0).floor();
    VirtualWindowSpecs {
        top,
        bottom: bottom.min(scroll_height).max(top).ceil(),
    }
}
