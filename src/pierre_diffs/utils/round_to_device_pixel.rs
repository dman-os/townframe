// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/roundToDevicePixel.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `window.devicePixelRatio ?? 1` is the argument (`CodeViewHost::device_pixel_ratio`
// answers it, read fresh on every call as upstream does). `Math.round` rounds
// halves up.

/// Snap a CSS-pixel value to the nearest device-pixel boundary. Browsers store
/// scrollTop on the device-pixel grid on fractional-DPR displays (1.25x, 1.5x,
/// etc.), so rounding computed scroll targets against that grid keeps delta
/// math settling cleanly instead of hovering around fractional residuals.
pub fn round_to_device_pixel(value: f64, device_pixel_ratio: f64) -> f64 {
    let scaled = value * device_pixel_ratio;
    let floor = scaled.floor();
    let rounded = if scaled - floor >= 0.5 {
        floor + 1.0
    } else {
        floor
    };
    rounded / device_pixel_ratio
}

#[cfg(test)]
mod tests {
    use super::round_to_device_pixel;

    #[test]
    fn snaps_to_the_device_pixel_grid() {
        assert_eq!(round_to_device_pixel(10.3, 1.0), 10.0);
        assert_eq!(round_to_device_pixel(10.5, 1.0), 11.0);
        assert_eq!(round_to_device_pixel(-10.5, 1.0), -10.0);
        assert_eq!(round_to_device_pixel(10.3, 2.0), 10.5);
        assert_eq!(round_to_device_pixel(10.1, 1.25), 10.4);
    }
}
