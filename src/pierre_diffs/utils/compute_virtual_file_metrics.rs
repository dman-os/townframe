// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/computeVirtualFileMetrics.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `Partial<VirtualFileMetrics>` is `PartialVirtualFileMetrics`, a
// struct of options merged over the defaults.

use crate::constants::DEFAULT_VIRTUAL_FILE_METRICS;
use crate::types::layout::VirtualFileMetrics;
use crate::types::options::HunkSeparators;

/// `Partial<VirtualFileMetrics>`
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct PartialVirtualFileMetrics {
    pub hunk_line_count: Option<usize>,
    pub line_height: Option<f64>,
    pub diff_header_height: Option<f64>,
    pub hunk_separator_height: Option<f64>,
    pub spacing: Option<f64>,
    pub padding_top: Option<f64>,
    pub padding_bottom: Option<f64>,
}

pub fn compute_virtual_file_metrics(metrics: PartialVirtualFileMetrics) -> VirtualFileMetrics {
    let defaults = DEFAULT_VIRTUAL_FILE_METRICS;
    VirtualFileMetrics {
        hunk_line_count: metrics.hunk_line_count.unwrap_or(defaults.hunk_line_count),
        line_height: metrics.line_height.unwrap_or(defaults.line_height),
        diff_header_height: metrics
            .diff_header_height
            .unwrap_or(defaults.diff_header_height),
        hunk_separator_height: metrics
            .hunk_separator_height
            .or(defaults.hunk_separator_height),
        spacing: metrics.spacing.unwrap_or(defaults.spacing),
        padding_top: metrics.padding_top.or(defaults.padding_top),
        padding_bottom: metrics.padding_bottom.or(defaults.padding_bottom),
    }
}

pub fn get_virtual_file_header_region(
    metrics: &VirtualFileMetrics,
    disable_file_header: bool,
) -> f64 {
    get_virtual_file_header_height(metrics, disable_file_header)
        + get_virtual_file_padding_top(metrics, disable_file_header)
}

// Collapsed items render only the header. The top padding belongs to the
// code content, which is removed on collapse.
pub fn get_virtual_file_header_height(
    metrics: &VirtualFileMetrics,
    disable_file_header: bool,
) -> f64 {
    if disable_file_header {
        0.0
    } else {
        metrics.diff_header_height
    }
}

pub fn get_virtual_file_padding_top(
    metrics: &VirtualFileMetrics,
    disable_file_header: bool,
) -> f64 {
    metrics.padding_top.unwrap_or(if disable_file_header {
        metrics.spacing
    } else {
        0.0
    })
}

pub fn get_virtual_file_padding_bottom(metrics: &VirtualFileMetrics) -> f64 {
    metrics.padding_bottom.unwrap_or(metrics.spacing)
}

pub fn get_default_hunk_separator_height(separator_type: HunkSeparators) -> f64 {
    match separator_type {
        HunkSeparators::Simple => 4.0,
        HunkSeparators::Metadata
        | HunkSeparators::LineInfo
        | HunkSeparators::LineInfoBasic
        | HunkSeparators::Custom => 32.0,
    }
}
