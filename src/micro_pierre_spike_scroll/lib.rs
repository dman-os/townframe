//! Pierre scroll spike (docs/scratch/pierre-spikes.md, "P2 scroll"): a micro that virtualizes a
//! synthetic plain-text file at the geometry `@pierre/diffs` uses, to find out what the Clay
//! presentation wire must carry for it. The model owns all geometry: 20 px rows, a window of
//! `ceil(ceil((viewport + 2 * overscroll) / 20) / chunk) * chunk + chunk` lines whose start is
//! quantized to `chunk`-line boundaries (`VirtualizedFile.ts:1178-1226`, `constants.ts:60-62`,
//! overscroll `Virtualizer.ts:25`), positioned by an absolutely placed window inside a content
//! box of the full file height. The DOM is read only through the `scroll` event payload.
use clay_mdk::Context;
use clay_mdk::Error;
use dioxus::html::MountedData;
use dioxus::html::ScrollBehavior;
use dioxus::html::geometry::PixelsVector2D;
use dioxus::prelude::*;
use dioxus_core::BorrowedAttributeValue;
use dioxus_core::ElementId;
use dioxus_core::WriteMutations;

pub const ID: &str = "dayframe.pierre_spike_scroll";
pub const PACKAGE_VERSION: u32 = 1;
pub const AUTHORITY: &str = "org.example.dayframe.pierre_spike_scroll";
pub const VIEW: &str = "view";
pub const PANELS_BINDING: &str = "panels";

/// `constants.ts:62` (`lineHeight`).
const LINE_HEIGHT: f64 = 20.0;
/// The window geometry profiles: `(name, overscroll px on each side of the viewport, chunk lines)`.
/// `pierre` is the file viewer's (`Virtualizer.ts:25` overscroll 1000, `constants.ts:60`
/// `hunkLineCount` 50: about 200 rows); `code-view` is the code view's (`CodeView.ts:763`
/// overscroll 200, `constants.ts:67-70` `hunkLineCount` 1: about 51 rows). The default is
/// `code-view`, the largest profile whose first window still fits the surface budgets with a
/// few spans per line (see the `budgets` test).
const PROFILES: [(&str, f64, usize); 5] = [
    ("pierre", 1000.0, 50),
    ("mid", 500.0, 10),
    ("code-view", 200.0, 1),
    // Pierre's overscroll with smaller update chunks (P4): the window start moves in 25 / 10
    // line steps, so a slow scroll updates the surface in smaller, more frequent batches.
    ("pierre-c25", 1000.0, 25),
    ("pierre-c10", 1000.0, 10),
];
const DEFAULT_PROFILE: usize = 2;

/// P3's rust and ts benchmark lines tokenized by Pierre's highlighter (`corpus/gen.mjs`):
/// `{lang: [line: [[text, color index]]]}`.
const CORPUS: &str = include_str!("corpus/tokens.json");

thread_local! {
    static TOKENS: Vec<Vec<Vec<(String, u32)>>> = {
        let parsed: serde_json::Value = serde_json::from_str(CORPUS).expect("corpus is JSON");
        ["rust", "ts"]
            .into_iter()
            .map(|lang| {
                parsed[lang]
                    .as_array()
                    .expect("corpus language")
                    .iter()
                    .map(|line| {
                        line.as_array()
                            .expect("corpus line")
                            .iter()
                            .map(|t| (t[0].as_str().expect("token text").to_owned(), t[1].as_u64().expect("color") as u32))
                            .collect()
                    })
                    .collect()
            })
            .collect()
    };
}

/// Token runs of corpus line `line`; `merged` joins whitespace-only tokens to the next token and
/// then adjacent runs of one color, as Shiki's `mergeWhitespaces` and same-style merge do.
fn corpus_runs(line: usize, merged: bool) -> Vec<(String, u32)> {
    TOKENS.with(|all| {
        let lang = &all[line % 2];
        let runs = &lang[(line / 2) % lang.len()];
        if !merged {
            return runs.clone();
        }
        let mut joined: Vec<(String, u32)> = Vec::new();
        let mut pending = String::new();
        for (text, color) in runs {
            if text.chars().all(char::is_whitespace) {
                pending.push_str(text);
            } else {
                joined.push((std::mem::take(&mut pending) + text, *color));
            }
        }
        if !pending.is_empty() {
            match joined.last_mut() {
                Some(last) => last.0.push_str(&pending),
                None => joined.push((pending, u32::MAX)),
            }
        }
        let mut out: Vec<(String, u32)> = Vec::new();
        for (text, color) in joined {
            match out.last_mut() {
                Some(last) if last.1 == color => last.0.push_str(&text),
                _ => out.push((text, color)),
            }
        }
        out
    })
}
/// The fixed height of the scroller the spike renders, until the first scroll event reports the
/// real client height.
const SCROLLER_HEIGHT: f64 = 600.0;

/// Paged-scroll rebase, after `CodeView.ts:649-690, 2983-3097`: a browser caps how tall an element
/// can be, so content taller than `REBASE_THRESHOLD` scrolls inside a box of at most
/// `REBASE_CONTAINER` px, and the model keeps `page_offset`, the logical px above that box's top.
/// Logical scrollTop is `page_offset + DOM scrollTop`. Scrolling past `REBASE_THRESHOLD` of the
/// box (or under `REBASE_TRIGGER`) moves the offset and jumps the DOM scrollTop back toward the
/// middle (`REBASE_TARGET_TOP` / its mirror), so there is always room to keep scrolling.
const REBASE_CONTAINER: f64 = 12_000_000.0;
const REBASE_TRIGGER: f64 = 1_000_000.0;
const REBASE_TARGET_TOP: f64 = 2_000_000.0;
const REBASE_TARGET_BOTTOM: f64 = REBASE_CONTAINER - REBASE_TARGET_TOP;
const REBASE_THRESHOLD: f64 = REBASE_CONTAINER - REBASE_TRIGGER;

/// Where a logical scrollTop lands in the paged box: `(DOM scrollTop, page offset)`, moving the
/// offset only when the DOM position would leave the safe band (`resolvePagedScrollPosition`,
/// `CodeView.ts:3045-3088`). `paged: false` is the unpaged layout, where the DOM is the position.
fn resolve_scroll(
    logical: f64,
    page_offset: f64,
    total_px: f64,
    viewport: f64,
    paged: bool,
) -> (f64, f64) {
    let max_total = (total_px - viewport).max(0.0);
    if !paged || max_total <= REBASE_THRESHOLD {
        return (logical.clamp(0.0, max_total), 0.0);
    }
    let max_paged = (total_px.min(REBASE_CONTAINER) - viewport).max(0.0);
    let max_offset = (max_total - max_paged).max(0.0);
    let offset = page_offset.clamp(0.0, max_offset);
    let paged_top = logical - offset;
    let down = paged_top > REBASE_THRESHOLD && offset < max_offset;
    let up = paged_top < REBASE_TRIGGER && offset > 0.0;
    if paged_top < 0.0 || paged_top > max_paged || down || up {
        let preferred = if up {
            REBASE_TARGET_BOTTOM.min(max_paged)
        } else {
            REBASE_TARGET_TOP
        };
        let mut top = preferred.clamp(0.0, max_paged);
        let mut offset = (logical - top).clamp(0.0, max_offset);
        top = (logical - offset).clamp(0.0, max_paged);
        offset = (logical - top).clamp(0.0, max_offset);
        return (top, offset);
    }
    (paged_top.clamp(0.0, max_paged), offset)
}

/// Height of the content box: the whole file, or the paged box when it is taller than the cap.
fn content_height(total_px: f64, viewport: f64, paged: bool) -> f64 {
    if paged && total_px - viewport > REBASE_THRESHOLD {
        total_px.min(REBASE_CONTAINER)
    } else {
        total_px
    }
}

pub fn manifest() -> clay_mdk::MicroManifest {
    use clay_mdk::Access;
    use clay_mdk::AttachmentDeclaration;
    use clay_mdk::BindingResolution;
    use clay_mdk::ContributionDeclaration;
    use clay_mdk::Entrypoints;
    use clay_mdk::MicroManifest;
    use clay_mdk::Necessity;
    use clay_mdk::Nsid;
    use clay_mdk::PublicationDeclaration;
    use clay_mdk::PublicationKind;
    use clay_mdk::Request;
    MicroManifest {
        entrypoints: Entrypoints::micro_client(),
        publications: vec![PublicationDeclaration {
            name: VIEW.into(),
            kind: PublicationKind::Presentation,
            contract: None,
        }],
        requires: vec![],
        configuration_requests: vec![],
        collection_contracts: vec![],
        collection_requests: vec![Request {
            binding: PANELS_BINDING.into(),
            contract: Nsid::literal(micro_layout_manifest::PANELS),
            access: Some(Access::Write),
            necessity: Necessity::Optional,
            resolution: BindingResolution::Configured,
            min_revision: None,
        }],
        contributions: vec![ContributionDeclaration {
            id: "layout-panel".into(),
            binding: PANELS_BINDING.into(),
            key: ID.into(),
            payload: serde_json::json!({
                "title": "Pierre scroll spike",
                "content": { "$attachment": 0 },
            }),
            attachments: vec![AttachmentDeclaration {
                index: 0,
                publication: VIEW.into(),
            }],
        }],
        extra: Default::default(),
    }
}

pub fn sidecar() -> clay_mdk::Sidecar {
    clay_mdk::Sidecar::new(ID, PACKAGE_VERSION, manifest()).with_authority(AUTHORITY)
}

async fn activate(context: Context) -> Result<(), Error> {
    context.publish_component(VIEW, view)?;
    Ok(())
}

clay_mdk::export_micro!(activate, manifest);

/// The window `(first line, line count)` for a scroll position: centred on the viewport, the
/// start floored to a chunk boundary, clamped to the file (`VirtualizedFile.ts:1178-1226`).
fn window(scroll_top: f64, viewport: f64, total_lines: usize, profile: usize) -> (usize, usize) {
    let center = ((scroll_top + viewport / 2.0) / LINE_HEIGHT).floor() as usize;
    window_at(center, viewport, total_lines, profile)
}

/// [`window`] for a caller that already knows which line is at the viewport's centre (W2: rows of
/// unequal height, so the line is not `y / 20`).
fn window_at(center: usize, viewport: f64, total_lines: usize, profile: usize) -> (usize, usize) {
    let (_, overscroll, chunk) = PROFILES[profile];
    let wanted = ((viewport + 2.0 * overscroll) / LINE_HEIGHT).ceil() as usize;
    let total = wanted.div_ceil(chunk) * chunk + chunk;
    if total_lines <= chunk {
        return (0, total_lines);
    }
    let center_hunk = center / chunk;
    let ideal = center_hunk as i64 - (total / chunk / 2) as i64;
    let hunks_in_file = total_lines.div_ceil(chunk) as i64;
    let start = ideal.clamp(0, hunks_in_file) as usize * chunk;
    let clamped = if ideal < 0 {
        (total as i64 + ideal * chunk as i64) as usize
    } else {
        total
    };
    (start, clamped.min(total_lines.saturating_sub(start)))
}

/// W2 (variable-height rows, `docs/scratch/pierre-spikes.md`): a 1000 line file with a "thread"
/// block of 2 to 12 lines (20 px each) after every 40th line, which the model can grow (a reply
/// arriving). The model owns all heights: `grown[k]` extra lines on thread `k`.
const THREAD_EVERY: usize = 40;
const THREAD_COUNT: usize = 25;
const THREAD_LINES: usize = THREAD_EVERY * THREAD_COUNT;

fn thread_base(k: usize) -> usize {
    2 + (k * 7) % 11
}

fn thread_height(k: usize, grown: &[usize]) -> f64 {
    (thread_base(k) + grown[k]) as f64 * LINE_HEIGHT
}

/// Content y of the top of `line` (threads sit after lines 39, 79, ...).
fn y_of_line(line: usize, grown: &[usize]) -> f64 {
    line as f64 * LINE_HEIGHT + (0..line / THREAD_EVERY).map(|k| thread_height(k, grown)).sum::<f64>()
}

/// The line at content `y` (a thread belongs to the line it follows).
fn line_at(y: f64, grown: &[usize]) -> usize {
    let mut top = 0.0;
    for block in 0..THREAD_COUNT {
        let lines_end = top + THREAD_EVERY as f64 * LINE_HEIGHT;
        if y < lines_end {
            return block * THREAD_EVERY + ((y - top) / LINE_HEIGHT).max(0.0) as usize;
        }
        top = lines_end + thread_height(block, grown);
        if y < top {
            return block * THREAD_EVERY + THREAD_EVERY - 1;
        }
    }
    THREAD_LINES - 1
}

#[component]
fn ThreadWindow(start: usize, count: usize, grown: Vec<usize>, probe: bool, log: Signal<String>) -> Element {
    rsx! {
        div {
            "data-testid": "window",
            "data-value": "{start}",
            "data-state": "{count}",
            style: "position:absolute;left:0;right:0;top:{y_of_line(start, &grown)}px",
            for line in start..start + count {
                // One root per iteration (a keyed list item): the line, then the thread after it.
                div { key: "{line}",
                    div { "data-value": "{line}",
                        Row { line, tokens: 1, source: 1 }
                    }
                    if line % THREAD_EVERY == THREAD_EVERY - 1 {
                        ThreadRow { k: line / THREAD_EVERY, lines: thread_base(line / THREAD_EVERY) + grown[line / THREAD_EVERY], probe, log }
                    }
                }
            }
        }
    }
}

#[component]
fn ThreadRow(k: usize, lines: usize, probe: bool, log: Signal<String>) -> Element {
    let body = rsx! {
        for i in 0..lines {
            div {
                key: "{i}",
                style: "height:20px;line-height:20px;font-size:12px;font-family:monospace;color:#f59e0b;white-space:pre",
                "reply {k}.{i}"
            }
        }
    };
    let style = "background:rgba(245,158,11,0.12);border-left:3px solid #f59e0b";
    if probe {
        rsx! {
            div {
                "data-state": "{k}",
                style,
                // The ResizeData the wire delivers (W2): one entry per observed size change.
                onresize: move |event| {
                    let height = event.data().get_border_box_size().map(|size| size.height);
                    log.with_mut(|text| text.push_str(&format!("{k}:{height:?};")));
                },
                {body}
            }
        }
    } else {
        rsx! { div { "data-state": "{k}", style, {body} } }
    }
}

/// Deterministic plain-text content of one line.
fn line_text(line: usize) -> String {
    let mixed = (line as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32;
    format!("let v{line} = step({}) ^ 0x{mixed:08x};", line % 97)
}

thread_local! {
    /// Total time spent inside `Row` renders (P4): the app's share of a dispatch's diff phase.
    static ROW_MS: std::cell::Cell<f64> = const { std::cell::Cell::new(0.0) };
}

struct RowTimer(std::time::Instant);

impl Drop for RowTimer {
    fn drop(&mut self) {
        ROW_MS.set(ROW_MS.get() + self.0.elapsed().as_secs_f64() * 1000.0);
    }
}

#[component]
fn Row(line: usize, tokens: usize, source: usize) -> Element {
    let _timer = RowTimer(std::time::Instant::now());
    if source != 0 {
        let runs = corpus_runs(line, source == 2);
        return rsx! {
            div { style: "height:20px;line-height:20px;white-space:pre;font-family:monospace;font-size:12px",
                span { style: "display:inline-block;width:8ch;text-align:right;opacity:0.5", "{line + 1}" }
                for (index, (text, color)) in runs.into_iter().enumerate() {
                    span { key: "{index}", style: "color:hsl({color * 37 % 360},60%,70%)", "{text}" }
                }
            }
        };
    }
    let text = line_text(line);
    rsx! {
        div { style: "height:20px;line-height:20px;white-space:pre;font-family:monospace;font-size:12px",
            span { style: "display:inline-block;width:8ch;text-align:right;opacity:0.5", "{line + 1}" }
            if tokens == 1 {
                span { "{text}" }
            } else {
                // Many tiny spans per line stand in for a highlighted row's token runs.
                for token in 0..tokens {
                    span { key: "{token}", "{text.get(token % 8..).unwrap_or(&text)} " }
                }
            }
        }
    }
}

/// The rows of the current window, positioned by the model at `start * 20` px.
#[component]
fn Window(
    start: usize,
    count: usize,
    tokens: usize,
    offset: f64,
    source: usize,
    keyed: bool,
) -> Element {
    rsx! {
        div {
            "data-testid": "window",
            "data-value": "{start}",
            "data-state": "{count}",
            style: "position:absolute;left:0;right:0;top:{(start as f64 * LINE_HEIGHT - offset).max(0.0)}px",
            if keyed {
                for line in start..start + count {
                    Row { key: "{line}", line, tokens, source }
                }
            } else {
                // Unkeyed (P4 comparison): rows match by position, so a shift re-renders every row.
                for line in start..start + count {
                    Row { line, tokens, source }
                }
            }
        }
    }
}

fn view() -> Element {
    let mut total_lines = use_signal(|| 1_000_000_usize);
    let mut tokens = use_signal(|| 1_usize);
    let mut viewport = use_signal(|| SCROLLER_HEIGHT);
    let mut profile = use_signal(|| DEFAULT_PROFILE);
    let mut paged = use_signal(|| true);
    // Row content: 0 synthetic spans, 1 corpus tokens as lexed, 2 corpus tokens merged (P4).
    let mut source = use_signal(|| 0_usize);
    let mut keyed = use_signal(|| true);
    let mut phases_text = use_signal(String::new);
    let mut bench_text = use_signal(String::new);
    let mut page_offset = use_signal(|| 0.0_f64);
    // The scroller's handle, from its `mounted` event: the guest end of the element commands.
    let mut scroller = use_signal(|| None::<std::rc::Rc<MountedData>>);
    let mut measured = use_signal(String::new);
    // W2: per-thread extra lines, how the guest reacts to a growth (0 nothing, 1 scrollTo the last
    // known position + delta, 2 read the offset then scrollTo it + delta), the scroller's
    // `overflow-anchor`, and whether thread rows observe their size.
    let mut grown = use_signal(|| vec![0_usize; THREAD_COUNT]);
    let mut correct = use_signal(|| 0_usize);
    let mut anchor = use_signal(|| true);
    let mut probe_resize = use_signal(|| false);
    let resize_log = use_signal(String::new);
    let last_top = use_hook(|| std::rc::Rc::new(std::cell::Cell::new(0.0_f64)));
    let last_top_for_grow = last_top.clone();
    let mut range = use_signal(|| window(0.0, SCROLLER_HEIGHT, 1_000_000, DEFAULT_PROFILE));

    let anchor_css = if anchor() { "auto" } else { "none" };
    let (start, count) = range();
    let total_px = total_lines() as f64 * LINE_HEIGHT;
    let total_height = if source() == 3 {
        y_of_line(THREAD_LINES, &grown())
    } else {
        content_height(total_px, viewport(), paged())
    };
    let offset_now = page_offset();
    let tokens_now = tokens();
    rsx! {
        div { style: "display:flex;flex-direction:column;gap:8px",
            div { style: "display:flex;gap:8px;flex-wrap:wrap",
                for lines in [1_000_000_usize, 3_000_000] {
                    button {
                        key: "{lines}",
                        "data-testid": "lines-{lines}",
                        onclick: move |_| {
                            total_lines.set(lines);
                            page_offset.set(0.0);
                            range.set(window(0.0, viewport(), lines, profile()));
                        },
                        "{lines} lines"
                    }
                }
                for (index, (name, _, _)) in PROFILES.into_iter().enumerate() {
                    button {
                        key: "{name}",
                        "data-testid": "profile-{name}",
                        onclick: move |_| {
                            profile.set(index);
                            range.set(window(0.0, viewport(), total_lines(), index));
                        },
                        "{name} window"
                    }
                }
                button {
                    "data-testid": "threads",
                    onclick: move |_| {
                        source.set(3);
                        total_lines.set(THREAD_LINES);
                        paged.set(false);
                        page_offset.set(0.0);
                        grown.set(vec![0; THREAD_COUNT]);
                        range.set(window(0.0, viewport(), THREAD_LINES, profile()));
                    },
                    "threads corpus"
                }
                for mode in [0_usize, 1, 2] {
                    button {
                        key: "correct-{mode}",
                        "data-testid": "correct-{mode}",
                        onclick: move |_| correct.set(mode),
                        "correct {mode}"
                    }
                }
                button { "data-testid": "anchor-on", onclick: move |_| anchor.set(true), "anchor on" }
                button { "data-testid": "anchor-off", onclick: move |_| anchor.set(false), "anchor off" }
                button { "data-testid": "resize-probe", onclick: move |_| probe_resize.set(true), "resize probe" }
                span { "data-testid": "resize-log", style: "display:none", "{resize_log}" }
                for lines in [1_usize, 4, 12] {
                    button {
                        key: "grow-{lines}",
                        "data-testid": "grow-{lines}",
                        onclick: {
                            let last_top = last_top_for_grow.clone();
                            move |_| {
                                let top = last_top.get();
                                let current = grown.peek().clone();
                                // The last thread whose block ends a row or more above the viewport.
                                let k = (0..THREAD_COUNT)
                                    .rev()
                                    .find(|&k| y_of_line((k + 1) * THREAD_EVERY, &current) <= top - LINE_HEIGHT)
                                    .expect("a thread above the viewport");
                                grown.with_mut(|g| g[k] += lines);
                                let delta = lines as f64 * LINE_HEIGHT;
                                let handle = scroller.peek().clone().expect("the scroller has mounted");
                                match *correct.peek() {
                                    0 => {}
                                    1 => {
                                        spawn(async move {
                                            handle
                                                .scroll(PixelsVector2D::new(0.0, top + delta), ScrollBehavior::Instant)
                                                .await
                                                .expect("the host corrected the scroller");
                                        });
                                    }
                                    _ => {
                                        spawn(async move {
                                            let at = handle.get_scroll_offset().await.expect("the host read the offset");
                                            handle
                                                .scroll(PixelsVector2D::new(0.0, at.y + delta), ScrollBehavior::Instant)
                                                .await
                                                .expect("the host corrected the scroller");
                                        });
                                    }
                                }
                            }
                        },
                        "grow {lines}"
                    }
                }
                for (index, name) in [(1_usize, "corpus-raw"), (2, "corpus-merged")] {
                    button {
                        key: "{name}",
                        "data-testid": "{name}",
                        onclick: move |_| source.set(index),
                        "{name}"
                    }
                }
                button {
                    "data-testid": "keyed-toggle",
                    onclick: move |_| keyed.set(!keyed()),
                    if keyed() { "keyed rows" } else { "unkeyed rows" }
                }
                button {
                    "data-testid": "phases-dump",
                    onclick: move |_| {
                        // Only the guest build has phase timings to hand over.
                        #[cfg(target_os = "wasi")]
                        phases_text.set(
                            serde_json::json!({
                                "phases": clay_mdk::guest::phases::take(),
                                "rowRenderMsTotal": ROW_MS.replace(0.0),
                            })
                            .to_string(),
                        );
                        #[cfg(not(target_os = "wasi"))]
                        phases_text.set(String::new());
                    },
                    "phases"
                }
                span { "data-testid": "phases", style: "display:none", "{phases_text}" }
                for per_line in [7_usize, 18] {
                    button {
                        key: "bench-{per_line}",
                        "data-testid": "bench-{per_line}",
                        onclick: move |_| bench_text.set(bench(per_line)),
                        "bench {per_line}"
                    }
                }
                span { "data-testid": "bench", "{bench_text}" }
                button {
                    "data-testid": "paged-toggle",
                    onclick: move |_| {
                        paged.set(!paged());
                        page_offset.set(0.0);
                    },
                    if paged() { "paged scroll: on" } else { "paged scroll: off" }
                }
                for per_line in [1_usize, 3, 7, 18, 38] {
                    button {
                        key: "{per_line}",
                        "data-testid": "tokens-{per_line}",
                        onclick: move |_| tokens.set(per_line),
                        "{per_line} spans/line"
                    }
                }
            }
            div { style: "display:flex;gap:8px;flex-wrap:wrap",
                for (name, line) in [("top", 0_usize), ("500000", 500_000), ("last", usize::MAX)] {
                    button {
                        key: "{name}",
                        "data-testid": "jump-{name}",
                        // `scrollTo` on the scroller: the guest learns the new position the same
                        // way it learns a user's, from the scroll event that follows.
                        onclick: move |_| {
                            let Some(handle) = scroller.peek().clone() else {
                                measured.set("the scroller has not announced mounted".into());
                                return;
                            };
                            // The target is a logical position: the model places it in the paged
                            // box (moving the page offset if it must) and scrolls the DOM there.
                            let total = *total_lines.peek();
                            let logical = line.min(total.saturating_sub(1)) as f64 * LINE_HEIGHT;
                            let (dom_top, offset) = resolve_scroll(
                                logical,
                                *page_offset.peek(),
                                total as f64 * LINE_HEIGHT,
                                *viewport.peek(),
                                *paged.peek(),
                            );
                            page_offset.set(offset);
                            range.set(window(logical, *viewport.peek(), total, *profile.peek()));
                            spawn(async move {
                                let scrolled = handle
                                    .scroll(
                                        PixelsVector2D::new(0.0, dom_top),
                                        ScrollBehavior::Instant,
                                    )
                                    .await;
                                // A failed command is shown, never dropped.
                                if let Err(error) = scrolled {
                                    measured.set(format!("scroll failed: {error}"));
                                }
                            });
                        },
                        "jump to line {line}"
                    }
                }
                button {
                    "data-testid": "measure",
                    onclick: move |_| {
                        let handle = scroller.peek().clone().expect("the scroller has mounted");
                        spawn(async move {
                            let rect = handle.get_client_rect().await.expect("the host read the rect");
                            let size = handle.get_scroll_size().await.expect("the host read the size");
                            let offset = handle.get_scroll_offset().await.expect("the host read the offset");
                            measured.set(format!(
                                "rect {}x{} scroll-size {}x{} scroll-offset {},{}",
                                rect.size.width, rect.size.height, size.width, size.height, offset.x, offset.y
                            ));
                        });
                    },
                    "measure"
                }
                span { "data-testid": "measured", "{measured}" }
            }
            div {
                "data-testid": "scroller",
                onmounted: move |event| scroller.set(Some(event.data())),
                style: "height:{SCROLLER_HEIGHT}px;overflow-y:auto;position:relative;border:1px solid gray;overflow-anchor:{anchor_css}",
                onscroll: move |event| {
                    let data = event.data();
                    let client = f64::from(data.client_height());
                    if client != *viewport.peek() {
                        viewport.set(client);
                    }
                    // Logical position = page offset + where the DOM is; the model alone decides
                    // when to rebase, and a rebase is a command back to the DOM.
                    let offset = *page_offset.peek();
                    let logical = offset + data.scroll_top();
                    last_top.set(logical);
                    let total = *total_lines.peek();
                    let (dom_top, next_offset) = resolve_scroll(
                        logical,
                        offset,
                        total as f64 * LINE_HEIGHT,
                        client,
                        *paged.peek(),
                    );
                    if next_offset != offset {
                        page_offset.set(next_offset);
                        if let Some(handle) = scroller.peek().clone() {
                            spawn(async move {
                                handle
                                    .scroll(PixelsVector2D::new(0.0, dom_top), ScrollBehavior::Instant)
                                    .await
                                    .expect("the host rebased the scroller");
                            });
                        }
                    }
                    let next = if *source.peek() == 3 {
                        let g = grown.peek();
                        window_at(line_at(logical + client / 2.0, &g), client, total, *profile.peek())
                    } else {
                        window(logical, client, total, *profile.peek())
                    };
                    if next != *range.peek() {
                        range.set(next);
                    }
                },
                div { style: "position:relative;height:{total_height}px",
                    if source() == 3 {
                        ThreadWindow { start, count, grown: grown(), probe: probe_resize(), log: resize_log }
                    } else {
                        Window { start, count, tokens: tokens_now, offset: offset_now, source: source(), keyed: keyed() }
                    }
                }
            }
        }
    }
}

/// Writer that drops every edit: the VirtualDom's own cost without the wire encoding.
pub(crate) struct Null;
macro_rules! null_writer {
    ($($f:ident($($a:ident: $t:ty),*)),* $(,)?) => {
        impl WriteMutations for Null {
            fn create_element(&mut self, _tag: &str, _ns: Option<&str>) {}
            fn set_attribute(
                &mut self,
                _name: &str,
                _ns: Option<&str>,
                _value: BorrowedAttributeValue<'_>,
            ) {
            }
            $(fn $f(&mut self, $($a: $t),*) {})*
        }
    };
}
null_writer!(
    push_id(id: ElementId),
    set_id(id: ElementId),
    child(index: usize),
    pop(),
    create_text(value: &str),
    clone(),
    append_children(m: usize),
    replace_with(m: usize),
    insert_after(m: usize),
    insert_before(m: usize),
    set_text(value: &str),
    add_event_listener(name: &str),
    remove_event_listener(name: &str),
    remove()
);

/// `reps` window shifts of 50 rows, each diffed into a writer that drops every edit; the diff
/// times in ms.
fn bench_diff(tokens: usize, reps: usize) -> Vec<f64> {
    use dioxus_core::ScopeId;
    use dioxus_core::VirtualDom;
    thread_local! {
        static BENCH_PROPS: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
    }
    fn bench_app() -> Element {
        let (start, tokens) = BENCH_PROPS.get();
        rsx! { Window { start, count: 140, tokens, offset: 0.0, source: 0, keyed: true } }
    }
    let mut diffs = Vec::new();
    for rep in 0..reps {
        BENCH_PROPS.set((100 + rep * 100, tokens));
        let mut dom = VirtualDom::new(bench_app);
        dom.rebuild(&mut Null);
        BENCH_PROPS.set((150 + rep * 100, tokens));
        dom.mark_dirty(ScopeId::APP);
        let started = std::time::Instant::now();
        dom.render_immediate(&mut Null);
        diffs.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    diffs
}

/// What the guest's own VirtualDom costs with no wire in the loop (P4): a 50-row window shift
/// diffed into a writer that drops every edit, median of 20, and the cost of one host call
/// (`Instant::now` is a `wasi:clocks` import). Run in the guest, this separates the diff's
/// compute from the bridge.
fn bench(tokens: usize) -> String {
    let mut diffs = bench_diff(tokens, 20);
    diffs.sort_by(f64::total_cmp);
    let started = std::time::Instant::now();
    for _ in 0..200 {
        std::hint::black_box(std::time::Instant::now());
    }
    let per_call = started.elapsed().as_secs_f64() * 1000.0 / 200.0;
    // Plain integer work, then small allocations: wasm's raw speed, then the allocator's.
    let started = std::time::Instant::now();
    let mut acc = 0_u64;
    for i in 0..20_000_000_u64 {
        acc = acc.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(i);
    }
    std::hint::black_box(acc);
    let compute = started.elapsed().as_secs_f64() * 1000.0;
    let started = std::time::Instant::now();
    let mut strings = Vec::new();
    for i in 0..100_000 {
        strings.push(format!("row {i}"));
    }
    std::hint::black_box(&strings);
    drop(strings);
    let alloc = started.elapsed().as_secs_f64() * 1000.0;
    // Linear memory access without the allocator, then held boxes, then alloc/free pairs.
    let started = std::time::Instant::now();
    let mut words = vec![0_u32; 4_000_000];
    for (i, w) in words.iter_mut().enumerate() {
        *w = i as u32;
    }
    std::hint::black_box(words.iter().map(|w| u64::from(*w)).sum::<u64>());
    let memory = started.elapsed().as_secs_f64() * 1000.0;
    let started = std::time::Instant::now();
    let held: Vec<Box<u64>> = (0..100_000_u64).map(Box::new).collect();
    std::hint::black_box(&held);
    let boxes = started.elapsed().as_secs_f64() * 1000.0;
    drop(held);
    let started = std::time::Instant::now();
    for i in 0..100_000_u64 {
        std::hint::black_box(Box::new(i));
    }
    let pairs = started.elapsed().as_secs_f64() * 1000.0;
    // Calls with a stack frame, then thread-local access.
    #[inline(never)]
    fn framed(i: u64) -> u64 {
        let mut frame = [i; 32];
        frame[(i % 32) as usize] ^= 1;
        std::hint::black_box(&mut frame);
        frame.iter().sum()
    }
    let started = std::time::Instant::now();
    let mut acc = 0_u64;
    for i in 0..1_000_000_u64 {
        acc = acc.wrapping_add(framed(i));
    }
    std::hint::black_box(acc);
    let frames = started.elapsed().as_secs_f64() * 1000.0;
    #[inline(never)]
    fn leaf(i: u64) -> u64 {
        i.wrapping_mul(3).wrapping_add(1)
    }
    let started = std::time::Instant::now();
    let mut acc = 0_u64;
    for i in 0..1_000_000_u64 {
        acc = acc.wrapping_add(leaf(std::hint::black_box(i)));
    }
    std::hint::black_box(acc);
    let leaves = started.elapsed().as_secs_f64() * 1000.0;
    thread_local! { static TLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) }; }
    let started = std::time::Instant::now();
    for _ in 0..1_000_000 {
        TLS.set(TLS.get() + 1);
    }
    let tls = started.elapsed().as_secs_f64() * 1000.0;
    format!(
        "leaf-calls-1M-ms {leaves:.1} framed-calls-1M-ms {frames:.1} tls-1M-ms {tls:.1} mem-4M-ms {memory:.1} held-100k-boxes-ms {boxes:.1} alloc-free-pairs-100k-ms {pairs:.1} ",
    ) + &format!(
        "diff-no-writer-median-ms {:.2} host-call-ms {:.4} int-loop-20M-ms {compute:.1} alloc-100k-strings-ms {alloc:.1}",
        diffs[10], per_call
    )
}

/// P5 control: the same `bench` as a plain core export, so the unbundled core module can run it
/// in V8 with no wcmp. `p5_init` installs a TLS block the way `clay_mdk` does at its first task
/// hook (`__wasm_init_tls` copies `.tdata` and calls the imported `__wasm_set_tls_base`);
/// `p5_bench` leaks its report, `p5_len` is that report's length.
#[cfg(all(target_os = "wasi", feature = "p5-direct"))]
mod p5_direct {
    use core::sync::atomic::AtomicUsize;
    use core::sync::atomic::Ordering;
    static LEN: AtomicUsize = AtomicUsize::new(0);

    unsafe extern "C" {
        fn __wasm_init_tls(base: *mut u8);
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn p5_init() {
        let block = Box::leak(vec![0_u8; 4096].into_boxed_slice());
        unsafe { __wasm_init_tls(block.as_mut_ptr().add(8 - (block.as_ptr() as usize % 8))) };
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn p5_bench(tokens: u32) -> *const u8 {
        let report = super::bench(tokens as usize).into_boxed_str();
        LEN.store(report.len(), Ordering::Relaxed);
        Box::leak(report).as_ptr()
    }

    /// One 50-row window shift diffed into the null writer, the same work as one rep of
    /// `bench`'s diff; the harness counts stack-pointer imports around this call.
    #[unsafe(no_mangle)]
    pub extern "C" fn p5_diff(tokens: u32) {
        super::bench_diff(tokens as usize, 1);
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn p5_len() -> usize {
        LEN.load(Ordering::Relaxed)
    }
}

/// What one window commit costs against the surface tree's budgets (`clay_surface` `tree.rs`),
/// measured by replaying the Dioxus diff the guest would ship through `SurfaceTree::apply`: the
/// same validation the host runs on every `surface.apply`.
#[cfg(all(test, not(target_os = "wasi")))]
mod budgets {
    use super::Null;
    use super::*;
    use clay_surface::AttributeValue;
    use clay_surface::Mutation;
    use clay_surface::SurfaceTree;
    use dioxus_core::BorrowedAttributeValue;
    use dioxus_core::ElementId;
    use dioxus_core::ScopeId;
    use dioxus_core::VirtualDom;
    use dioxus_core::WriteMutations;
    use std::cell::Cell;

    /// Dioxus `WriteMutations` to wire `Mutation`s, one to one (the guest's `WireMutations`).
    #[derive(Default)]
    struct Recorder(Vec<Mutation>);

    impl WriteMutations for Recorder {
        fn push_id(&mut self, id: ElementId) {
            self.0.push(Mutation::PushId(id.raw() as u32));
        }
        fn set_id(&mut self, id: ElementId) {
            self.0.push(Mutation::SetId(id.raw() as u32));
        }
        fn child(&mut self, index: usize) {
            self.0.push(Mutation::Child(index as u32));
        }
        fn pop(&mut self) {
            self.0.push(Mutation::Pop);
        }
        fn create_element(&mut self, tag: &str, ns: Option<&str>) {
            self.0
                .push(Mutation::CreateElement((tag.into(), ns.map(Into::into))));
        }
        fn create_text(&mut self, value: &str) {
            self.0.push(Mutation::CreateText(value.into()));
        }
        fn clone(&mut self) {
            self.0.push(Mutation::CloneNode);
        }
        fn append_children(&mut self, m: usize) {
            self.0.push(Mutation::AppendChildren(m as u32));
        }
        fn replace_with(&mut self, m: usize) {
            self.0.push(Mutation::ReplaceWith(m as u32));
        }
        fn insert_after(&mut self, m: usize) {
            self.0.push(Mutation::InsertAfter(m as u32));
        }
        fn insert_before(&mut self, m: usize) {
            self.0.push(Mutation::InsertBefore(m as u32));
        }
        fn set_attribute(
            &mut self,
            name: &str,
            ns: Option<&str>,
            value: BorrowedAttributeValue<'_>,
        ) {
            let value = match value {
                BorrowedAttributeValue::Text(text) => AttributeValue::Text(text.into()),
                BorrowedAttributeValue::Float(float) => AttributeValue::Float(float),
                BorrowedAttributeValue::Int(int) => AttributeValue::Int(int),
                BorrowedAttributeValue::Bool(bool) => AttributeValue::Boolean(bool),
                BorrowedAttributeValue::None => AttributeValue::None,
                BorrowedAttributeValue::Any(_) => unreachable!("`Any` never crosses the wire"),
            };
            self.0.push(Mutation::SetAttribute((
                name.into(),
                ns.map(Into::into),
                value,
            )));
        }
        fn set_text(&mut self, value: &str) {
            self.0.push(Mutation::SetText(value.into()));
        }
        fn add_event_listener(&mut self, name: &str) {
            self.0.push(Mutation::AddEventListener(name.into()));
        }
        fn remove_event_listener(&mut self, name: &str) {
            self.0.push(Mutation::RemoveEventListener(name.into()));
        }
        fn remove(&mut self) {
            self.0.push(Mutation::Remove);
        }
    }

    thread_local! {
        static PROPS: Cell<(usize, usize, usize)> = const { Cell::new((0, 0, 1)) };
    }

    fn app() -> Element {
        let (start, count, tokens) = PROPS.get();
        rsx! { Window { start, count, tokens, offset: 0.0, source: 0, keyed: true } }
    }

    /// One committed batch against a fresh tree, or the first window and then a move to the
    /// next one. Returns, per commit, `(mutations, retained nodes, result)`.
    fn commits(
        first: (usize, usize, usize),
        next: Option<(usize, usize, usize)>,
    ) -> Vec<(usize, usize, Result<u64, String>)> {
        PROPS.set(first);
        let mut dom = VirtualDom::new(app);
        let mut tree = SurfaceTree::<()>::new();
        let mut out = Vec::new();
        let mut commit = |recorder: Recorder, tree: &mut SurfaceTree<()>| {
            let result = tree.apply(&recorder.0).map_err(|error| error.to_string());
            out.push((recorder.0.len(), tree.node_count(), result));
        };
        let mut recorder = Recorder::default();
        dom.rebuild(&mut recorder);
        commit(recorder, &mut tree);
        if let Some(next) = next {
            PROPS.set(next);
            dom.mark_dirty(ScopeId::APP);
            let mut recorder = Recorder::default();
            dom.render_immediate(&mut recorder);
            commit(recorder, &mut tree);
        }
        out
    }

    /// The largest first window that commits, for `tokens` spans per line.
    fn max_first_window(tokens: usize) -> usize {
        (1..=400)
            .take_while(|&count| commits((0, count, tokens), None)[0].2.is_ok())
            .last()
            .unwrap_or(0)
    }

    /// Native split of one 50-row window shift (P4): the VirtualDom with no writer, then with the
    /// recording writer. Meaningful under `--release`.
    #[test]
    fn print_diff_split() {
        for tokens in [7, 18] {
            let mut times = [Vec::new(), Vec::new()];
            for (slot, recording) in [false, true].into_iter().enumerate() {
                for rep in 0..20 {
                    PROPS.set((100 + rep * 100, 140, tokens));
                    let mut dom = VirtualDom::new(app);
                    dom.rebuild(&mut Recorder::default());
                    PROPS.set((150 + rep * 100, 140, tokens));
                    dom.mark_dirty(ScopeId::APP);
                    let started = std::time::Instant::now();
                    if recording {
                        dom.render_immediate(&mut Recorder::default());
                    } else {
                        dom.render_immediate(&mut Null);
                    }
                    times[slot].push(started.elapsed().as_secs_f64() * 1000.0);
                }
                times[slot].sort_by(f64::total_cmp);
            }
            println!(
                "SPLIT tokens={tokens} native 50-row shift: no-writer median {:.2} ms, recording median {:.2} ms",
                times[0][10], times[1][10]
            );
        }
    }

    #[test]
    fn print_window_budgets() {
        for tokens in [1, 3, 7, 18, 38] {
            let max = max_first_window(tokens);
            let per_row =
                (commits((0, 6, tokens), None)[0].0 - commits((0, 2, tokens), None)[0].0) / 4;
            let nodes_per_row =
                (commits((0, 6, tokens), None)[0].1 - commits((0, 2, tokens), None)[0].1) / 4;
            let pierre = &commits((0, 200, tokens), None)[0];
            // One chunk move (50 lines) of the largest window that commits at all.
            let shift = commits((100, max.max(1), tokens), Some((150, max.max(1), tokens)));
            println!(
                "BUDGET tokens={tokens} mutations_per_row={per_row} nodes_per_row={nodes_per_row} \
                 max_first_window_rows={max} pierre200: mutations={} result={:?} \
                 shift50_of_{max}: mutations={} nodes_after={} result={:?}",
                pierre.0, pierre.2, shift[1].0, shift[1].1, shift[1].2,
            );
        }
    }
}

#[cfg(test)]
mod paged {
    use super::*;

    #[test]
    fn unpaged_and_short_files_are_the_identity() {
        assert_eq!(
            resolve_scroll(500.0, 0.0, 1_000_000.0, 600.0, true),
            (500.0, 0.0)
        );
        assert_eq!(resolve_scroll(15e6, 0.0, 20e6, 600.0, false), (15e6, 0.0));
    }

    #[test]
    fn crossing_the_threshold_moves_the_offset_and_keeps_the_logical_position() {
        let total = 60e6;
        let (top, offset) = resolve_scroll(11.5e6, 0.0, total, 598.0, true);
        assert_eq!((top, offset), (2e6, 9.5e6));
        assert_eq!(top + offset, 11.5e6);
        // Up past the trigger rebases the other way, landing at the mirrored target.
        let (top, offset) = resolve_scroll(offset + 500_000.0, offset, total, 598.0, true);
        assert_eq!(top + offset, 10_000_000.0);
        assert_eq!(top, 10e6 - 0.0 - offset);
        assert!(top > 9e6);
    }

    #[test]
    fn the_end_of_the_file_is_reachable() {
        let total = 60e6;
        let (top, offset) = resolve_scroll(total - 598.0, 0.0, total, 598.0, true);
        assert_eq!(top + offset, total - 598.0);
        assert!(top <= REBASE_CONTAINER - 598.0);
    }
}
