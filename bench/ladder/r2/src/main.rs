//! R2: the ladder workload (`bench/ladder/common.js`, the Pierre spike micro's rows and window
//! geometry) as a plain Dioxus web app. The `Row` / `Window` components are the micro's
//! (`src/micro_pierre_spike_scroll/lib.rs`), so the VirtualDom diff is the same work; what changes
//! is everything around it: no guest wasm, no Clay wire, no wcmp, dioxus-web's own renderer.
use dioxus::prelude::*;

const ROW: f64 = 20.0;
const CHUNK: usize = 50;
const OVERSCROLL: f64 = 1000.0;
const TOTAL_LINES: usize = 8000;
const SCROLLER_HEIGHT: f64 = 600.0;
const CORPUS: &str = include_str!("../../../../src/micro_pierre_spike_scroll/corpus/tokens.json");

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

#[derive(Clone, Copy, PartialEq)]
enum Workload {
    Syn(usize),
    Corpus,
    CorpusRust,
}

/// `window()` of the micro, profile `pierre`.
fn window(scroll_top: f64, viewport: f64) -> (usize, usize) {
    let wanted = ((viewport + 2.0 * OVERSCROLL) / ROW).ceil() as usize;
    let total = wanted.div_ceil(CHUNK) * CHUNK + CHUNK;
    let center = ((scroll_top + viewport / 2.0) / ROW).floor() as usize;
    let center_hunk = center / CHUNK;
    let ideal = center_hunk as i64 - (total / CHUNK / 2) as i64;
    let hunks_in_file = TOTAL_LINES.div_ceil(CHUNK) as i64;
    let start = ideal.clamp(0, hunks_in_file) as usize * CHUNK;
    let clamped = if ideal < 0 { (total as i64 + ideal * CHUNK as i64) as usize } else { total };
    (start, clamped.min(TOTAL_LINES.saturating_sub(start)))
}

fn line_text(line: usize) -> String {
    let mixed = (line as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32;
    format!("let v{line} = step({}) ^ 0x{mixed:08x};", line % 97)
}

fn corpus_runs(workload: Workload, line: usize) -> Vec<(String, u32)> {
    TOKENS.with(|all| match workload {
        Workload::Corpus => {
            let lang = &all[line % 2];
            lang[(line / 2) % lang.len()].clone()
        }
        _ => all[0][line % 1000].clone(),
    })
}

#[component]
fn Row(line: usize, workload: Workload) -> Element {
    if let Workload::Syn(tokens) = workload {
        let text = line_text(line);
        return rsx! {
            div { style: "height:20px;line-height:20px;white-space:pre;font-family:monospace;font-size:12px",
                span { style: "display:inline-block;width:8ch;text-align:right;opacity:0.5", "{line + 1}" }
                for token in 0..tokens {
                    span { key: "{token}", "{text.get(token % 8..).unwrap_or(&text)} " }
                }
            }
        };
    }
    let runs = corpus_runs(workload, line);
    rsx! {
        div { style: "height:20px;line-height:20px;white-space:pre;font-family:monospace;font-size:12px",
            span { style: "display:inline-block;width:8ch;text-align:right;opacity:0.5", "{line + 1}" }
            for (index, (text, color)) in runs.into_iter().enumerate() {
                span { key: "{index}", style: "color:hsl({color * 37 % 360},60%,70%)", "{text}" }
            }
        }
    }
}

#[component]
fn Window(start: usize, count: usize, workload: Workload) -> Element {
    rsx! {
        div {
            "data-testid": "window",
            "data-value": "{start}",
            "data-state": "{count}",
            style: "position:absolute;left:0;right:0;top:{start as f64 * ROW}px",
            for line in start..start + count {
                Row { key: "{line}", line, workload }
            }
        }
    }
}

fn workload_from_url() -> Workload {
    let search = web_sys::window().expect("window").location().search().expect("search");
    let value = search.trim_start_matches('?').split('&').find_map(|kv| kv.strip_prefix("workload=")).unwrap_or("syn7").to_owned();
    match value.as_str() {
        "syn7" => Workload::Syn(7),
        "syn18" => Workload::Syn(18),
        "corpus" => Workload::Corpus,
        "corpus-rust" => Workload::CorpusRust,
        other => panic!("unknown workload {other}"),
    }
}

fn app() -> Element {
    let workload = use_hook(workload_from_url);
    let mut range = use_signal(|| window(0.0, SCROLLER_HEIGHT));
    let (start, count) = range();
    rsx! {
        div {
            "data-testid": "scroller",
            style: "height:{SCROLLER_HEIGHT}px;overflow-y:auto;position:relative;border:1px solid gray",
            onscroll: move |event| {
                let data = event.data();
                let next = window(data.scroll_top(), f64::from(data.client_height()));
                if next != *range.peek() {
                    range.set(next);
                }
            },
            div { style: "position:relative;height:{TOTAL_LINES as f64 * ROW}px",
                Window { start, count, workload }
            }
        }
    }
}

fn main() {
    dioxus::launch(app);
}
