//! Step-4 probe: can a guest tokenize incrementally inside a dispatch budget, resumable per window?
//! Uses irosashi's per-line `Session` API (explicit `StateStack` handle) to:
//!  1. tokenize the whole file in chunks of `chunk_lines`, timing every chunk (budget = max/p95 chunk);
//!  2. keep a state checkpoint every `chunk_lines` lines, then tokenize a 200-line window at several
//!     offsets (a) resuming from the nearest checkpoint, (b) from line 0, to price "windowed" work.
//! Theme resolution is excluded on purpose: the Session API yields scope-stack tokens, and the theme
//! lookup is a memoised per-scope-stack table, so it is not the cost being probed.
use irosashi::{TokenizeOptions, split_lines};
use std::time::Instant;

pub fn irosashi_chunks(hl: &irosashi::Highlighter, lang: &str, text: &str, chunk_lines: usize) -> String {
    let lang = match lang { "ts" => "typescript", "md" => "markdown", o => o };
    let mut session = hl.session(lang).unwrap();
    let ranges = split_lines(text);
    let opts = TokenizeOptions::default();

    let mut state = session.initial_state();
    let mut checkpoints = vec![state.clone()]; // checkpoints[i] = state before line i*chunk_lines
    let mut chunk_ms = Vec::new();
    let mut tokens = 0usize;
    let t_all = Instant::now();
    for chunk in ranges.chunks(chunk_lines) {
        let t = Instant::now();
        for (k, r) in chunk.iter().enumerate() {
            let first = checkpoints.len() == 1 && k == 0;
            let res = session.tokenize_line(&text[r.clone()], &state, first, opts);
            tokens += res.tokens.len();
            state = res.state;
        }
        chunk_ms.push(t.elapsed().as_secs_f64() * 1e3);
        checkpoints.push(state.clone());
    }
    let total_ms = t_all.elapsed().as_secs_f64() * 1e3;

    // Window probe: 200 lines starting at ~10 evenly spread offsets.
    let window = 200usize;
    let n = ranges.len();
    let offsets: Vec<usize> = (1..=10).map(|i| (n.saturating_sub(window)) * i / 10 / chunk_lines * chunk_lines).collect();
    let (mut from_ckpt, mut from_zero) = (Vec::new(), Vec::new());
    for &off in &offsets {
        let t = Instant::now();
        let mut st = checkpoints[off / chunk_lines].clone();
        for (i, r) in ranges[off..(off + window).min(n)].iter().enumerate() {
            st = session.tokenize_line(&text[r.clone()], &st, off == 0 && i == 0, opts).state;
        }
        from_ckpt.push(t.elapsed().as_secs_f64() * 1e3);
        let t = Instant::now();
        let mut st = session.initial_state();
        for (i, r) in ranges[..(off + window).min(n)].iter().enumerate() {
            st = session.tokenize_line(&text[r.clone()], &st, i == 0, opts).state;
        }
        from_zero.push(t.elapsed().as_secs_f64() * 1e3);
    }
    let pct = |v: &[f64], p: f64| { let mut s = v.to_vec(); s.sort_by(|a, b| a.partial_cmp(b).unwrap()); s[((s.len() - 1) as f64 * p).round() as usize] };
    let med = |v: &[f64]| pct(v, 0.5);
    format!(
        "{{\"probe\":\"irosashi-session\",\"lines\":{n},\"chunkLines\":{chunk_lines},\"tokens\":{tokens},\"totalMs\":{total_ms:.1},\"chunkMs\":{{\"p50\":{:.2},\"p95\":{:.2},\"max\":{:.2},\"n\":{}}},\"windowLines\":{window},\"windowFromCheckpointMs\":{{\"median\":{:.2},\"max\":{:.2}}},\"windowFromZeroMs\":{{\"median\":{:.2},\"max\":{:.2}}},\"checkpoints\":{},\"peakMiB\":{:.1}}}",
        pct(&chunk_ms, 0.5), pct(&chunk_ms, 0.95), pct(&chunk_ms, 1.0), chunk_ms.len(),
        med(&from_ckpt), pct(&from_ckpt, 1.0), med(&from_zero), pct(&from_zero, 1.0),
        checkpoints.len(), crate::run::peak_memory_bytes() as f64 / 1048576.0
    )
}
