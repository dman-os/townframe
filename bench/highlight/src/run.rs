//! Shared measurement loop for the native, wasip3 and wasm-bindgen entry points.
use crate::{Engine, Lines};
use std::time::Instant;

/// Linear-memory high water in bytes on wasm (memory never shrinks, so current size is the peak),
/// VmHWM on Linux native, 0 elsewhere.
pub fn peak_memory_bytes() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        (core::arch::wasm32::memory_size(0) as u64) * 65536
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let s = std::fs::read_to_string("/proc/self/status").unwrap();
        let l = s.lines().find(|l| l.starts_with("VmHWM:")).unwrap();
        l.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap() * 1024
    }
}

pub struct Report {
    pub init_ms: f64,
    pub run_ms: Vec<f64>,
    pub lines: usize,
    pub tokens: usize,
    pub peak_bytes: u64,
    pub last: Lines,
}

pub fn bench(engine: &str, lang: &str, text: &str, reps: usize) -> Report {
    let t = Instant::now();
    let e = Engine::new(engine);
    let init_ms = t.elapsed().as_secs_f64() * 1e3;
    let mut run_ms = Vec::new();
    let mut last = Vec::new();
    for _ in 0..reps {
        drop(std::mem::take(&mut last));
        let t = Instant::now();
        last = e.tokenize(lang, text);
        run_ms.push(t.elapsed().as_secs_f64() * 1e3);
    }
    let tokens = last.iter().map(Vec::len).sum();
    Report { init_ms, run_ms, lines: last.len(), tokens, peak_bytes: peak_memory_bytes(), last }
}

impl Report {
    pub fn json(&self, engine: &str, lang: &str, size: &str, bytes: usize) -> String {
        format!(
            "{{\"engine\":\"{engine}\",\"lang\":\"{lang}\",\"size\":\"{size}\",\"bytes\":{bytes},\"lines\":{},\"tokens\":{},\"initMs\":{:.2},\"ms\":[{}],\"peakMiB\":{:.1}}}",
            self.lines,
            self.tokens,
            self.init_ms,
            self.run_ms.iter().map(|m| format!("{m:.2}")).collect::<Vec<_>>().join(","),
            self.peak_bytes as f64 / 1048576.0
        )
    }

    /// Per line `[[text, "#rrggbb", fontStyle], ...]`, the shape fidelity.mjs reads.
    pub fn dump(&self, text: &str) -> String {
        let mut out = String::from("[");
        for (i, line) in self.last.iter().enumerate() {
            if i > 0 { out.push(','); }
            out.push('[');
            for (j, t) in line.iter().enumerate() {
                if j > 0 { out.push(','); }
                let s = &text[t.start as usize..t.end as usize];
                let colour = if t.rgba == 0 { "null".to_owned() } else { format!("\"#{:06x}\"", t.rgba >> 8) };
                out.push_str(&format!("[{},{colour},{}]", serde_json::to_string(s).unwrap(), t.font_style));
            }
            out.push(']');
        }
        out.push(']');
        out
    }
}
