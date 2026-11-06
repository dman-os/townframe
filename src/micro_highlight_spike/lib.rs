#![cfg(target_arch = "wasm32")]
//! Spike micro (H1): highlights a Rust text through the `townframe:highlights/highlight` import,
//! which the build composes (`wac plug`) with the wrapper component around Pierre's unmodified
//! `highlights.wasm`, and renders what came back: line and record counts, an FNV-1a digest of the
//! record stream (comparable with the digest of the direct path), the histogram of token ids, and
//! the guest-side time of each repetition.
use clay_mdk::Context;
use clay_mdk::Entrypoints;
use clay_mdk::Error;
use clay_mdk::MicroManifest;
use clay_mdk::PublicationDeclaration;
use clay_mdk::PublicationKind;
use dioxus::prelude::*;
use std::collections::BTreeMap;
use std::time::Instant;

wit_bindgen::generate!({
    path: "../../bench/highlight/component/wit",
    world: "client",
});

use townframe::highlights::highlight::highlight;

mod languages;

/// Pierre's language id for `name` (alias-aware, from the generated `languages.js` table).
fn language_id(name: &str) -> u8 {
    let index = languages::LANGUAGES
        .binary_search_by(|(candidate, _)| (*candidate).cmp(name))
        .unwrap_or_else(|_| panic!("unknown language {name}"));
    languages::LANGUAGES[index].1
}
const VIEW: &str = "view";
const TEXT: &str = include_str!(env!("HIGHLIGHT_SPIKE_CORPUS"));
const REPS: usize = match usize::from_str_radix(env!("HIGHLIGHT_SPIKE_REPS"), 10) {
    Ok(reps) => reps,
    Err(_) => panic!("HIGHLIGHT_SPIKE_REPS is not a number"),
};

pub fn manifest() -> MicroManifest {
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
        collection_requests: vec![],
        contributions: vec![],
        extra: Default::default(),
    }
}

async fn activate(context: Context) -> Result<(), Error> {
    context.publish_component(VIEW, view)?;
    Ok(())
}

clay_mdk::export_micro!(activate, manifest);

/// What one highlighted run produced.
#[derive(Clone)]
struct Report {
    lines: usize,
    records: usize,
    digest: u64,
    histogram: BTreeMap<u32, usize>,
    micros: Vec<u128>,
}

fn run() -> Report {
    let mut micros = Vec::with_capacity(REPS);
    let mut last = Vec::new();
    for _ in 0..REPS {
        let started = Instant::now();
        last = highlight(TEXT, language_id("rust"));
        micros.push(started.elapsed().as_micros());
    }
    // (end_utf16, token_id) pairs; the token id 0xFFFFFFFF ends a line.
    let mut digest = 0xcbf2_9ce4_8422_2325_u64;
    let mut histogram = BTreeMap::new();
    let mut lines = 0;
    for pair in last.chunks_exact(2) {
        for word in pair {
            for byte in word.to_le_bytes() {
                digest = (digest ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
        if pair[1] == u32::MAX {
            lines += 1;
        } else {
            *histogram.entry(pair[1]).or_default() += 1;
        }
    }
    Report {
        lines,
        records: last.len() / 2,
        digest,
        histogram,
        micros,
    }
}

fn view() -> Element {
    let report = use_hook(run);
    let tokens = report
        .histogram
        .iter()
        .map(|(id, count)| format!("{id}:{count}"))
        .collect::<Vec<_>>()
        .join(" ");
    let micros = report
        .micros
        .iter()
        .map(u128::to_string)
        .collect::<Vec<_>>()
        .join(",");
    rsx! {
        div {
            p { "lines: {report.lines}" }
            p { "records: {report.records}" }
            p { "digest: {report.digest:016x}" }
            p { "tokens: {tokens}" }
            p { "micros: {micros}" }
        }
    }
}
