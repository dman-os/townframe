//! Picks the text the micro highlights and how many times: `HIGHLIGHT_SPIKE_CORPUS` (a path,
//! default the committed ~1k-line Rust snippet) and `HIGHLIGHT_SPIKE_REPS` (default 1). The
//! measurement builds point the first at the 10k/100k-line corpus files of `bench/highlight`.
fn main() {
    println!("cargo:rerun-if-env-changed=HIGHLIGHT_SPIKE_CORPUS");
    println!("cargo:rerun-if-env-changed=HIGHLIGHT_SPIKE_REPS");
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let corpus = std::env::var("HIGHLIGHT_SPIKE_CORPUS")
        .unwrap_or_else(|_| format!("{manifest}/snippet.rs.txt"));
    println!("cargo:rerun-if-changed={corpus}");
    println!("cargo:rustc-env=HIGHLIGHT_SPIKE_CORPUS={corpus}");
    println!(
        "cargo:rustc-env=HIGHLIGHT_SPIKE_REPS={}",
        std::env::var("HIGHLIGHT_SPIKE_REPS").unwrap_or_else(|_| "1".into())
    );
}
