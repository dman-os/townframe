//! Write the spike's serialized sidecar to the path given as the one argument, beside its
//! component, as `x/dayframe-packages.ts` does for every package.
fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: emit_manifest <output path>");
    std::fs::write(
        path,
        serde_json::to_vec(&micro_pierre_spike_scroll::sidecar()).unwrap(),
    )
    .unwrap();
}
