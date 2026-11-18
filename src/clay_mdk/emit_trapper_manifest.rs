//! Write the `trapper` fixture's serialized sidecar to the path given as the one argument: the
//! `trapper.manifest.json` that `x/dayframe-packages.ts` places beside `trapper.component.wasm`
//! (the counter's `emit_manifest` does the same for the counter).

mod trapper_manifest;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: emit_trapper_manifest <output path>");
    std::fs::write(
        path,
        serde_json::to_vec(&trapper_manifest::sidecar()).unwrap(),
    )
    .unwrap();
}
