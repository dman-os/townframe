//! Write the spike's serialized manifest to the path given as the one argument: the
//! `pierre-spike.manifest.json` sidecar that `x/dayframe-web.ts` places beside the component.
fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: emit_manifest <output path>");
    std::fs::write(
        path,
        serde_json::to_vec(&micro_pierre_spike::sidecar()).unwrap(),
    )
    .unwrap();
}
