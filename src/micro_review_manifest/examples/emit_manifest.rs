//! Write the review micro's serialized manifest to the path given as the one argument: the
//! `review.manifest.json` sidecar placed beside `review.component.wasm`. STAND-IN until
//! `big_cartridges` delivers manifests; generating it from `manifest()` means the sidecar can
//! never drift from the declaration the composition root composes against.
fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: emit_manifest <output path>");
    std::fs::write(
        path,
        serde_json::to_vec(&micro_review_manifest::sidecar()).unwrap(),
    )
    .unwrap();
}
