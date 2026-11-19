//! Records the commit and build details that `day --version` reports (shadow-rs).

fn main() {
    shadow_rs::ShadowBuilder::builder()
        .build()
        .expect("shadow-rs collects the build information");
}
