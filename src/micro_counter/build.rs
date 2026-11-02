//! Embed the counter's manifest as a `dayframe.manifest` custom section (package identity
//! lives with the component bytes; see `clay_mdk::package`). The section content is
//! generated as a byte-array static here because a build script cannot rewrite the final
//! component — the linker carries the `link_section` static through componentization.
use std::fmt::Write as _;

fn main() {
    let manifest = micro_counter_manifest::manifest();
    let json = serde_json::to_vec(&manifest).unwrap();
    let mut bytes = String::new();
    for byte in &json {
        write!(bytes, "{byte},").unwrap();
    }
    let generated = format!(
        "#[used]\n#[unsafe(link_section = \"dayframe.manifest\")]\nstatic DAYFRAME_MANIFEST: [u8; {}] = [{bytes}];\n",
        json.len()
    );
    let out_dir = std::env::var("OUT_DIR").unwrap();
    std::fs::write(format!("{out_dir}/dayframe_manifest.rs"), generated).unwrap();
    println!("cargo:rerun-if-changed=build.rs");
}
