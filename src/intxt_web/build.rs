//! Embeds the client build in the component.
//!
//! `INTXT_WEB_CLIENT_DIR` names the directory `x/intxt-web.ts` wrote the hydrating client into
//! (`intxt_web.js`, `intxt_web_bg.wasm` and wasm-bindgen's `snippets`); every file under it is
//! compiled in with `include_bytes!` and served under `/assets/`. Unset, the table is empty: tests
//! and the host build need no client, and the server still renders every page.

use std::fmt::Write as _;
use std::path::Path;
use std::path::PathBuf;

fn collect(root: &Path, directory: &Path, found: &mut Vec<PathBuf>) {
    println!("cargo:rerun-if-changed={}", directory.display());
    for entry in std::fs::read_dir(directory).expect("the client directory is readable") {
        let path = entry.expect("a directory entry").path();
        if path.is_dir() {
            collect(root, &path, found);
        } else {
            found.push(path.strip_prefix(root).expect("under the root").to_owned());
        }
    }
}

fn main() {
    println!("cargo:rerun-if-env-changed=INTXT_WEB_CLIENT_DIR");
    println!("cargo:rerun-if-changed=build.rs");
    let mut table = String::from("pub static CLIENT_ASSETS: &[(&str, &[u8])] = &[\n");
    if let Some(root) = std::env::var_os("INTXT_WEB_CLIENT_DIR") {
        let root = std::fs::canonicalize(root).expect("INTXT_WEB_CLIENT_DIR exists");
        let mut files = Vec::new();
        collect(&root, &root, &mut files);
        files.sort();
        for file in files {
            let name = file.to_str().expect("a utf-8 file name");
            let absolute = root.join(&file);
            writeln!(
                table,
                "    ({name:?}, include_bytes!({:?})),",
                absolute.to_str().expect("a utf-8 path")
            )
            .expect("write to a string");
        }
    }
    table.push_str("];\n");
    let out = PathBuf::from(std::env::var_os("OUT_DIR").expect("cargo sets OUT_DIR"));
    std::fs::write(out.join("client_assets.rs"), table).expect("OUT_DIR is writable");
}
