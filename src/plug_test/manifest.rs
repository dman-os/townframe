fn main() {
    let manifest = plug_test::plug_manifest();
    let json = serde_json::to_string_pretty(&manifest).expect("manifest json");
    println!("{json}");
}
