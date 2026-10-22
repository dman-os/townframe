fn main() {
    let manifest = plug_dayledger_manifest::plug_manifest();
    let json = serde_json::to_string_pretty(&manifest).unwrap();
    println!("{json}");
}
