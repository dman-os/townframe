//! `hlbench <engine> <lang> <file> <size-label> <reps> [dump.json]`
//! Same source builds for native, wasm32-wasip3 (run under wasmtime) and, via the cdylib, for the web.
fn main() {
    let a: Vec<String> = std::env::args().collect();
    // `hlbench probe <lang> <file> <chunk-lines>`: resumable per-window tokenization probe (irosashi).
    #[cfg(feature = "irosashi")]
    if a[1] == "probe" {
        let text = std::fs::read_to_string(&a[3]).unwrap();
        let e = highlight_bench::Engine::new(if cfg!(feature = "irosashi-full") { "irosashi" } else { "irosashi-min" });
        println!("{}", highlight_bench::probe::irosashi_chunks(e.irosashi(), &a[2], &text, a[4].parse().unwrap()));
        return;
    }
    let (engine, lang, path, size) = (&a[1], &a[2], &a[3], &a[4]);
    let reps: usize = a[5].parse().unwrap();
    let text = std::fs::read_to_string(path).unwrap();
    let rep = highlight_bench::run::bench(engine, lang, &text, reps);
    println!("{}", rep.json(engine, lang, size, text.len()));
    if let Some(dump) = a.get(6) {
        std::fs::write(dump, rep.dump(&text)).unwrap();
    }
}
