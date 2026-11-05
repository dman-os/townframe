//! `pierre-wasmtime <highlights.wasm> <lang-id> <file> <reps>`
//! Replays the ABI of @pierre/highlights/dist/highlighter.js (#run, writeInput, tokenizeLineRecords):
//!   byte 0 = lang id, byte 1 = mode (3 = UTF-16 line records), u32@2 = input length,
//!   input at 81920 NUL-terminated, call `highlight`, u32@6 = out ptr, u32@10 = out length.
//! Records are (end_utf16, highlight_id) u32 pairs; id 0xFFFFFFFF ends a line.
use std::time::Instant;
use wasmtime::{Caller, Engine, Instance, Linker, Module, Store};

const INPUT_PTR: usize = 81920;
const PAGE: usize = 65536;

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (wasm, lang, file, reps): (&str, u8, &str, usize) = (&a[1], a[2].parse().unwrap(), &a[3], a[4].parse().unwrap());
    let text = std::fs::read(file).unwrap();

    let mut cfg = wasmtime::Config::new();
    cfg.wasm_simd(true);
    let engine = Engine::new(&cfg).unwrap();
    let t = Instant::now();
    let module = Module::from_file(&engine, wasm).unwrap();
    let compile_ms = t.elapsed().as_secs_f64() * 1e3;

    let t = Instant::now();
    let mut store = Store::new(&engine, ());
    let mut linker = Linker::new(&engine);
    // ID_Start / ID_Continue for the code point encoded at ptr (len bytes of UTF-8), same contract
    // as highlighter.js #idClass.
    for (name, cont) in [("is_id_start", false), ("is_id_continue", true)] {
        linker
            .func_wrap("env", name, move |mut c: Caller<'_, ()>, ptr: i32, len: i32| -> i32 {
                let mem = c.get_export("memory").unwrap().into_memory().unwrap();
                let d = mem.data(&c);
                let s = std::str::from_utf8(&d[ptr as usize..(ptr + len) as usize]).unwrap();
                let ch = s.chars().next().unwrap();
                (if cont { unicode_ident::is_xid_continue(ch) } else { unicode_ident::is_xid_start(ch) }) as i32
            })
            .unwrap();
    }
    let instance: Instance = linker.instantiate(&mut store, &module).unwrap();
    let init_ms = t.elapsed().as_secs_f64() * 1e3;
    let mem = instance.get_memory(&mut store, "memory").unwrap();
    let highlight = instance.get_typed_func::<(), ()>(&mut store, "highlight").unwrap();

    let mut ms = Vec::new();
    let (mut out_len, mut lines) = (0u32, 0usize);
    for _ in 0..reps {
        let t = Instant::now();
        let need = (INPUT_PTR + text.len() + 96).div_ceil(PAGE);
        let have = mem.size(&store) as usize;
        if need > have {
            mem.grow(&mut store, (need - have) as u64).unwrap();
        }
        let d = mem.data_mut(&mut store);
        d[INPUT_PTR..INPUT_PTR + text.len()].copy_from_slice(&text);
        d[INPUT_PTR + text.len()] = 0;
        d[0] = lang;
        d[1] = 3;
        d[2..6].copy_from_slice(&(text.len() as u32).to_le_bytes());
        highlight.call(&mut store, ()).unwrap();
        let d = mem.data(&store);
        let out_ptr = u32::from_le_bytes(d[6..10].try_into().unwrap()) as usize;
        out_len = u32::from_le_bytes(d[10..14].try_into().unwrap());
        // Touch every record like a consumer would, counting lines (terminator records).
        lines = d[out_ptr..out_ptr + out_len as usize]
            .chunks_exact(8)
            .filter(|r| u32::from_le_bytes(r[4..8].try_into().unwrap()) == u32::MAX)
            .count();
        ms.push(t.elapsed().as_secs_f64() * 1e3);
    }
    println!(
        "{{\"engine\":\"highlights-wasmtime\",\"file\":\"{}\",\"bytes\":{},\"lines\":{lines},\"recordBytes\":{out_len},\"compileMs\":{compile_ms:.1},\"initMs\":{init_ms:.2},\"ms\":[{}],\"wasmMemMiB\":{:.1}}}",
        file.rsplit('/').next().unwrap(),
        text.len(),
        ms.iter().map(|m| format!("{m:.2}")).collect::<Vec<_>>().join(","),
        mem.data_size(&store) as f64 / 1048576.0
    );
}
