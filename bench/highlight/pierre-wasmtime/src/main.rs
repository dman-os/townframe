//! `pierre-wasmtime <highlights.wasm> <lang-id> <file> <reps> [<highlighter.component.wasm>]`
//! Replays the ABI of @pierre/highlights/dist/highlighter.js (#run, writeInput, tokenizeLineRecords):
//!   byte 0 = lang id, byte 1 = mode (3 = UTF-16 line records), u32@2 = input length,
//!   input at 81920 NUL-terminated, call `highlight`, u32@6 = out ptr, u32@10 = out length.
//! Records are (end_utf16, highlight_id) u32 pairs; id 0xFFFFFFFF ends a line.
//! With a fifth argument, the same text also goes through the H1 wrapper component
//! (`bench/highlight/component`), called as `townframe:highlights/highlight`'s `highlight(text,
//! lang)`, and its records are compared byte for byte with the direct path's.
use std::time::Instant;
use wasmtime::Caller;
use wasmtime::Engine;
use wasmtime::Instance;
use wasmtime::Linker;
use wasmtime::Module;
use wasmtime::Store;

/// How many times Pierre asked the host for an identifier class (only non-ASCII code points).
static ID_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

const INPUT_PTR: usize = 81920;
const PAGE: usize = 65536;

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (wasm, lang, file, reps): (&str, u8, &str, usize) =
        (&a[1], a[2].parse().unwrap(), &a[3], a[4].parse().unwrap());
    let text = std::fs::read(file).unwrap();
    let mut direct_records = Vec::new();

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
            .func_wrap(
                "env",
                name,
                move |mut c: Caller<'_, ()>, ptr: i32, len: i32| -> i32 {
                    ID_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let mem = c.get_export("memory").unwrap().into_memory().unwrap();
                    let d = mem.data(&c);
                    let s = std::str::from_utf8(&d[ptr as usize..(ptr + len) as usize]).unwrap();
                    let ch = s.chars().next().unwrap();
                    (if cont {
                        unicode_ident::is_xid_continue(ch)
                    } else {
                        unicode_ident::is_xid_start(ch)
                    }) as i32
                },
            )
            .unwrap();
    }
    let instance: Instance = linker.instantiate(&mut store, &module).unwrap();
    let init_ms = t.elapsed().as_secs_f64() * 1e3;
    let mem = instance.get_memory(&mut store, "memory").unwrap();
    let highlight = instance
        .get_typed_func::<(), ()>(&mut store, "highlight")
        .unwrap();

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
        direct_records = d[out_ptr..out_ptr + out_len as usize].to_vec();
        // Touch every record like a consumer would, counting lines (terminator records).
        lines = d[out_ptr..out_ptr + out_len as usize]
            .chunks_exact(8)
            .filter(|r| u32::from_le_bytes(r[4..8].try_into().unwrap()) == u32::MAX)
            .count();
        ms.push(t.elapsed().as_secs_f64() * 1e3);
    }
    // The golden the H1 guest tests compare their rendered view with: the same lines
    // `micro_highlight_spike` renders (see its `run`), computed from the direct path's records.
    if let Some(path) = std::env::var_os("PIERRE_GOLDEN_OUT") {
        let mut digest = 0xcbf2_9ce4_8422_2325_u64;
        let mut histogram = std::collections::BTreeMap::<u32, usize>::new();
        for pair in direct_records.chunks_exact(8) {
            for byte in pair {
                digest = (digest ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
            }
            let id = u32::from_le_bytes(pair[4..8].try_into().unwrap());
            if id != u32::MAX {
                *histogram.entry(id).or_default() += 1;
            }
        }
        let tokens = histogram
            .iter()
            .map(|(id, n)| format!("{id}:{n}"))
            .collect::<Vec<_>>()
            .join(" ");
        std::fs::write(
            path,
            format!(
                "lines: {lines}\nrecords: {}\ndigest: {digest:016x}\ntokens: {tokens}\n",
                direct_records.len() / 8
            ),
        )
        .unwrap();
    }
    let component = a.get(5).map(|path| {
        use wasmtime::component::{Component, Linker};
        let text = std::str::from_utf8(&text).unwrap();
        let mut cfg = wasmtime::Config::new();
        cfg.wasm_component_model(true);
        let engine = Engine::new(&cfg).unwrap();
        let t = Instant::now();
        let component = Component::from_file(&engine, path).unwrap();
        let compile_ms = t.elapsed().as_secs_f64() * 1e3;
        let mut store = Store::new(&engine, ());
        let t = Instant::now();
        let instance = Linker::<()>::new(&engine).instantiate(&mut store, &component).unwrap();
        let init_ms = t.elapsed().as_secs_f64() * 1e3;
        let api = instance
            .get_export_index(&mut store, None, "townframe:highlights/highlight@0.1.0")
            .unwrap();
        let func = instance.get_export_index(&mut store, Some(&api), "highlight").unwrap();
        let func = instance.get_typed_func::<(&str, u8), (Vec<u32>,)>(&mut store, func).unwrap();
        let mut ms = Vec::new();
        let mut records = Vec::new();
        for _ in 0..reps {
            let t = Instant::now();
            records = func.call(&mut store, (text, lang)).unwrap().0;
            ms.push(t.elapsed().as_secs_f64() * 1e3);
        }
        let identical = records.len() * 4 == direct_records.len()
            && records.iter().zip(direct_records.chunks_exact(4)).all(|(w, b)| w.to_le_bytes() == b);
        format!(
            ",\"component\":{{\"compileMs\":{compile_ms:.1},\"initMs\":{init_ms:.2},\"recordBytes\":{},\"identical\":{identical},\"ms\":[{}]}}",
            records.len() * 4,
            ms.iter().map(|m| format!("{m:.2}")).collect::<Vec<_>>().join(",")
        )
    });
    println!(
        "{{\"engine\":\"highlights-wasmtime\",\"file\":\"{}\",\"bytes\":{},\"lines\":{lines},\"recordBytes\":{out_len},\"compileMs\":{compile_ms:.1},\"initMs\":{init_ms:.2},\"ms\":[{}],\"wasmMemMiB\":{:.1},\"idCalls\":{}{}}}",
        file.rsplit('/').next().unwrap(),
        text.len(),
        ms.iter()
            .map(|m| format!("{m:.2}"))
            .collect::<Vec<_>>()
            .join(","),
        mem.data_size(&store) as f64 / 1048576.0,
        ID_CALLS.load(std::sync::atomic::Ordering::Relaxed),
        component.unwrap_or_default()
    );
}
