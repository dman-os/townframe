//! Replays `store_pruning` inputs through `willow25`'s `MemoryStore` and writes the results.
//!
//! This is the reference-implementation half of the conformance check: it answers "does the
//! published corpus still describe what `willow25`'s in-memory store does?", independently of
//! `big_willow`. It replays every `input` file exactly as `willow_test_vector_generation`'s
//! `single_test_vector_store_pruning` does and writes the resulting `t`/`f` string for each
//! vector into an output directory. Replaying the pinned corpus reproduces every published
//! output, which is the evidence behind the pruning semantics `big_willow` follows; see
//! `big_willow::conformance::store_pruning` and ADR 010 §6.
//!
//! It is also what to run to regenerate outputs after the corpus changes: the vector name is
//! `blake3` of the input, so replaying existing inputs keeps every name and changes only the
//! expected outputs.
//!
//! Usage: `cargo run -p big_willow --example regen_store_pruning -- <corpus> <out>`

use std::path::PathBuf;

use ufotofu::codec::Decodable;
use willow25::prelude::*;
use willow25::storage::{MemoryStore, Store};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let corpus = PathBuf::from(args.get(1).expect("usage: <corpus> <out>"));
    let out = PathBuf::from(args.get(2).expect("usage: <corpus> <out>"));

    let mut names: Vec<String> = std::fs::read_dir(corpus.join("input"))
        .expect("the corpus has an input directory")
        .map(|entry| {
            entry
                .expect("a readable directory entry")
                .file_name()
                .into_string()
                .expect("the corpus uses utf-8 file names")
        })
        .collect();
    names.sort();

    std::fs::create_dir_all(&out).expect("the output directory can be created");

    let mut changed = Vec::new();
    for name in &names {
        let bytes = std::fs::read(corpus.join("input").join(name)).expect("the input reads");
        let (count, encoded) = bytes.split_first().expect("an input starts with a count byte");

        let mut producer = ufotofu::producer::clone_from_slice(encoded);
        let mut entries = Vec::with_capacity(usize::from(*count));
        for _ in 0..*count {
            entries.push(
                AuthorisedEntry::decode(&mut producer)
                    .await
                    .expect("the corpus encodes valid entries"),
            );
        }
        assert_eq!(
            producer.offset(),
            encoded.len(),
            "{name} holds bytes past the entries it declares",
        );

        // The generator's own sequence: insert every entry, then ask whether each exact entry
        // is in the store.
        let mut store = MemoryStore::new();
        for entry in &entries {
            store.insert_entry(entry.clone()).await.expect("the store inserts");
        }

        let mut results = Vec::with_capacity(entries.len());
        for entry in &entries {
            let present = match store
                .get_entry(entry.namespace_id(), entry, None)
                .await
                .expect("the store reads")
            {
                None => false,
                Some(retrieved) => &retrieved == entry,
            };
            results.push(if present { b't' } else { b'f' });
        }

        let path = out.join(name);
        let previous = std::fs::read(corpus.join("output").join(name)).ok();
        if previous.as_deref() != Some(results.as_slice()) {
            changed.push(name.clone());
        }
        std::fs::write(&path, &results).expect("the output writes");
    }

    println!(
        "{} vectors replayed, {} outputs differ from the corpus",
        names.len(),
        changed.len(),
    );
    for name in changed.iter().take(20) {
        println!("  {name}");
    }
    if changed.len() > 20 {
        println!("  ... and {} more", changed.len() - 20);
    }
}
