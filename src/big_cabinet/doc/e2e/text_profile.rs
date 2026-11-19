//! Print-only wall time per phase of a large Automerge Text datum: a create, then one-line edits
//! (each `AtHeads` on the previous receipt's heads, or `AtCurrent`), then a read. The default is
//! 200 lines so normal runs stay cheap; `TEXTPROF_LINES=4000` (~216 KB) is the real run, in release:
//! `TEXTPROF_LINES=4000 cargo nextest run -p big_cabinet --release text_profile --no-capture`.

use super::*;
use std::time::Instant;

fn text_key() -> DatumKey {
    format!("{TEXT_NOTE}/main").parse().unwrap()
}

fn text_note(text: &str) -> Value {
    json!({"$type": TEXT_NOTE, "text": text})
}

/// How an edit rewrites the middle line.
#[derive(Clone, Copy)]
enum Edit {
    /// Replaces it with a much shorter line, the way a note's line is cut down.
    Shorten,
    /// Appends to it.
    Grow,
}

async fn phase_times(edits: &[(&str, bool, Edit)]) {
    let lines_n: usize = std::env::var("TEXTPROF_LINES").map_or(200, |n| n.parse().unwrap());
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let mut lines: Vec<String> = (0..lines_n)
        .map(|i| format!("line {i:05} of a generated file, long enough to add up"))
        .collect();
    let bytes = lines.join("\n").len();
    println!("TEXTPROF text = {lines_n} lines, {bytes} bytes");
    let start = Instant::now();
    let created = node
        .create(
            &alice,
            "create",
            [(text_key(), text_note(&lines.join("\n")))],
        )
        .await
        .unwrap();
    println!("TEXTPROF create wall {:?}", start.elapsed());
    let doc_id = created.doc_id;
    // The creation heads equal the current heads until the first edit lands.
    let mut heads = created.heads;
    for (label, at_heads, edit) in edits {
        let line = lines_n / 2;
        lines[line] = match edit {
            Edit::Shorten => format!("line {line:05} EDITED"),
            Edit::Grow => format!("{} // {label}", lines[line]),
        };
        let mode = if *at_heads {
            WriteMode::AtHeads {
                heads: heads.clone(),
            }
        } else {
            WriteMode::AtCurrent
        };
        let joined = lines.join("\n");
        let start = Instant::now();
        let receipt = node
            .write(
                &alice,
                &doc_id,
                mode,
                [(text_key(), text_note(&joined))],
                [],
            )
            .await
            .unwrap();
        println!("TEXTPROF {label} wall {:?}", start.elapsed());
        heads = receipt.heads;
    }
    let start = Instant::now();
    let read = node
        .read_one(&alice, &doc_id, At::Current, &text_key())
        .await;
    println!("TEXTPROF read wall {:?}", start.elapsed());
    assert_eq!(accepted(&read).0["text"], json!(lines.join("\n")));
    node.stop().await;
}

/// The order of an agent that read the document and edits from its heads: the first edit is
/// `AtHeads` on the creation heads and shortens the line.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_text_edited_at_its_creation_heads() {
    phase_times(&[
        ("edit 1 AtHeads(creation) shorten", true, Edit::Shorten),
        ("edit 2 AtCurrent grow", false, Edit::Grow),
        ("edit 3 AtHeads(receipt) grow", true, Edit::Grow),
    ])
    .await;
}

/// The same edits with `AtCurrent` first, to tell what a first edit costs from what `AtHeads` does.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_text_edited_at_current_first() {
    phase_times(&[
        ("edit 1 AtCurrent grow", false, Edit::Grow),
        ("edit 2 AtHeads(receipt) shorten", true, Edit::Shorten),
        ("edit 3 AtHeads(receipt) grow", true, Edit::Grow),
    ])
    .await;
}
