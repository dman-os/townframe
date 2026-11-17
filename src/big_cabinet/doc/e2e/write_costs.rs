//! Per-write cost of a signed datum write: a one-line edit to an Automerge Text field, written
//! through the repository (gate read, dmeta, claim and signature), against the same splice applied
//! to a plain Automerge document. The difference is what every signed write adds.
//!
//! The pin: the cost of an edit does not grow with the datum's history. The default run rewrites one
//! fixed line (line 100 of 200) 30 times, changing its content each time, and asserts that the median
//! allocations of edits 6-10 (1-based) are within 1.25x of the median of edits 26-30, at two sizes of
//! other datums. The line is fixed on purpose: the cost of an edit tracks the text's size and the
//! edit's position (the diff over the full text), so walking the line would confound history with
//! position. Allocations, not wall time, because a count is stable on a shared runner. They are
//! counted across all threads, so background tasks add a little noise.
//!
//! A second, print-only axis reports the per-edit allocations of an edit at line 10 against line 190
//! of the same text. `WRITE_COSTS_EDITS` and `WRITE_COSTS_DATUMS` (comma-separated) override the
//! sizes for a release run, which only prints:
//! `WRITE_COSTS_EDITS=1000 WRITE_COSTS_DATUMS=0,100 cargo nextest run -p big_cabinet --release write_costs --no-capture`.

use super::list_costs::all_allocations;
use super::*;
use automerge::AutoCommit;
use automerge::ObjType;
use automerge::transaction::Transactable;
use std::time::Duration;
use std::time::Instant;

const LINES: usize = 200;
/// The line every edit of the history pin rewrites.
const PINNED_LINE: usize = 100;

fn text_key() -> DatumKey {
    format!("{TEXT_NOTE}/main").parse().unwrap()
}

fn text_note(text: &str) -> Value {
    json!({"$type": TEXT_NOTE, "text": text})
}

/// Bytes, wall time and allocations of one batch of edits.
#[derive(Default)]
struct Tally {
    write_bytes: usize,
    plain_bytes: usize,
    wall: Duration,
    allocations: u64,
    edits: usize,
}

impl Tally {
    fn line(&self, label: &str) -> String {
        let n = self.edits.max(1);
        format!(
            "{label}: write {:.0} B/edit, plain splice {:.0} B/edit, overhead {:.0} B/edit, {:.1} ms/edit, {} allocs/edit",
            self.write_bytes as f64 / n as f64,
            self.plain_bytes as f64 / n as f64,
            (self.write_bytes as f64 - self.plain_bytes as f64) / n as f64,
            self.wall.as_secs_f64() * 1000.0 / n as f64,
            self.allocations / n as u64,
        )
    }
}

/// Rewrites `line` `edits` times and prints the report; returns the allocations of each edit, in
/// order.
async fn edit_costs(slots: usize, edits: usize, line: usize) -> Vec<u64> {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let mut lines: Vec<String> = (0..LINES)
        .map(|i| format!("    let value_{i} = compute_{i}(input, {i});"))
        .collect();
    let mut datums: Vec<(DatumKey, Value)> = (0..slots)
        .map(|i| (key(&format!("n{i:05}")), note("first")))
        .collect();
    datums.push((text_key(), text_note(&lines.join("\n"))));
    let doc_id = node.create(&alice, "create", datums).await.unwrap().doc_id;

    let mut plain = AutoCommit::new();
    let text = plain
        .put_object(automerge::ROOT, "text", ObjType::Text)
        .unwrap();
    plain.splice_text(&text, 0, 0, &lines.join("\n")).unwrap();
    plain.commit();

    let mut saved_before = 0;
    node.sabotage(&doc_id, |doc| saved_before = doc.save().len())
        .await;
    let plain_saved_before = plain.save().len();

    let deciles = 10;
    let mut tallies: Vec<Tally> = (0..deciles).map(|_| Tally::default()).collect();
    let mut per_edit = Vec::with_capacity(edits);
    for edit in 0..edits {
        let old_len = lines[line].chars().count();
        lines[line] =
            format!("    let value_{line} = compute_{line}(input, {edit}); // edit {edit}");
        let offset: usize = lines[..line].iter().map(|l| l.chars().count() + 1).sum();
        let tally = &mut tallies[edit * deciles / edits];

        let plain_heads = plain.get_heads();
        plain
            .splice_text(&text, offset, old_len as isize, &lines[line])
            .unwrap();
        plain.commit();
        tally.plain_bytes += plain
            .get_changes(&plain_heads)
            .iter()
            .map(|change| change.raw_bytes().len())
            .sum::<usize>();

        let heads = node.sabotage(&doc_id, |_| ()).await;
        let joined = lines.join("\n");
        let allocations = all_allocations();
        let start = Instant::now();
        node.write(
            &alice,
            &doc_id,
            WriteMode::AtCurrent,
            [(text_key(), text_note(&joined))],
            [],
        )
        .await
        .unwrap();
        tally.wall += start.elapsed();
        let spent = all_allocations() - allocations;
        per_edit.push(spent);
        tally.allocations += spent;
        node.sabotage(&doc_id, |doc| {
            tally.write_bytes += doc
                .get_changes(&heads)
                .iter()
                .map(|change| change.raw_bytes().len())
                .sum::<usize>();
        })
        .await;
        tally.edits += 1;
    }

    let read = node
        .read_one(&alice, &doc_id, At::Current, &text_key())
        .await;
    assert_eq!(accepted(&read).0["text"], json!(lines.join("\n")));
    let mut saved_after = 0;
    node.sabotage(&doc_id, |doc| saved_after = doc.save().len())
        .await;
    let plain_saved_after = plain.save().len();
    node.stop().await;

    let mut total = Tally::default();
    for tally in &tallies {
        total.write_bytes += tally.write_bytes;
        total.plain_bytes += tally.plain_bytes;
        total.wall += tally.wall;
        total.allocations += tally.allocations;
        total.edits += tally.edits;
    }
    println!(
        "== {slots} other datums, {edits} one-line edits to line {line} of a {LINES}-line text datum"
    );
    println!("{}", total.line("all"));
    println!("{}", tallies[0].line("first 10%"));
    println!("{}", tallies[deciles - 1].line("last 10%"));
    println!(
        "saved doc {saved_before} -> {saved_after} B (+{:.0} B/edit); plain doc {plain_saved_before} -> {plain_saved_after} B (+{:.0} B/edit)",
        (saved_after - saved_before) as f64 / edits as f64,
        (plain_saved_after - plain_saved_before) as f64 / edits as f64,
    );
    per_edit
}

fn median(mut values: Vec<u64>) -> u64 {
    values.sort_unstable();
    values[values.len() / 2]
}

#[tokio::test(flavor = "multi_thread")]
async fn a_one_line_edit_costs_what_the_signed_write_adds() {
    let overridden =
        std::env::var("WRITE_COSTS_EDITS").is_ok() || std::env::var("WRITE_COSTS_DATUMS").is_ok();
    let edits: usize =
        std::env::var("WRITE_COSTS_EDITS").map_or(30, |edits| edits.parse().unwrap());
    let datums: Vec<usize> = std::env::var("WRITE_COSTS_DATUMS").map_or_else(
        |_| vec![0, 100],
        |list| {
            list.split(',')
                .map(|slots| slots.parse().unwrap())
                .collect()
        },
    );
    for slots in datums {
        let per_edit = edit_costs(slots, edits, PINNED_LINE).await;
        if overridden {
            continue;
        }
        let early = median(per_edit[5..10].to_vec());
        let late = median(per_edit[25..30].to_vec());
        println!(
            "{slots} other datums: median allocs of edits 6-10 = {early}, of edits 26-30 = {late}"
        );
        assert!(
            late as f64 <= early as f64 * 1.25,
            "{slots} other datums: median allocations of edits 26-30 ({late}) exceed 1.25x the median of edits 6-10 ({early}): the cost of an edit grows with history",
        );
    }

    if overridden {
        return;
    }
    // Position axis, print-only: the diff over the full text makes an edit near the end cost more than
    // one near the start. The first edits of each run are skipped as warm-up.
    let near_start = median(edit_costs(0, 8, 10).await[3..8].to_vec());
    let near_end = median(edit_costs(0, 8, 190).await[3..8].to_vec());
    println!(
        "position axis: median allocs of an edit at line 10 = {near_start}, at line 190 = {near_end} ({:.2}x)",
        near_end as f64 / near_start as f64
    );
}
