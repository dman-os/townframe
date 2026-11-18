//! Sample file pairs for the Diffs tab. Each pair is the "before" text and an ordered list of
//! edits that make the "after" text, so the diff is reviewable at the edit list instead of two
//! near-identical walls of text. An edit's `from` must occur exactly once.

use pierre_diffs_dioxus::FileContents;

const RUST_BEFORE: &str = r#"use std::collections::HashMap;
use std::time::Duration;

/// A least-recently-used cache with a fixed capacity.
pub struct Lru<K, V> {
    capacity: usize,
    tick: u64,
    entries: HashMap<K, (u64, V)>,
}

impl<K: std::hash::Hash + Eq + Clone, V> Lru<K, V> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "an empty cache holds nothing");
        Self {
            capacity,
            tick: 0,
            entries: HashMap::with_capacity(capacity),
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.tick += 1;
        let tick = self.tick;
        self.entries.get_mut(key).map(|(used, value)| {
            *used = tick;
            &*value
        })
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.tick += 1;
        if self.entries.len() == self.capacity && !self.entries.contains_key(&key) {
            self.evict_oldest();
        }
        self.entries.insert(key, (self.tick, value));
    }

    fn evict_oldest(&mut self) {
        let oldest = self
            .entries
            .iter()
            .min_by_key(|(_, (used, _))| *used)
            .map(|(key, _)| key.clone());
        if let Some(oldest) = oldest {
            self.entries.remove(&oldest);
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// How long a rendered thumbnail stays valid.
pub const THUMBNAIL_TTL: Duration = Duration::from_secs(60);

pub struct Thumbnails {
    cache: Lru<String, Vec<u8>>,
}

impl Thumbnails {
    pub fn new() -> Self {
        Self {
            cache: Lru::new(64),
        }
    }

    pub fn fetch(&mut self, path: &str, render: impl FnOnce() -> Vec<u8>) -> &[u8] {
        if self.cache.get(&path.to_owned()).is_none() {
            let bytes = render();
            self.cache.insert(path.to_owned(), bytes);
        }
        self.cache
            .get(&path.to_owned())
            .map(Vec::as_slice)
            .expect("just inserted")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_the_least_recently_used() {
        let mut cache = Lru::new(2);
        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.get(&"a");
        cache.insert("c", 3);
        assert!(cache.get(&"b").is_none());
        assert_eq!(cache.get(&"a"), Some(&1));
    }

    #[test]
    fn reinserting_a_key_does_not_evict() {
        let mut cache = Lru::new(1);
        cache.insert("a", 1);
        cache.insert("a", 2);
        assert_eq!(cache.len(), 1);
    }
}
"#;

const RUST_EDITS: &[(&str, &str)] = &[
    (
        "use std::time::Duration;\n",
        "use std::time::Duration;\nuse std::time::Instant;\n",
    ),
    (
        "    tick: u64,\n    entries: HashMap<K, (u64, V)>,\n}",
        "    tick: u64,\n    evictions: u64,\n    entries: HashMap<K, (u64, V)>,\n}",
    ),
    (
        "            tick: 0,\n            entries",
        "            tick: 0,\n            evictions: 0,\n            entries",
    ),
    (
        "        if let Some(oldest) = oldest {\n            self.entries.remove(&oldest);\n        }",
        "        if let Some(oldest) = oldest {\n            self.entries.remove(&oldest);\n            self.evictions += 1;\n        }",
    ),
    (
        "    pub fn len(&self) -> usize {",
        "    /// How many entries capacity pressure has dropped so far.\n    pub fn evictions(&self) -> u64 {\n        self.evictions\n    }\n\n    pub fn len(&self) -> usize {",
    ),
    (
        "pub struct Thumbnails {\n    cache: Lru<String, Vec<u8>>,\n}",
        "pub struct Thumbnails {\n    cache: Lru<String, (Instant, Vec<u8>)>,\n}",
    ),
    (
        "        if self.cache.get(&path.to_owned()).is_none() {\n            let bytes = render();\n            self.cache.insert(path.to_owned(), bytes);\n        }\n        self.cache\n            .get(&path.to_owned())\n            .map(Vec::as_slice)\n            .expect(\"just inserted\")",
        "        let stale = self\n            .cache\n            .get(&path.to_owned())\n            .is_none_or(|(rendered, _)| rendered.elapsed() > THUMBNAIL_TTL);\n        if stale {\n            self.cache\n                .insert(path.to_owned(), (Instant::now(), render()));\n        }\n        self.cache\n            .get(&path.to_owned())\n            .map(|(_, bytes)| bytes.as_slice())\n            .expect(\"just inserted\")",
    ),
    (
        "        assert_eq!(cache.len(), 1);\n    }",
        "        assert_eq!(cache.len(), 1);\n        assert_eq!(cache.evictions(), 0);\n    }",
    ),
];

const TS_BEFORE: &str = r#"import { readFileSync } from "node:fs";

export interface Annotation {
  line: number;
  author: string;
  body: string;
}

export interface Review {
  file: string;
  annotations: Annotation[];
}

const SEVERITY_ORDER = ["note", "warning", "error"] as const;
type Severity = (typeof SEVERITY_ORDER)[number];

export function severityOf(body: string): Severity {
  if (body.startsWith("error:")) return "error";
  if (body.startsWith("warning:")) return "warning";
  return "note";
}

export function loadReview(path: string): Review {
  const raw = JSON.parse(readFileSync(path, "utf8"));
  return {
    file: String(raw.file),
    annotations: raw.annotations.map((entry: any) => ({
      line: Number(entry.line),
      author: String(entry.author),
      body: String(entry.body),
    })),
  };
}

export function groupByLine(review: Review): Map<number, Annotation[]> {
  const groups = new Map<number, Annotation[]>();
  for (const annotation of review.annotations) {
    const group = groups.get(annotation.line) ?? [];
    group.push(annotation);
    groups.set(annotation.line, group);
  }
  return groups;
}

export function worstSeverity(annotations: Annotation[]): Severity {
  let worst = 0;
  for (const annotation of annotations) {
    worst = Math.max(worst, SEVERITY_ORDER.indexOf(severityOf(annotation.body)));
  }
  return SEVERITY_ORDER[worst];
}

export function summarize(review: Review): string {
  const groups = groupByLine(review);
  const lines = [...groups.keys()].sort((a, b) => a - b);
  return lines
    .map((line) => `${review.file}:${line} ${worstSeverity(groups.get(line)!)}`)
    .join("\n");
}
"#;

const TS_EDITS: &[(&str, &str)] = &[
    (
        "export interface Annotation {\n  line: number;\n  author: string;\n  body: string;\n}",
        "export interface Annotation {\n  line: number;\n  author: string;\n  body: string;\n  resolved?: boolean;\n}",
    ),
    (
        "      body: String(entry.body),\n    })),",
        "      body: String(entry.body),\n      resolved: Boolean(entry.resolved),\n    })),",
    ),
    (
        "  for (const annotation of review.annotations) {\n    const group",
        "  for (const annotation of review.annotations) {\n    if (annotation.resolved) continue;\n    const group",
    ),
    (
        "  return SEVERITY_ORDER[worst];\n}",
        "  return SEVERITY_ORDER[worst];\n}\n\nexport function unresolvedCount(review: Review): number {\n  return review.annotations.filter((annotation) => !annotation.resolved).length;\n}",
    ),
];

fn file(name: &str, contents: String) -> Box<FileContents> {
    Box::new(FileContents {
        name: name.to_owned(),
        contents,
        lang: None,
        header: None,
        cache_key: None,
    })
}

fn pair(
    name: &str,
    before: &str,
    edits: &[(&str, &str)],
) -> (Box<FileContents>, Box<FileContents>) {
    let after = edits.iter().fold(before.to_owned(), |text, (from, to)| {
        assert_eq!(
            text.matches(from).count(),
            1,
            "fixture edit must match exactly once: {from:?}"
        );
        text.replace(from, to)
    });
    (file(name, before.to_owned()), file(name, after))
}

/// A Rust file with eight scattered edits.
pub fn rust_pair() -> (Box<FileContents>, Box<FileContents>) {
    pair("src/thumbnails.rs", RUST_BEFORE, RUST_EDITS)
}

/// A TypeScript file with four edits.
pub fn ts_pair() -> (Box<FileContents>, Box<FileContents>) {
    pair("review/annotations.ts", TS_BEFORE, TS_EDITS)
}
