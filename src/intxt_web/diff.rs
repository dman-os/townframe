//! A unified diff as classified lines, for a plain server-rendered `<pre>`.
//!
//! This is the viewer's stand-in until claytxt's diff view replaces it (citizenship C12): no
//! syntax highlighting, no side by side, no folding. [`Diff::parse`] only decides what class each
//! line gets and where the page stops showing lines; the component writes the lines as text nodes,
//! so the markup escapes them and a hostile diff cannot inject any.

/// The most diff text one page renders. An intxt's patch blob is up to 8 MiB, and a server-rendered
/// page of that size is not a page anyone reads; the rest is counted, not shown.
pub const MAX_RENDERED_OCTETS: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineKind {
    /// `diff --git`, `index`, `---`, `+++`, renames, mode lines: what precedes a file's hunks.
    File,
    /// `@@ -1,3 +1,4 @@ context`
    Hunk,
    Added,
    Removed,
    /// A line the hunk keeps, and `\ No newline at end of file`.
    Context,
}

impl LineKind {
    /// The CSS class the stylesheet colours the line with.
    pub fn class(self) -> &'static str {
        match self {
            Self::File => "diff-file",
            Self::Hunk => "diff-hunk",
            Self::Added => "diff-added",
            Self::Removed => "diff-removed",
            Self::Context => "diff-context",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Line {
    pub kind: LineKind,
    /// The line as written, without its newline.
    pub text: String,
    /// The line's number in the new version of the file, for the lines a hunk keeps or adds.
    pub new_line: Option<u32>,
}

/// The lines of one file of a patch, from its `diff --git` header to the next one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffFile {
    /// The new path (`b/` side of the header).
    pub path: String,
    pub lines: Vec<Line>,
    pub additions: usize,
    pub deletions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diff {
    pub lines: Vec<Line>,
    /// How many octets of the patch are not in `lines`: what the page cut off, plus whatever the
    /// reader never fetched.
    pub omitted_octets: u64,
}

impl Diff {
    /// Classifies `text`, which holds the first octets of a patch of `total_octets`. Lines are kept
    /// until [`MAX_RENDERED_OCTETS`] is reached; every octet not kept is counted in
    /// `omitted_octets`. A last line the fetch cut short is dropped, not shown half.
    pub fn parse(text: &str, total_octets: u64) -> Self {
        let complete = if (text.len() as u64) < total_octets {
            // A prefix: its last line may be cut off.
            text.rfind('\n').map_or("", |end| &text[..=end])
        } else {
            text
        };
        let mut lines = Vec::new();
        let mut kept = 0usize;
        // Whether the next line is inside a hunk, where `---`/`+++` are a removal and an addition
        // and not file headers.
        let mut in_hunk = false;
        // The new-side number of the next line a hunk keeps or adds.
        let mut next_new = 0u32;
        for line in complete.lines() {
            if kept + line.len() + 1 > MAX_RENDERED_OCTETS {
                break;
            }
            kept += line.len() + 1;
            let kind = if line.starts_with("diff ") {
                in_hunk = false;
                LineKind::File
            } else if line.starts_with("@@") {
                in_hunk = true;
                next_new = hunk_new_start(line);
                LineKind::Hunk
            } else if !in_hunk {
                LineKind::File
            } else {
                match line.as_bytes().first() {
                    Some(b'+') => LineKind::Added,
                    Some(b'-') => LineKind::Removed,
                    _ => LineKind::Context,
                }
            };
            // A `\ No newline` marker is not a line of the file.
            let new_line = match kind {
                LineKind::Added | LineKind::Context if !line.starts_with('\\') => {
                    next_new += 1;
                    Some(next_new - 1)
                }
                _ => None,
            };
            lines.push(Line {
                kind,
                text: line.to_owned(),
                new_line,
            });
        }
        Self {
            lines,
            omitted_octets: total_octets.saturating_sub(kept as u64),
        }
    }
}

/// The new-side start of a `@@ -a,b +c,d @@` header.
fn hunk_new_start(header: &str) -> u32 {
    header
        .split_whitespace()
        .find_map(|part| part.strip_prefix('+'))
        .and_then(|range| range.split(',').next())
        .and_then(|start| start.parse().ok())
        .unwrap_or(0)
}

impl Diff {
    /// The patch split per file, in patch order. Lines before the first `diff` header (a patch
    /// that is only hunks) belong to a file with no path.
    pub fn files(&self) -> Vec<DiffFile> {
        let mut files: Vec<DiffFile> = Vec::new();
        for line in &self.lines {
            if line.text.starts_with("diff ") || files.is_empty() {
                let path = line
                    .text
                    .rsplit_once(" b/")
                    .map_or("", |(_, path)| path)
                    .to_owned();
                files.push(DiffFile {
                    path,
                    lines: Vec::new(),
                    additions: 0,
                    deletions: 0,
                });
            }
            let file = files.last_mut().expect("a file was just pushed");
            match line.kind {
                LineKind::Added => file.additions += 1,
                LineKind::Removed => file.deletions += 1,
                _ => {}
            }
            file.lines.push(line.clone());
        }
        files
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PATCH: &str = "\
diff --git a/src/a.rs b/src/a.rs
index 1111111..2222222 100644
--- a/src/a.rs
+++ b/src/a.rs
@@ -1,3 +1,3 @@ fn main
 keep
-old
+new
\\ No newline at end of file
";

    fn kinds(diff: &Diff) -> Vec<LineKind> {
        diff.lines.iter().map(|line| line.kind).collect()
    }

    #[test]
    fn lines_are_classified_by_where_they_sit_in_the_file() {
        let diff = Diff::parse(PATCH, PATCH.len() as u64);
        assert_eq!(
            kinds(&diff),
            [
                LineKind::File,
                LineKind::File,
                LineKind::File,
                LineKind::File,
                LineKind::Hunk,
                LineKind::Context,
                LineKind::Removed,
                LineKind::Added,
                LineKind::Context,
            ]
        );
        assert_eq!(diff.omitted_octets, 0);
    }

    #[test]
    fn a_removed_line_that_looks_like_a_header_is_still_a_removal() {
        let patch = "diff --git a/x b/x\n@@ -1 +0,0 @@\n--- not a header\n+++ nor this\n";
        let diff = Diff::parse(patch, patch.len() as u64);
        assert_eq!(
            kinds(&diff),
            [
                LineKind::File,
                LineKind::Hunk,
                LineKind::Removed,
                LineKind::Added
            ]
        );
    }

    #[test]
    fn a_second_file_returns_to_headers() {
        let patch = "diff --git a/x b/x\n@@ -1 +1 @@\n-a\n+b\ndiff --git a/y b/y\n--- a/y\n";
        let diff = Diff::parse(patch, patch.len() as u64);
        assert_eq!(diff.lines[4].kind, LineKind::File);
        assert_eq!(diff.lines[5].kind, LineKind::File);
    }

    #[test]
    fn text_is_kept_verbatim_for_the_markup_to_escape() {
        let patch = "@@ -1 +1 @@\n+<script>alert(\"x\")</script> & more\n";
        let diff = Diff::parse(patch, patch.len() as u64);
        assert_eq!(diff.lines[1].text, "+<script>alert(\"x\")</script> & more");
    }

    #[test]
    fn a_fetch_cut_mid_line_drops_the_partial_line_and_counts_the_rest() {
        let total = PATCH.len() as u64;
        let prefix = &PATCH[..PATCH.find("-old").unwrap() + 2];
        let diff = Diff::parse(prefix, total);
        assert_eq!(diff.lines.last().unwrap().text, " keep");
        let shown: u64 = diff
            .lines
            .iter()
            .map(|line| line.text.len() as u64 + 1)
            .sum();
        assert_eq!(diff.omitted_octets, total - shown);
    }

    #[test]
    fn the_page_stops_at_the_render_cap_and_says_how_much_it_left() {
        let line = format!("+{}\n", "x".repeat(99));
        let count = MAX_RENDERED_OCTETS / 100 + 50;
        let patch = format!("@@ -0,0 +1 @@\n{}", line.repeat(count));
        let diff = Diff::parse(&patch, patch.len() as u64);
        let shown: usize = diff.lines.iter().map(|line| line.text.len() + 1).sum();
        assert!(shown <= MAX_RENDERED_OCTETS);
        assert!(diff.lines.len() < count);
        assert_eq!(diff.omitted_octets, (patch.len() - shown) as u64);
    }

    #[test]
    fn files_split_with_new_side_line_numbers() {
        let patch = "diff --git a/x.rs b/x.rs\n--- a/x.rs\n+++ b/x.rs\n@@ -5,2 +10,3 @@\n keep\n-old\n+new\n+more\n\\ No newline at end of file\ndiff --git a/y.rs b/y.rs\n@@ -1 +1 @@\n-a\n+b\n";
        let files = Diff::parse(patch, patch.len() as u64).files();
        assert_eq!(files.len(), 2);
        assert_eq!(
            (files[0].path.as_str(), files[1].path.as_str()),
            ("x.rs", "y.rs")
        );
        assert_eq!((files[0].additions, files[0].deletions), (2, 1));
        let numbers: Vec<_> = files[0].lines.iter().map(|line| line.new_line).collect();
        assert_eq!(
            numbers,
            [
                None,
                None,
                None,
                None,
                Some(10),
                None,
                Some(11),
                Some(12),
                None
            ]
        );
        assert_eq!(files[1].lines[2].new_line, None);
        assert_eq!(files[1].lines[3].new_line, Some(1));
    }
}
