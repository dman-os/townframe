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
            lines.push(Line {
                kind,
                text: line.to_owned(),
            });
        }
        Self {
            lines,
            omitted_octets: total_octets.saturating_sub(kept as u64),
        }
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
}
