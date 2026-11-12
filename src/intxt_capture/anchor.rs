//! Code targets of review comments: spelling, quoting and carrying across revisions (ADR 041
//! §2).
//!
//! A comment on code targets a path's content at a commit or an op, by 1-based lines and a W3C
//! text quote of those lines. Showing it on another revision of the same path asks where those
//! lines went, and the answer comes from git's own diff between the two blobs, not from a
//! document history:
//!
//! - lines outside every changed hunk shift by the hunks above them;
//! - lines inside a changed hunk are searched for by quote within that hunk (the span of the
//!   hunks the range touches);
//! - neither: the comment is outdated at this revision.
//!
//! Every position is verified against the quote, so a wrong diff can not place a comment on
//! other text. Which blob a path has at an op, and where a renamed path went, is the caller's:
//! this module is given blob ids.

use crate::Error;
use crate::git::Git;
use crate::revisions;
use big_cabinet_core::datum::Extra;
use intxt_core::annotation::LineRange;
use intxt_core::annotation::TextQuote;
use intxt_core::work::WorktreeOp;
use std::collections::BTreeMap;

/// How much text a quote keeps on each side of the quoted lines, as the W3C selector's context.
const CONTEXT_CHARS: usize = 32;

/// What a code target is read at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// A commit, spelled as the hex id (full or abbreviated; resolving it is the caller's).
    Commit(String),
    /// A worktree op by its sequence number, spelled `op<seq>`: the prefix keeps it from being
    /// read as an all-digit abbreviated commit id.
    Op(u64),
}

/// A parsed `<commit>:<path>:<lines>` or `op<seq>:<path>:<lines>`.
#[derive(Debug, Clone, PartialEq)]
pub struct Target {
    pub source: Source,
    pub path: String,
    /// 1-based, inclusive.
    pub lines: LineRange,
}

impl Target {
    /// `lines` is `a-b` or `a`. The path is everything between the first and the last colon, so
    /// it may contain colons.
    pub fn parse(spelling: &str) -> Result<Self, Error> {
        let bad = |reason: &str| Error::Revision(format!("target {spelling:?}: {reason}"));
        let (source, rest) = spelling
            .split_once(':')
            .ok_or_else(|| bad("expected <commit>:<path>:<lines>"))?;
        let (path, lines) = rest
            .rsplit_once(':')
            .ok_or_else(|| bad("expected <commit>:<path>:<lines>"))?;
        if path.is_empty() {
            return Err(bad("the path is empty"));
        }
        let source = match source.strip_prefix("op") {
            Some(seq) => Source::Op(
                seq.parse()
                    .map_err(|_| bad("the op is not `op` and a number"))?,
            ),
            None => {
                if !(4..=64).contains(&source.len())
                    || !source.bytes().all(|b| b.is_ascii_hexdigit())
                {
                    return Err(bad("the commit is not a hex id or `op<seq>`"));
                }
                Source::Commit(source.to_ascii_lowercase())
            }
        };
        let number = |text: &str| -> Result<u32, Error> {
            match text.parse::<u32>() {
                Ok(0) | Err(_) => Err(bad("lines are positive numbers, `a` or `a-b`")),
                Ok(n) => Ok(n),
            }
        };
        let (start, end) = match lines.split_once('-') {
            Some((start, end)) => (number(start)?, number(end)?),
            None => (number(lines)?, number(lines)?),
        };
        if end < start {
            return Err(bad("the range ends before it starts"));
        }
        let lines = LineRange {
            start,
            end,
            extra: Extra::default(),
        };
        Ok(Self {
            source,
            path: path.to_owned(),
            lines,
        })
    }
}

/// Where a quoted range ended up on another revision.
#[derive(Debug, Clone, PartialEq)]
pub enum Placement {
    /// `exact` is whether the quote's prefix and suffix hold as well as its text; when only the
    /// text does, the lines are still right but their surroundings changed.
    Placed { lines: LineRange, exact: bool },
    /// The quoted text is not in the destination where the diff says it should be.
    Outdated,
}

/// The blob id of `path` at `commit`, or none when the commit has no such path.
pub fn commit_blob(git: &Git, commit: &str, path: &str) -> Result<Option<String>, Error> {
    git.ask(&[
        "rev-parse",
        "--verify",
        "--quiet",
        &format!("{commit}:{path}"),
    ])
}

/// The first parent of `commit`, none for a root commit.
pub fn first_parent(git: &Git, commit: &str) -> Result<Option<String>, Error> {
    git.ask(&["rev-parse", "--verify", "--quiet", &format!("{commit}^")])
}

/// Where `commit`'s work is now: of `tips`, the revisions an intxt's tracked refs have reached,
/// the one `commit` is an ancestor of (or is), so a comment follows its branch forward; when
/// `commit` is no ancestor of any, because the branch was rewritten, the only tip there is; else
/// `commit` itself, which no other revision is known to supersede.
pub fn current_commit(git: &Git, commit: &str, tips: &[String]) -> Result<String, Error> {
    let mut ahead = Vec::new();
    for tip in tips {
        if revisions::is_ancestor(git, commit, tip)? {
            ahead.push(tip);
        }
    }
    let mut newest = None;
    for tip in &ahead {
        let mut is_newest = true;
        for other in &ahead {
            is_newest &= revisions::is_ancestor(git, other, tip)?;
        }
        if is_newest {
            newest = Some((*tip).clone());
        }
    }
    Ok(match (newest, ahead.is_empty(), tips) {
        (Some(tip), _, _) => tip,
        (None, true, [only]) => only.clone(),
        _ => commit.to_owned(),
    })
}

/// The blob id of `path` at op `at` of a worktree whose ops are `ops` (ADR 040 §3, content at an
/// op): the blob of the latest op at or before `at` that lists the path, none if that op deleted
/// it, and, when no such op mentions the path, the path in the tree of the `HEAD` the worktree's
/// first op observed (none when it observed none).
pub fn blob_at_op(
    git: &Git,
    ops: &BTreeMap<u64, WorktreeOp>,
    at: u64,
    path: &str,
) -> Result<Option<String>, Error> {
    for op in ops.range(..=at).rev().map(|(_, op)| op) {
        if let Some(blob) = op.files.get(path) {
            return Ok(Some(blob.clone()));
        }
        if op.deleted.iter().any(|deleted| deleted == path) {
            return Ok(None);
        }
    }
    let head = ops
        .values()
        .next()
        .and_then(|first| first.git.as_ref()?.head.as_deref());
    match head {
        Some(head) => commit_blob(git, head, path),
        None => Ok(None),
    }
}

/// What a blob holds. Binary content is an error: it has no lines to comment on.
pub fn blob_text(git: &Git, blob: &str) -> Result<String, Error> {
    String::from_utf8(git.run(&["cat-file", "blob", blob])?)
        .map_err(|_| Error::Format(format!("blob {blob} is not UTF-8 text")))
}

/// The quote of `lines` (1-based, inclusive) in `content`: the lines' text without its final
/// newline, and up to 32 characters either side.
pub fn quote(content: &str, lines: &LineRange) -> Result<TextQuote, Error> {
    let index = LineIndex::new(content);
    let (start, end) = (lines.start as usize, lines.end as usize);
    if start == 0 || end < start || end > index.count() {
        return Err(Error::Revision(format!(
            "lines {}-{} are outside the content, which has {} lines",
            lines.start,
            lines.end,
            index.count()
        )));
    }
    let (from, to) = index.span(content, start - 1, end - 1);
    let prefix: String = {
        let mut chars: Vec<char> = content[..from].chars().rev().take(CONTEXT_CHARS).collect();
        chars.reverse();
        chars.into_iter().collect()
    };
    let suffix: String = content[to..].chars().take(CONTEXT_CHARS).collect();
    Ok(TextQuote {
        exact: content[from..to].to_owned(),
        prefix: (!prefix.is_empty()).then_some(prefix),
        suffix: (!suffix.is_empty()).then_some(suffix),
        extra: Extra::default(),
    })
}

/// Where `lines` of `old_blob` are in `new_blob`, verified against `quote`.
///
/// A range no hunk touches is shifted by the net size of the hunks above it. A range a hunk
/// touches is searched for, as the quote's text, among the windows of the same number of lines
/// inside where the diff puts the range: the touched hunks' new sides with the range's
/// untouched lines beside them. Several windows with the text are narrowed
/// to those whose prefix and suffix hold too, if any, then to the one closest to where the diff
/// puts the range; two equally close ones are ambiguous and the comment is [`Placement::Outdated`]
/// rather than placed on a guess.
pub fn carry(
    git: &Git,
    old_blob: &str,
    new_blob: &str,
    lines: &LineRange,
    quote: &TextQuote,
) -> Result<Placement, Error> {
    let new = blob_text(git, new_blob)?;
    let index = LineIndex::new(&new);
    let hunks = if old_blob == new_blob {
        Vec::new()
    } else {
        hunks(git, old_blob, new_blob)?
    };
    let (start, end) = (lines.start as usize, lines.end as usize);
    let count = end - start + 1;

    let touched: Vec<&Hunk> = hunks
        .iter()
        .filter(|hunk| hunk.touches(start, end))
        .collect();
    if touched.is_empty() {
        let delta: i64 = hunks
            .iter()
            .filter(|hunk| hunk.old_end_exclusive() <= start)
            .map(|hunk| hunk.new_len as i64 - hunk.old_len as i64)
            .sum();
        let new_start = start as i64 + delta;
        if new_start < 1 {
            return Ok(Placement::Outdated);
        }
        return Ok(verify(&new, &index, new_start as usize - 1, count, quote)
            .map_or(Placement::Outdated, |exact| {
                placed(new_start as usize - 1, count, exact)
            }));
    }

    let first = touched[0];
    let last = touched[touched.len() - 1];
    // The range's own lines before the first touched hunk and after the last one are unchanged,
    // so they sit beside those hunks' new sides: the region is where the diff puts the whole
    // range. The range is expected at its first line's place, which inside a hunk is the same
    // offset into the hunk's new side.
    let head = first.old_first_line() - start.min(first.old_first_line());
    let tail = (end + 1).saturating_sub(last.old_end_exclusive());
    let region = first.new_first_line() - head..last.new_first_line() + last.new_len + tail;
    let expected = region.start + start.saturating_sub(first.old_first_line());
    let mut found: Vec<(usize, bool)> = Vec::new();
    if region.len() >= count {
        for candidate in region.start..=region.end - count {
            if let Some(exact) = verify(&new, &index, candidate, count, quote) {
                found.push((candidate, exact));
            }
        }
    }
    if found.iter().any(|(_, exact)| *exact) {
        found.retain(|(_, exact)| *exact);
    }
    let distance = |candidate: usize| candidate.abs_diff(expected);
    let Some(nearest) = found
        .iter()
        .map(|(candidate, _)| distance(*candidate))
        .min()
    else {
        return Ok(Placement::Outdated);
    };
    let mut nearest = found
        .iter()
        .filter(|(candidate, _)| distance(*candidate) == nearest);
    match (nearest.next(), nearest.next()) {
        (Some(&(candidate, exact)), None) => Ok(placed(candidate, count, exact)),
        _ => Ok(Placement::Outdated),
    }
}

fn placed(first_line: usize, count: usize, exact: bool) -> Placement {
    Placement::Placed {
        lines: LineRange {
            start: first_line as u32 + 1,
            end: (first_line + count) as u32,
            extra: Extra::default(),
        },
        exact,
    }
}

/// Whether `count` lines of `content` from the 0-based line `first` hold the quote's text:
/// none when they do not, else whether the context holds too.
fn verify(
    content: &str,
    index: &LineIndex,
    first: usize,
    count: usize,
    quote: &TextQuote,
) -> Option<bool> {
    if first + count > index.count() {
        return None;
    }
    let (from, to) = index.span(content, first, first + count - 1);
    (content[from..to] == quote.exact).then(|| {
        quote
            .prefix
            .as_deref()
            .is_none_or(|p| content[..from].ends_with(p))
            && quote
                .suffix
                .as_deref()
                .is_none_or(|s| content[to..].starts_with(s))
    })
}

/// Byte offsets of every line start.
struct LineIndex {
    starts: Vec<usize>,
}

impl LineIndex {
    fn new(content: &str) -> Self {
        let mut starts = vec![0];
        starts.extend(content.match_indices('\n').map(|(at, _)| at + 1));
        if starts.last() == Some(&content.len()) {
            // a trailing newline or an empty file starts no line
            starts.pop();
        }
        Self { starts }
    }

    fn count(&self) -> usize {
        self.starts.len()
    }

    /// The byte span of the 0-based lines `first..=last`, without the last line's newline.
    fn span(&self, content: &str, first: usize, last: usize) -> (usize, usize) {
        let end = self.starts.get(last + 1).copied().unwrap_or(content.len());
        let end = if content[..end].ends_with('\n') {
            end - 1
        } else {
            end
        };
        (self.starts[first], end)
    }
}

/// One `@@ -a,b +c,d @@` header. A side with no lines is positioned after line `start`; one
/// with lines starts at `start`.
#[derive(Debug, Clone, Copy)]
struct Hunk {
    old_start: usize,
    old_len: usize,
    new_start: usize,
    new_len: usize,
}

impl Hunk {
    /// The first old line after the hunk.
    fn old_end_exclusive(&self) -> usize {
        self.old_start + self.old_len.max(1)
    }

    /// The first old line the hunk changes, or for an insertion the line after it.
    fn old_first_line(&self) -> usize {
        if self.old_len == 0 {
            self.old_start + 1
        } else {
            self.old_start
        }
    }

    /// The 0-based first line of the hunk's new side.
    fn new_first_line(&self) -> usize {
        if self.new_len == 0 {
            self.new_start
        } else {
            self.new_start - 1
        }
    }

    /// Whether the old lines `start..=end` have a changed line, or an insertion between two of
    /// them.
    fn touches(&self, start: usize, end: usize) -> bool {
        if self.old_len == 0 {
            start <= self.old_start && self.old_start < end
        } else {
            self.old_start <= end && start < self.old_end_exclusive()
        }
    }
}

fn hunks(git: &Git, old_blob: &str, new_blob: &str) -> Result<Vec<Hunk>, Error> {
    let diff = git.run(&[
        "diff",
        "-U0",
        "--no-color",
        "--no-ext-diff",
        "--no-textconv",
        "--no-renames",
        old_blob,
        new_blob,
    ])?;
    let diff = String::from_utf8_lossy(&diff);
    let bad = |header: &str| Error::Format(format!("git diff hunk header {header:?}"));
    let range = |side: &str, header: &str| -> Result<(usize, usize), Error> {
        let side = side.get(1..).ok_or_else(|| bad(header))?;
        let (start, len) = side.split_once(',').unwrap_or((side, "1"));
        Ok((
            start.parse().map_err(|_| bad(header))?,
            len.parse().map_err(|_| bad(header))?,
        ))
    };
    let mut hunks = Vec::new();
    for header in diff.lines().filter(|line| line.starts_with("@@ ")) {
        let mut sides = header.split(' ').skip(1);
        let (old, new) = sides.next().zip(sides.next()).ok_or_else(|| bad(header))?;
        let (old_start, old_len) = range(old, header)?;
        let (new_start, new_len) = range(new, header)?;
        hunks.push(Hunk {
            old_start,
            old_len,
            new_start,
            new_len,
        });
    }
    Ok(hunks)
}

#[cfg(test)]
mod tests;
