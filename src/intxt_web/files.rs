//! Files changed: each revision's patch per file, with comment threads where their anchors fall.

use crate::diff::Line;
use crate::view::IntxtView;

/// The files of one revision's patch.
#[derive(Debug, Clone, PartialEq)]
pub struct EntryFiles {
    /// Index into [`IntxtView::entries`].
    pub entry: usize,
    pub label: String,
    pub files: Vec<FileView>,
    /// Octets of the patch not shown (see [`crate::diff::Diff::omitted_octets`]).
    pub omitted_octets: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileView {
    pub path: String,
    pub additions: usize,
    pub deletions: usize,
    pub lines: Vec<FileLine>,
    /// Threads that point at this file without a line the patch shows: listed under the file.
    /// Indices into [`IntxtView::threads`].
    pub loose: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileLine {
    pub line: Line,
    /// Threads anchored at this line, shown after it. Indices into [`IntxtView::threads`].
    pub threads: Vec<usize>,
}

impl IntxtView {
    /// The patch of every revision that holds one, per file. A thread is anchored where one of
    /// its root's targets names this file's path and a last line that is a new-side line of the
    /// patch. The target's lines are the commenter's view of the file, and the patch shows only
    /// what changed, so a thread on a line the patch does not show is listed under the file.
    pub fn files(&self) -> Vec<EntryFiles> {
        self.revisions()
            .filter_map(|(index, entry)| {
                let diff = entry.patch.as_ref()?;
                let files = diff
                    .files()
                    .into_iter()
                    .map(|file| {
                        let mut lines: Vec<FileLine> = file
                            .lines
                            .into_iter()
                            .map(|line| FileLine {
                                line,
                                threads: Vec::new(),
                            })
                            .collect();
                        let mut loose = Vec::new();
                        for (thread_index, thread) in self.threads.iter().enumerate() {
                            let mut pointed = false;
                            let mut at_line = None;
                            for target in &thread.root.targets {
                                if target.path.as_deref() != Some(file.path.as_str()) {
                                    continue;
                                }
                                pointed = true;
                                if let Some((_, end)) = target.lines {
                                    at_line = lines
                                        .iter()
                                        .position(|held| held.line.new_line == Some(end));
                                    break;
                                }
                            }
                            match (pointed, at_line) {
                                (_, Some(at)) => lines[at].threads.push(thread_index),
                                (true, None) => loose.push(thread_index),
                                (false, None) => {}
                            }
                        }
                        FileView {
                            path: file.path,
                            additions: file.additions,
                            deletions: file.deletions,
                            lines,
                            loose,
                        }
                    })
                    .collect();
                Some(EntryFiles {
                    entry: index,
                    label: entry.label.clone(),
                    files,
                    omitted_octets: diff.omitted_octets,
                })
            })
            .collect()
    }
}

/// The lines of `path` in the patches whose new-side number is within `lines` (first to last): the
/// hunk a guide section points at.
pub fn excerpt<'a>(files: &'a [EntryFiles], path: &str, lines: (u32, u32)) -> Vec<&'a Line> {
    files
        .iter()
        .flat_map(|entry| &entry.files)
        .filter(|file| file.path == path)
        .flat_map(|file| &file.lines)
        .map(|held| &held.line)
        .filter(|line| {
            line.new_line
                .is_some_and(|number| (lines.0..=lines.1).contains(&number))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::FixtureSource;

    #[test]
    fn a_thread_sits_after_the_line_it_ends_on_or_under_the_file_when_the_patch_does_not_show_it() {
        let view = FixtureSource::view();
        let revisions = view.files();
        assert_eq!(revisions.len(), 1);
        let [ui, view_rs] = &revisions[0].files[..] else {
            panic!("the fixture patch has two files")
        };
        let thread_of = |body: &str| {
            view.threads
                .iter()
                .position(|thread| thread.root.body.starts_with(body))
                .unwrap()
        };
        // ui.rs line 14 is the added `MARKUP` constant: its thread is inline, right after it.
        let at = ui
            .lines
            .iter()
            .position(|line| !line.threads.is_empty())
            .unwrap();
        assert_eq!(ui.lines[at].line.new_line, Some(14));
        assert_eq!(ui.lines[at].threads, [thread_of("Is this constant")]);
        // Lines 200-210 are not in the patch: listed under the file.
        assert_eq!(ui.loose, [thread_of("The footer")]);
        // view.rs: the question ends on line 124, the last context line of the hunk.
        let at = view_rs
            .lines
            .iter()
            .position(|line| !line.threads.is_empty())
            .unwrap();
        assert_eq!(view_rs.lines[at].line.new_line, Some(124));
        assert!(view_rs.loose.is_empty());
        // A thread on a branch points at no file of the patch.
        assert!(
            ![ui, view_rs]
                .iter()
                .any(|file| file.loose.contains(&thread_of("Branch entries")))
        );
    }

    #[test]
    fn an_excerpt_is_the_new_side_lines_in_range() {
        let revisions = FixtureSource::view().files();
        let lines = excerpt(&revisions, "src/intxt_web/ui.rs", (11, 12));
        let texts: Vec<_> = lines.iter().map(|line| line.text.as_str()).collect();
        assert_eq!(
            texts,
            ["+use crate::view::CommitView;", "+use crate::diff::Diff;"]
        );
        assert!(excerpt(&revisions, "src/none.rs", (1, 99)).is_empty());
    }
}
