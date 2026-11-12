//! Real git repositories in temporary directories, driven by the git CLI.

use super::*;
use crate::testing::Repo;

fn git(repo: &Repo) -> Git {
    Git::new(&repo.root)
}

/// Lines `a1`..`a<n>`, one per line.
fn numbered(prefix: &str, count: usize) -> String {
    (1..=count).map(|n| format!("{prefix}{n}\n")).collect()
}

fn blob(repo: &Repo, commit: &str, path: &str) -> String {
    commit_blob(&git(repo), commit, path).unwrap().unwrap()
}

/// Quote `lines` of `path` at `old`, carry to `new`, and say whether the quote's context held.
fn placement(repo: &Repo, old: &str, new: &str, path: &str, lines: LineRange) -> Placement {
    let old = blob(repo, old, path);
    let new = blob(repo, new, path);
    let text = blob_text(&git(repo), &old).unwrap();
    let quote = quote(&text, &lines).unwrap();
    carry(&git(repo), &old, &new, &lines, &quote).unwrap()
}

fn lr(start: u32, end: u32) -> LineRange {
    LineRange {
        start,
        end,
        extra: Extra::default(),
    }
}

/// Where the lines went, ignoring whether their context survived: none when outdated.
fn carried(repo: &Repo, old: &str, new: &str, path: &str, lines: LineRange) -> Option<LineRange> {
    lines_of(placement(repo, old, new, path, lines))
}

fn lines_of(placement: Placement) -> Option<LineRange> {
    match placement {
        Placement::Placed { lines, .. } => Some(lines),
        Placement::Outdated => None,
    }
}

#[test]
fn targets_parse() {
    assert_eq!(
        Target::parse("AbC1234:src/a.rs:3-5").unwrap(),
        Target {
            source: Source::Commit("abc1234".into()),
            path: "src/a.rs".into(),
            lines: lr(3, 5),
        }
    );
    assert_eq!(
        Target::parse("op12:a:b.txt:7").unwrap(),
        Target {
            source: Source::Op(12),
            path: "a:b.txt".into(),
            lines: lr(7, 7),
        }
    );
    // An all-digit abbreviated commit is a commit, not an op.
    assert_eq!(
        Target::parse("1234567:f:1").unwrap().source,
        Source::Commit("1234567".into())
    );
}

#[test]
fn bad_targets_are_refused() {
    for spelling in [
        "",
        "abc1234",
        "abc1234:a.rs",
        "abc1234::3",
        "abc1234:a.rs:",
        "abc1234:a.rs:0",
        "abc1234:a.rs:5-3",
        "abc1234:a.rs:x",
        "abc1234:a.rs:1-2-3",
        "abc:a.rs:1",
        "xyz1234:a.rs:1",
        "opx:a.rs:1",
        "op:a.rs:1",
    ] {
        assert!(
            matches!(Target::parse(spelling), Err(Error::Revision(_))),
            "{spelling:?}"
        );
    }
}

#[test]
fn quotes_carry_up_to_32_characters_of_context() {
    let content = numbered("line", 30);
    let first = quote(&content, &lr(1, 1)).unwrap();
    assert_eq!(first.exact, "line1");
    assert_eq!(first.prefix, None);
    assert_eq!(
        first.suffix.as_deref(),
        Some("\nline2\nline3\nline4\nline5\nline6\nl")
    );

    let last = quote(&content, &lr(30, 30)).unwrap();
    assert_eq!(last.exact, "line30");
    assert_eq!(last.prefix.as_deref().map(|p| p.chars().count()), Some(32));
    assert!(last.prefix.as_deref().unwrap().ends_with("line29\n"));
    // The final newline is not part of the quote: it is the whole suffix.
    assert_eq!(last.suffix.as_deref(), Some("\n"));

    let span = quote(&content, &lr(2, 3)).unwrap();
    assert_eq!(span.exact, "line2\nline3");
    assert_eq!(span.prefix.as_deref(), Some("line1\n"));
}

#[test]
fn a_last_line_without_a_newline_quotes_whole() {
    let quoted = quote("a\nb", &lr(2, 2)).unwrap();
    assert_eq!(quoted.exact, "b");
    assert_eq!(quoted.prefix.as_deref(), Some("a\n"));
    assert_eq!(quoted.suffix, None);
}

#[test]
fn quotes_outside_the_content_are_refused() {
    assert!(matches!(
        quote("a\nb\n", &lr(3, 3)),
        Err(Error::Revision(_))
    ));
    assert!(matches!(
        quote("a\nb\n", &lr(2, 3)),
        Err(Error::Revision(_))
    ));
    assert!(matches!(quote("", &lr(1, 1)), Err(Error::Revision(_))));
}

#[test]
fn an_unchanged_file_keeps_its_lines() {
    let repo = Repo::new();
    repo.write("f.txt", numbered("a", 10));
    let one = repo.commit_all("one");
    repo.write("other.txt", "x\n");
    let two = repo.commit_all("two");
    assert_eq!(
        placement(&repo, &one, &two, "f.txt", lr(4, 6)),
        Placement::Placed {
            lines: lr(4, 6),
            exact: true
        }
    );
}

#[test]
fn lines_below_an_insertion_shift_and_lines_above_stay() {
    let repo = Repo::new();
    repo.write("f.txt", numbered("a", 10));
    let one = repo.commit_all("one");
    repo.write(
        "f.txt",
        format!(
            "a1\na2\nnew1\nnew2\nnew3\n{}",
            numbered("a", 10)
                .split_once("a3\n")
                .unwrap()
                .1
                .to_owned()
                .replacen("a4", "a3\na4", 1)
        ),
    );
    let two = repo.commit_all("two");
    // a1..a2 are above the insertion at line 3; a3 and below are pushed down by 3.
    assert_eq!(
        carried(&repo, &one, &two, "f.txt", lr(1, 2)),
        Some(lr(1, 2))
    );
    assert_eq!(
        carried(&repo, &one, &two, "f.txt", lr(3, 4)),
        Some(lr(6, 7))
    );
    assert_eq!(
        carried(&repo, &one, &two, "f.txt", lr(10, 10)),
        Some(lr(13, 13))
    );
}

#[test]
fn lines_below_a_deletion_shift_up() {
    let repo = Repo::new();
    repo.write("f.txt", numbered("a", 10));
    let one = repo.commit_all("one");
    repo.write("f.txt", "a1\na2\na6\na7\na8\na9\na10\n");
    let two = repo.commit_all("two");
    assert_eq!(
        carried(&repo, &one, &two, "f.txt", lr(7, 8)),
        Some(lr(4, 5))
    );
}

#[test]
fn a_range_with_an_insertion_inside_it_is_outdated() {
    let repo = Repo::new();
    repo.write("f.txt", numbered("a", 6));
    let one = repo.commit_all("one");
    repo.write("f.txt", "a1\na2\na3\nnew\na4\na5\na6\n");
    let two = repo.commit_all("two");
    assert_eq!(carried(&repo, &one, &two, "f.txt", lr(2, 5)), None);
    // Either side of the insertion still places.
    assert_eq!(
        carried(&repo, &one, &two, "f.txt", lr(2, 3)),
        Some(lr(2, 3))
    );
    assert_eq!(
        carried(&repo, &one, &two, "f.txt", lr(4, 5)),
        Some(lr(5, 6))
    );
}

#[test]
fn a_target_inside_a_modified_hunk_is_found_by_its_quote() {
    let repo = Repo::new();
    repo.write("f.txt", "top\nalpha\nbeta\ngamma\ndelta\nbottom\n");
    let one = repo.commit_all("one");
    // One hunk rewrites lines 2-5: the target `gamma` is inside it, and moved.
    repo.write("f.txt", "top\nnew1\nnew2\nnew3\ngamma\nbeta\nbottom\n");
    let two = repo.commit_all("two");
    assert_eq!(
        carried(&repo, &one, &two, "f.txt", lr(4, 4)),
        Some(lr(5, 5))
    );
}

#[test]
fn an_edited_target_line_is_outdated() {
    let repo = Repo::new();
    repo.write("f.txt", "top\nalpha\nbeta\nbottom\n");
    let one = repo.commit_all("one");
    repo.write("f.txt", "top\nalpha\nbeta changed\nbottom\n");
    let two = repo.commit_all("two");
    assert_eq!(carried(&repo, &one, &two, "f.txt", lr(3, 3)), None);
    // The neighbours outside the hunk are untouched.
    assert_eq!(
        carried(&repo, &one, &two, "f.txt", lr(2, 2)),
        Some(lr(2, 2))
    );
}

#[test]
fn a_deleted_target_is_outdated() {
    let repo = Repo::new();
    repo.write("f.txt", "top\nalpha\nbeta\nbottom\n");
    let one = repo.commit_all("one");
    repo.write("f.txt", "top\nbottom\n");
    let two = repo.commit_all("two");
    assert_eq!(carried(&repo, &one, &two, "f.txt", lr(2, 3)), None);
}

#[test]
fn repeated_text_in_a_hunk_picks_the_closest_to_the_diff_position() {
    let repo = Repo::new();
    repo.write("f.txt", "top\nx\nx\nx\nbottom\n");
    let one = repo.commit_all("one");
    // The hunk replaces the three x lines with y x y x y x: three windows hold `x`.
    repo.write("f.txt", "top\ny\nx\ny\nx\ny\nx\nbottom\n");
    let two = repo.commit_all("two");
    // The diff position of old line 4 (the third x) is the hunk's new line 4 + 2; the candidate
    // at new line 7 is the nearest to it.
    let old = blob(&repo, &one, "f.txt");
    let new = blob(&repo, &two, "f.txt");
    let wanted = TextQuote {
        exact: "x".into(),
        prefix: None,
        suffix: None,
        extra: Extra::default(),
    };
    let carry =
        |lines: LineRange| lines_of(carry(&git(&repo), &old, &new, &lines, &wanted).unwrap());
    assert_eq!(carry(lr(4, 4)), Some(lr(7, 7)));
    assert_eq!(carry(lr(2, 2)), Some(lr(3, 3)));
}

#[test]
fn a_range_reaching_past_its_hunk_is_searched_with_its_unchanged_lines() {
    let repo = Repo::new();
    repo.write("f.txt", "top\nb\nc\nd\nbottom\n");
    let one = repo.commit_all("one");
    // The hunk replaces old line 3 only. Old lines 2-3 (b and the hunk) sit at new lines 2-5;
    // a quote of `b\nk` is found at 2-3, beside the hunk, as well as at 4-5 inside it, and the
    // window that starts where the range starts wins.
    repo.write("f.txt", "top\nb\nk\nb\nk\nd\nbottom\n");
    let two = repo.commit_all("two");
    let old = blob(&repo, &one, "f.txt");
    let new = blob(&repo, &two, "f.txt");
    let wanted = TextQuote {
        exact: "b\nk".into(),
        prefix: None,
        suffix: None,
        extra: Extra::default(),
    };
    let placed = carry(&git(&repo), &old, &new, &lr(2, 3), &wanted).unwrap();
    assert_eq!(lines_of(placed), Some(lr(2, 3)));
}

#[test]
fn equally_close_repeats_are_ambiguous() {
    let repo = Repo::new();
    repo.write("f.txt", "top\na\nb\nc\nbottom\n");
    let one = repo.commit_all("one");
    // One hunk rewrites old lines 2-4 into new lines 2-4. The diff puts old line 3 at new line
    // 3, between the two `x` candidates at 2 and 4: neither is closer, so it is not guessed.
    repo.write("f.txt", "top\nx\nz\nx\nbottom\n");
    let two = repo.commit_all("two");
    let old = blob(&repo, &one, "f.txt");
    let new = blob(&repo, &two, "f.txt");
    let wanted = TextQuote {
        exact: "x".into(),
        prefix: None,
        suffix: None,
        extra: Extra::default(),
    };
    assert_eq!(
        carry(&git(&repo), &old, &new, &lr(3, 3), &wanted).unwrap(),
        Placement::Outdated
    );
    // Off the midpoint, the nearer one wins.
    assert_eq!(
        lines_of(carry(&git(&repo), &old, &new, &lr(2, 2), &wanted).unwrap()),
        Some(lr(2, 2))
    );
}

#[test]
fn context_narrows_repeats_before_distance_does() {
    let repo = Repo::new();
    repo.write("f.txt", "top\nfoo\nx\nbar\nbottom\n");
    let one = repo.commit_all("one");
    repo.write("f.txt", "top\nx\nq\nfoo\nx\nbar\nbottom\n");
    let two = repo.commit_all("two");
    // `x` between foo and bar is at new line 5, though the nearer candidate by position (new
    // line 2) has other neighbours.
    assert_eq!(
        carried(&repo, &one, &two, "f.txt", lr(3, 3)),
        Some(lr(5, 5))
    );
}

#[test]
fn changed_surroundings_place_inexactly() {
    let repo = Repo::new();
    repo.write("f.txt", "top\nalpha\nbeta\ngamma\nbottom\n");
    let one = repo.commit_all("one");
    repo.write("f.txt", "top\nalpha2\nbeta\ngamma\nbottom\n");
    let two = repo.commit_all("two");
    // `beta` is untouched by the hunk (it only changed `alpha`), but its prefix changed.
    assert_eq!(
        placement(&repo, &one, &two, "f.txt", lr(3, 3)),
        Placement::Placed {
            lines: lr(3, 3),
            exact: false
        }
    );
}

#[test]
fn binary_content_has_no_lines() {
    let repo = Repo::new();
    repo.write("b.bin", [0xff, 0xfe, 0x00, b'\n']);
    let one = repo.commit_all("one");
    let blob = blob(&repo, &one, "b.bin");
    assert!(matches!(
        blob_text(&git(&repo), &blob),
        Err(Error::Format(_))
    ));
}

#[test]
fn a_missing_path_has_no_blob() {
    let repo = Repo::new();
    repo.write("a.txt", "a\n");
    let one = repo.commit_all("one");
    assert_eq!(commit_blob(&git(&repo), &one, "nope.txt").unwrap(), None);
}

/// Three commits each commented on the lines they added, rebased onto a base that inserts lines
/// above, in the middle of, and below the commits' lines: every comment lands on its text.
#[test]
fn a_rebase_replay_places_every_comment() {
    let repo = Repo::new();
    repo.write("f.txt", numbered("base", 20));
    repo.commit_all("base");
    repo.git(&["switch", "-q", "-c", "topic"]);
    // Each commit appends or edits its own lines.
    repo.write("f.txt", format!("{}c1 a\nc1 b\n", numbered("base", 20)));
    let c1 = repo.commit_all("c1");
    repo.write(
        "f.txt",
        format!("{}c1 a\nc1 b\nc2 only\n", numbered("base", 20))
            .replace("base10\n", "base10\nc2 mid\n"),
    );
    let c2 = repo.commit_all("c2");
    repo.write(
        "f.txt",
        format!("c3 head\n{}c1 a\nc1 b\nc2 only\n", numbered("base", 20))
            .replace("base10\n", "base10\nc2 mid\n"),
    );
    let c3 = repo.commit_all("c3");

    // Comments, built on each commit's own content.
    let comment = |commit: &str, lines: LineRange| {
        let blob = blob(&repo, commit, "f.txt");
        let text = blob_text(&git(&repo), &blob).unwrap();
        let quote = quote(&text, &lines).unwrap();
        (blob, lines, quote)
    };
    let on_c1 = comment(&c1, lr(21, 22));
    let on_c2 = comment(&c2, lr(11, 11));
    let on_c3 = comment(&c3, lr(1, 1));

    // The new base inserts above, inside and below the region the commits touch.
    repo.git(&["switch", "-q", "main"]);
    let mut text = numbered("base", 20);
    // Insertions a line or more clear of the commits' own hunks, so the rebase has no conflict.
    text = text
        .replace("base3\n", "base3\nnew early\nnew early 2\n")
        .replace("base15\n", "base15\nnew late\n");
    repo.write("f.txt", text);
    repo.commit_all("new base");
    repo.git(&["switch", "-q", "topic"]);
    repo.git(&["rebase", "main"]);
    let rebased: Vec<String> = repo
        .git(&["rev-list", "--reverse", "main..topic"])
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(rebased.len(), 3);

    // Each comment is carried onto its rebased commit, and the text on those lines is its quote.
    let git = git(&repo);
    for ((old_blob, lines, quote), commit) in [on_c1, on_c2, on_c3].into_iter().zip(&rebased) {
        let new_blob = commit_blob(&git, commit, "f.txt").unwrap().unwrap();
        let Placement::Placed { lines: placed, .. } =
            carry(&git, &old_blob, &new_blob, &lines, &quote).unwrap()
        else {
            panic!("{commit}: outdated");
        };
        let text = blob_text(&git, &new_blob).unwrap();
        assert_eq!(super::quote(&text, &placed).unwrap().exact, quote.exact);
    }
}
