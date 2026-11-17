// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/DiffHunksRendererLanguages.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest. Upstream drives the Shiki backend
// (`preferredHighlighter: 'shiki-js'`), whose grammars load lazily, and asserts
// which grammars `hasLoadedLanguages` reports. The `highlights` backend
// ported here has bundled lexers (everything is "loaded"), so the loading
// assertions become the observable fact they stood for: which language each
// side of the diff was tokenised with, recorded at the wasm boundary by a
// [`HighlightRecords`] test double (a trivial one-token-per-line lexer in
// place of the wasm). Not ported: the cold/destination-preloaded variants and
// `renderDiff`/`hydrate` background loading (async grammar loading, `hydrate`
// and `onRenderUpdate` are dropped; PARITY.md), so the `asyncRender`
// cold/preloaded pairs are one synchronous case each. The Terraform test keeps
// its aliases against the highlights language table.

use std::cell::RefCell;
use std::collections::BTreeSet;

use crate::highlighter_types::DiffsHighlighter;
use crate::highlights::backend::HighlightsHighlighter;
use crate::highlights::highlighter::HighlightRecords;
use crate::highlights::highlighter::lang_id_of;
use crate::highlights::tokens::LINE_MARKER;
use crate::renderers::test_support::renderer_with;
use crate::test_utils::hast::diff_of;
use crate::types::options::DiffStyle;
use crate::types::options::ThemeOption;
use crate::types::patch::FileDiffMetadata;
use crate::utils::render::get_token_options::get_token_options;

/// One token per non-empty line; records every `(language id)` it is asked for.
#[derive(Default)]
struct RecordingLexer {
    languages: RefCell<BTreeSet<u8>>,
}

impl HighlightRecords for &RecordingLexer {
    fn records(&self, text: &str, lang_id: u8) -> Vec<u32> {
        self.languages.borrow_mut().insert(lang_id);
        let mut records = Vec::new();
        let mut line_start = 0usize;
        let lines: Vec<&str> = text.split('\n').collect();
        for (index, line) in lines.iter().enumerate() {
            let end = line_start + line.encode_utf16().count();
            if !line.is_empty() {
                records.extend([end as u32, 1]);
            }
            if index + 1 < lines.len() {
                records.extend([(end + 1) as u32, LINE_MARKER]);
            }
            line_start = end + 1;
        }
        records
    }
}

const PYTHON: (&str, &str, &str) = ("example.py", "print(\"python\")\n", "python");
const JAVASCRIPT: (&str, &str, &str) =
    ("example.js", "console.log(\"javascript\");\n", "javascript");

fn theme() -> ThemeOption {
    ThemeOption::Name(String::from("pierre-dark"))
}

fn render(
    lexer: &RecordingLexer,
    diff: &FileDiffMetadata,
    configure: impl FnOnce(&mut crate::types::options::BaseDiffOptions),
) {
    let highlighter = HighlightsHighlighter::new(lexer);
    let renderer = renderer_with::<()>(|options| {
        options.base.theme = Some(theme());
        options.diff_style = Some(DiffStyle::Split);
        configure(options);
    });
    let result = renderer.render_diff(&highlighter, diff, &crate::constants::DEFAULT_RENDER_RANGE);
    assert!(result.additions_content_ast.is_some() || result.deletions_content_ast.is_some());
}

fn ids(names: &[&str]) -> BTreeSet<u8> {
    names
        .iter()
        .map(|name| lang_id_of(name).expect("a bundled language"))
        .collect()
}

#[test]
fn the_bundled_terraform_loader_renders_an_ordinary_tf_edit_and_provides_its_aliases() {
    let lexer = RecordingLexer::default();
    let diff = diff_of(
        "example.tf",
        "locals {\n  label = \"before\"\n}\n",
        "locals {\n  label = \"after\"\n}\n",
    );
    render(&lexer, &diff, |_| {});
    let highlighter = HighlightsHighlighter::new(&lexer);
    for lang in ["terraform", "tf", "tfvars"] {
        assert!(highlighter.has_loaded_languages(&[lang]));
        let tokens = highlighter.code_to_tokens(
            "locals { label = \"after\" }",
            &get_token_options(lang, &theme(), None),
        );
        assert!(!tokens.is_empty(), "{lang}");
    }
    // `terraform` and `tfvars` are one lexer.
    assert_eq!(lang_id_of("terraform"), lang_id_of("tfvars"));
}

#[test]
fn render_diff_tokenises_each_side_with_its_own_language() {
    let cases = [
        (PYTHON, (PYTHON.0, "print(\"changed\")\n", PYTHON.2)),
        (
            JAVASCRIPT,
            (JAVASCRIPT.0, "console.log(\"changed\");\n", JAVASCRIPT.2),
        ),
        (PYTHON, JAVASCRIPT),
        (JAVASCRIPT, PYTHON),
    ];
    for (old, new) in cases {
        let lexer = RecordingLexer::default();
        let diff = diff_of_files(old, new);
        if old.0 == new.0 {
            assert_eq!(diff.prev_name, None);
        }
        render(&lexer, &diff, |_| {});
        // Both grammars are used: the destination's and the source's.
        assert_eq!(
            *lexer.languages.borrow(),
            ids(&[old.2, new.2]),
            "{} -> {}",
            old.2,
            new.2
        );
        let highlighter = HighlightsHighlighter::new(&lexer);
        assert!(highlighter.has_loaded_languages(&[old.2]));
        assert!(highlighter.has_loaded_languages(&[new.2]));
    }
}

fn diff_of_files(old: (&str, &str, &str), new: (&str, &str, &str)) -> FileDiffMetadata {
    use crate::types::patch::DiffFileInput;
    use crate::types::patch::FileContents;
    use crate::types::patch::ParseDiffOptions;
    use crate::utils::parse_diff_from_file::parse_diff_from_file;
    let file = |(name, contents, _): (&str, &str, &str)| FileContents {
        name: name.to_owned(),
        contents: contents.to_owned(),
        lang: None,
        header: None,
        cache_key: None,
    };
    parse_diff_from_file(
        &DiffFileInput::Both {
            old_file: file(old),
            new_file: file(new),
        },
        &ParseDiffOptions::default(),
        true,
    )
    .expect("valid input")
    .value
}

#[test]
fn an_explicit_lang_override_takes_precedence_over_both_filenames() {
    for lang in ["json", "text"] {
        let lexer = RecordingLexer::default();
        let mut diff = diff_of_files(PYTHON, JAVASCRIPT);
        diff.lang = Some(lang.to_owned());
        render(&lexer, &diff, |_| {});
        let used = lexer.languages.borrow().clone();
        assert_eq!(used, ids(&[lang]), "{lang}");
        assert!(!used.contains(&lang_id_of("python").expect("python")));
        assert!(!used.contains(&lang_id_of("javascript").expect("javascript")));
    }
}

#[test]
fn a_rename_above_the_size_threshold_does_not_use_either_grammar() {
    let lexer = RecordingLexer::default();
    render(&lexer, &diff_of_files(PYTHON, JAVASCRIPT), |options| {
        options.base.tokenize_max_length = Some(0);
    });
    let used = lexer.languages.borrow().clone();
    assert_eq!(used, ids(&["text"]));
}
