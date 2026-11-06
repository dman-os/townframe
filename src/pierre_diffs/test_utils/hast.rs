// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/testUtils.ts
// (the renderer-facing helpers: hast queries, `projectColumn`, `rowDigests`,
// `annotationProjection`, `collectRowSourceMismatches`) and the fixture
// stand-ins the renderer tests share.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: test-only module; hast nodes are the crate's typed `Element`s, so
// the `isHastElement` guards are `as_element`; `number | undefined` fields are
// `Option`s. `fileOld`/`fileNew` (the demo fixtures, absent from the
// extracted tree) are replaced by `big_fixture_files`.

use crate::hast::Element;
use crate::hast::ElementContent;
use crate::hast::PropValue;
use crate::renderers::diff_hunks_renderer::HunksRenderResult;
use crate::types::patch::DiffFileInput;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::ParseDiffOptions;
use crate::utils::parse_diff_from_file::parse_diff_from_file;

/// `parseDiffFromFile(oldFile, newFile)` with default options.
pub fn diff_of(name: &str, old_contents: &str, new_contents: &str) -> FileDiffMetadata {
    let file = |contents: &str| FileContents {
        name: name.to_owned(),
        contents: contents.to_owned(),
        lang: None,
        header: None,
        cache_key: None,
    };
    parse_diff_from_file(
        &DiffFileInput::Both {
            old_file: file(old_contents),
            new_file: file(new_contents),
        },
        &ParseDiffOptions::default(),
        true,
    )
    .expect("valid input")
    .value
}

/// Stand-in for upstream's `fileOld`/`fileNew` demo fixtures: 600 numbered
/// lines with a leading gap of 3 (above the threshold), three single-line
/// gaps (changes 10 lines apart), replaced, inserted and deleted lines, then
/// widely spaced edits out to line 600.
pub fn big_fixture_files() -> (String, String) {
    let old: Vec<String> = (1..=600).map(|index| format!("line {index}\n")).collect();
    let mut new = old.clone();
    // Single-line gaps: edits at 8, 18, 28, 38 (10 apart, 4 lines of context each side).
    for line in [8, 18, 28, 38] {
        new[line - 1] = format!("edited {line}\n");
    }
    // Widely spaced mixed edits: a replacement, then a growing pure insertion, then a deletion.
    for line in (80..=580).step_by(25) {
        new[line - 1] = format!("replaced {line}\n");
    }
    new[200] = String::from("inserted a\ninserted b\ninserted c\n");
    new.remove(301);
    new.remove(301);
    (old.concat(), new.concat())
}

pub fn big_fixture_diff() -> FileDiffMetadata {
    let (old, new) = big_fixture_files();
    diff_of("test.txt", &old, &new)
}

/// The string value of a property, if it is one.
pub fn prop_str<'element>(element: &'element Element, key: &str) -> Option<&'element str> {
    match element.properties.get(key) {
        Some(PropValue::Str(value)) => Some(value),
        _ => None,
    }
}

/// Whether the property is present and not `undefined`.
pub fn has_prop(element: &Element, key: &str) -> bool {
    element
        .properties
        .get(key)
        .is_some_and(|value| *value != PropValue::Undefined)
}

pub fn prop_number(element: &Element, key: &str) -> Option<i64> {
    match element.properties.get(key) {
        Some(PropValue::Int(value)) => Some(*value),
        _ => None,
    }
}

pub fn is_line_element(node: &ElementContent) -> bool {
    node.as_element()
        .is_some_and(|element| has_prop(element, "data-line"))
}

pub fn is_annotation_element(node: &ElementContent) -> bool {
    node.as_element()
        .is_some_and(|element| has_prop(element, "data-line-annotation"))
}

pub fn annotation_index_of(node: &ElementContent) -> Option<&str> {
    node.as_element()
        .and_then(|element| prop_str(element, "data-line-annotation"))
}

pub fn line_index_of(node: &ElementContent) -> Option<&str> {
    node.as_element()
        .and_then(|element| prop_str(element, "data-line-index"))
}

pub fn line_type_of(node: &ElementContent) -> Option<&str> {
    node.as_element()
        .and_then(|element| prop_str(element, "data-line-type"))
}

/// Every element below `nodes`, depth first, parents before children.
pub fn collect_all_elements(nodes: &[ElementContent]) -> Vec<&Element> {
    let mut elements = Vec::new();
    for node in nodes {
        if let Some(element) = node.as_element() {
            elements.push(element);
            elements.extend(collect_all_elements(&element.children));
        }
    }
    elements
}

pub fn find_slot_elements(element: &Element) -> Vec<&Element> {
    let mut slots = Vec::new();
    for child in &element.children {
        if let Some(child) = child.as_element() {
            if child.tag_name == "slot" {
                slots.push(child);
            }
            slots.extend(find_slot_elements(child));
        }
    }
    slots
}

pub fn count_rendered_lines(ast: &[ElementContent]) -> usize {
    collect_all_elements(ast)
        .into_iter()
        .filter(|element| has_prop(element, "data-line"))
        .count()
}

/// `(unified indexes, split indexes)` of every rendered line, in order.
pub fn extract_line_numbers(ast: &[ElementContent]) -> (Vec<usize>, Vec<usize>) {
    let (mut unified, mut split) = (Vec::new(), Vec::new());
    for element in collect_all_elements(ast) {
        if has_prop(element, "data-line")
            && let Some(line_index) = prop_str(element, "data-line-index")
        {
            // data-line-index format is "unifiedIndex,splitIndex"
            let mut parts = line_index.split(',');
            if let Some(index) = parts.next().and_then(|part| part.parse().ok()) {
                unified.push(index);
            }
            if let Some(index) = parts.next().and_then(|part| part.parse().ok()) {
                split.push(index);
            }
        }
    }
    (unified, split)
}

/// Rows in split mode by unique split line index; each is one visual row.
pub fn count_split_rows(result: &HunksRenderResult) -> usize {
    let mut indexes = std::collections::BTreeSet::new();
    for ast in [&result.additions_content_ast, &result.deletions_content_ast]
        .into_iter()
        .flatten()
    {
        for element in collect_all_elements(ast) {
            if let Some(line_index) = prop_str(element, "data-line-index")
                && let Some(split) = line_index
                    .split(',')
                    .nth(1)
                    .and_then(|part| part.parse::<usize>().ok())
            {
                indexes.insert(split);
            }
        }
    }
    indexes.len()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKind {
    Line,
    Buffer,
    NoNewline,
    Separator,
    Annotation,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedRow {
    pub kind: RowKind,
    pub unified_index: Option<usize>,
    pub split_index: Option<usize>,
    pub line_number: Option<i64>,
    pub alt_line_number: Option<i64>,
    pub line_type: Option<String>,
    pub text: Option<String>,
    pub buffer_size: Option<i64>,
    pub separator: Option<String>,
    pub annotation_index: Option<String>,
}

impl ProjectedRow {
    fn of(kind: RowKind) -> Self {
        Self {
            kind,
            unified_index: None,
            split_index: None,
            line_number: None,
            alt_line_number: None,
            line_type: None,
            text: None,
            buffer_size: None,
            separator: None,
            annotation_index: None,
        }
    }
}

/// Projects the top-level nodes of a rendered content column into one
/// `ProjectedRow` per visual row (`projectColumn`).
pub fn project_column(ast: &[ElementContent]) -> Vec<ProjectedRow> {
    let mut rows = Vec::new();
    for node in ast {
        let Some(element) = node.as_element() else {
            continue;
        };
        if has_prop(element, "data-line") {
            let line_index = prop_str(element, "data-line-index").unwrap_or("");
            let mut parts = line_index.split(',');
            let mut text = node.text_content();
            if text.ends_with('\n') {
                text.pop();
            }
            rows.push(ProjectedRow {
                unified_index: parts.next().and_then(|part| part.parse().ok()),
                split_index: parts.next().and_then(|part| part.parse().ok()),
                line_number: prop_number(element, "data-line"),
                alt_line_number: prop_number(element, "data-alt-line"),
                line_type: prop_str(element, "data-line-type").map(str::to_owned),
                text: Some(text),
                ..ProjectedRow::of(RowKind::Line)
            });
        } else if has_prop(element, "data-content-buffer") {
            rows.push(ProjectedRow {
                buffer_size: prop_number(element, "data-buffer-size"),
                ..ProjectedRow::of(RowKind::Buffer)
            });
        } else if has_prop(element, "data-no-newline") {
            rows.push(ProjectedRow::of(RowKind::NoNewline));
        } else if let Some(separator) = prop_str(element, "data-separator") {
            rows.push(ProjectedRow {
                separator: Some(separator.to_owned()),
                ..ProjectedRow::of(RowKind::Separator)
            });
        } else if let Some(annotation) = prop_str(element, "data-line-annotation") {
            rows.push(ProjectedRow {
                annotation_index: Some(annotation.to_owned()),
                ..ProjectedRow::of(RowKind::Annotation)
            });
        } else {
            rows.push(ProjectedRow::of(RowKind::Other));
        }
    }
    rows
}

/// One short reviewable line per projected row (`rowDigests`).
pub fn row_digests(rows: &[ProjectedRow]) -> Vec<String> {
    let number =
        |value: Option<usize>| value.map_or_else(|| String::from("undefined"), |v| v.to_string());
    rows.iter()
        .map(|row| match row.kind {
            RowKind::Line => {
                let alt = match (row.alt_line_number, row.line_number) {
                    (Some(alt), Some(line)) if alt != line => format!("(alt {alt})"),
                    _ => String::new(),
                };
                format!(
                    "u{}/s{} {} #{}{alt} |{}|",
                    number(row.unified_index),
                    number(row.split_index),
                    row.line_type.as_deref().unwrap_or("undefined"),
                    row.line_number
                        .map_or_else(|| String::from("NaN"), |v| v.to_string()),
                    row.text.as_deref().unwrap_or("")
                )
            }
            RowKind::Buffer => format!("buffer x{}", row.buffer_size.unwrap_or(0)),
            RowKind::Separator => {
                format!("separator {}", row.separator.as_deref().unwrap_or(""))
            }
            RowKind::Annotation => {
                format!(
                    "annotation {}",
                    row.annotation_index.as_deref().unwrap_or("")
                )
            }
            RowKind::NoNewline => String::from("no-newline"),
            RowKind::Other => String::from("other"),
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnotationProjectionEntry {
    pub line_index: Option<String>,
    pub annotation_index: String,
    pub slot_names: Vec<Option<String>>,
}

/// Pairs every annotation element with the `data-line-index` of the line row
/// preceding it, in document order (`annotationProjection`).
pub fn annotation_projection(ast: &[ElementContent]) -> Vec<AnnotationProjectionEntry> {
    let mut entries = Vec::new();
    let mut last_line_index: Option<String> = None;
    for element in collect_all_elements(ast) {
        if has_prop(element, "data-line") {
            last_line_index = prop_str(element, "data-line-index").map(str::to_owned);
            continue;
        }
        let Some(annotation_index) = prop_str(element, "data-line-annotation") else {
            continue;
        };
        entries.push(AnnotationProjectionEntry {
            line_index: last_line_index.clone(),
            annotation_index: annotation_index.to_owned(),
            slot_names: find_slot_elements(element)
                .into_iter()
                .map(|slot| prop_str(slot, "name").map(str::to_owned))
                .collect(),
        });
    }
    entries
}

/// The column a mismatch report is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnName {
    Unified,
    Deletions,
    Additions,
}

/// Compares rendered line-row text against the original source lines and
/// returns one message per mismatch (`collectRowSourceMismatches`).
pub fn collect_row_source_mismatches(
    rows: &[ProjectedRow],
    column: ColumnName,
    old_lines: &[&str],
    new_lines: &[&str],
) -> Vec<String> {
    let mut mismatches = Vec::new();
    for row in rows {
        let (RowKind::Line, Some(line_number)) = (row.kind, row.line_number) else {
            continue;
        };
        let from_old = column == ColumnName::Deletions
            || (column == ColumnName::Unified
                && row.line_type.as_deref() == Some("change-deletion"));
        let source = if from_old { old_lines } else { new_lines };
        let expected = usize::try_from(line_number - 1)
            .ok()
            .and_then(|index| source.get(index).copied());
        if row.text.as_deref() != expected {
            mismatches.push(format!(
                "{column:?} #{line_number} ({:?}): rendered {:?} != source {expected:?}",
                row.line_type, row.text
            ));
        }
    }
    mismatches
}
