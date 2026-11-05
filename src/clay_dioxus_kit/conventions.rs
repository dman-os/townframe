//! Mechanical checks of the README's structural conventions (`KIT-0xx`), run as ordinary tests.
//!
//! `guardrails.toml` already refuses a dependency on any workspace crate (`allowed_dependencies`
//! is empty) and compiles the crate for all three targets; what it cannot see is how the sources
//! are laid out and what they name. These tests read the sources of this crate (and the showcase
//! that holds the specimens) as text, with comments stripped so a doc comment may still explain
//! a forbidden thing, and fail with the file and line that broke the rule. Adding a component
//! needs no edit here: the component list is derived from `headless/`.
use std::path::Path;
use std::path::PathBuf;

/// One source file of the crate: its path relative to the crate root and its text. `code` is the
/// text before the test module with comment lines removed; `text` is the whole file.
struct Source {
    path: String,
    text: String,
}

impl Source {
    fn code(&self) -> impl Iterator<Item = (usize, &str)> {
        let production = self
            .text
            .split_once("#[cfg(test)]")
            .map_or(self.text.as_str(), |(production, _)| production);
        production
            .lines()
            .enumerate()
            .map(|(index, line)| (index + 1, line))
            .filter(|(_, line)| !line.trim_start().starts_with("//"))
    }

    /// The string literals of the production code, each with its line number.
    fn literals(&self) -> Vec<(usize, &str)> {
        let mut literals = Vec::new();
        for (line_number, line) in self.code() {
            let mut parts = line.split('"');
            // The pieces at odd positions are the ones between a pair of quotes.
            parts.next();
            literals.extend(parts.step_by(2).map(|literal| (line_number, literal)));
        }
        literals
    }

    fn violations(
        &self,
        rule: &str,
        mut offends: impl FnMut(&str) -> Option<String>,
    ) -> Vec<String> {
        self.code()
            .filter_map(|(line_number, line)| {
                offends(line).map(|what| format!("{rule}: {}:{line_number}: {what}", self.path))
            })
            .collect()
    }
}

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(path: &Path) -> Source {
    Source {
        path: path
            .strip_prefix(crate_root())
            .unwrap_or(path)
            .display()
            .to_string(),
        text: std::fs::read_to_string(path)
            .unwrap_or_else(|err| panic!("reading {}: {err}", path.display())),
    }
}

/// The `.rs` files directly inside `dir`, sorted.
fn rust_files(dir: &Path) -> Vec<Source> {
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .unwrap_or_else(|err| panic!("listing {}: {err}", dir.display()))
        .map(|entry| entry.expect("directory entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .collect();
    paths.sort();
    paths.iter().map(|path| read(path)).collect()
}

/// Every source file of the crate, the headless parts included.
fn all_sources() -> Vec<Source> {
    let mut sources = rust_files(&crate_root());
    sources.extend(rust_files(&crate_root().join("headless")));
    sources
}

/// The component names, derived from `headless/<name>.rs`: `(snake_case, PascalCase)`.
fn components() -> Vec<(String, String)> {
    rust_files(&crate_root().join("headless"))
        .iter()
        .map(|source| {
            let snake = Path::new(&source.path)
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("file stem")
                .to_owned();
            let pascal = snake
                .split('_')
                .map(|word| {
                    let (head, tail) = word.split_at(1);
                    head.to_uppercase() + tail
                })
                .collect();
            (snake, pascal)
        })
        .collect()
}

/// The styled part of every component (`<name>.rs` at the crate root).
fn styled_parts() -> Vec<Source> {
    components()
        .iter()
        .map(|(snake, _)| read(&crate_root().join(format!("{snake}.rs"))))
        .collect()
}

fn headless_parts() -> Vec<Source> {
    rust_files(&crate_root().join("headless"))
}

fn assert_no_violations(violations: Vec<String>) {
    assert!(
        violations.is_empty(),
        "kit convention broken:\n{}",
        violations.join("\n")
    );
}

/// `KIT-001`: the only runtime dependency is `dioxus`, with default features off. (A dependency
/// on a workspace crate is refused by `guardrails.toml`; this also refuses third-party ones.)
#[test]
fn the_only_runtime_dependency_is_dioxus_without_default_features() {
    let manifest = read(&crate_root().join("Cargo.toml"));
    let dependencies: Vec<_> = manifest
        .text
        .lines()
        .skip_while(|line| line.trim() != "[dependencies]")
        .skip(1)
        .take_while(|line| !line.trim_start().starts_with('['))
        .filter(|line| !line.trim().is_empty() && !line.trim_start().starts_with('#'))
        .collect();
    assert!(
        dependencies.len() == 1
            && dependencies[0].starts_with("dioxus = ")
            && dependencies[0].contains("default-features = false"),
        "KIT-001: [dependencies] must be exactly `dioxus` with default-features = false, got {dependencies:#?}"
    );
}

/// `KIT-002`, `KIT-072`: a component is `headless/<name>.rs` (public) plus `<name>.rs` (private),
/// both declared, the headless part with tests, and the README lists it.
#[test]
fn a_component_is_a_public_headless_module_and_a_private_styled_one() {
    let lib = read(&crate_root().join("lib.rs"));
    let headless = read(&crate_root().join("headless.rs"));
    let readme = read(&crate_root().join("README.md"));
    let mut violations = Vec::new();
    for (snake, pascal) in components() {
        if !headless.text.contains(&format!("pub mod {snake};")) {
            violations.push(format!(
                "KIT-002: headless.rs does not declare `pub mod {snake};`"
            ));
        }
        if !lib.text.contains(&format!("\nmod {snake};")) {
            violations.push(format!(
                "KIT-002: lib.rs does not declare a private `mod {snake};`"
            ));
        }
        if lib.text.contains(&format!("pub mod {snake};")) {
            violations.push(format!(
                "KIT-002: lib.rs makes the styled `{snake}` module public"
            ));
        }
        if !lib.text.contains(&format!("pub use {snake}::{pascal};")) {
            violations.push(format!(
                "KIT-002: lib.rs does not re-export `{snake}::{pascal}`"
            ));
        }
        if !readme.text.contains(&format!("`headless::{snake}`")) {
            violations.push(format!(
                "KIT-002: the README component table has no row for `headless::{snake}`"
            ));
        }
        let headless_part = read(&crate_root().join("headless").join(format!("{snake}.rs")));
        let (_, tests) = headless_part
            .text
            .split_once("#[cfg(test)]")
            .unwrap_or(("", ""));
        if !tests.contains("#[test]") {
            violations.push(format!("KIT-072: headless/{snake}.rs has no test"));
        }
    }
    assert_no_violations(violations);
}

/// `KIT-010`, `KIT-011`: a headless part names no class and imports no styled part; it receives
/// class strings through its `*Skin` props.
#[test]
fn headless_parts_hold_no_styling() {
    let styled: Vec<_> = components()
        .into_iter()
        .flat_map(|(snake, pascal)| [snake, pascal])
        .collect();
    let violations = headless_parts()
        .iter()
        .flat_map(|source| {
            let mut found = source.violations("KIT-011", |line| {
                let class_literal = line.contains("class: \"") || line.contains("class:\"");
                let styled_import = line.split("crate::").skip(1).any(|rest| {
                    let head: String = rest.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
                    styled.contains(&head)
                });
                (class_literal || styled_import).then(|| format!("a headless part names a class or imports a styled part: {}", line.trim()))
            });
            // A `use` of a Tailwind-looking token is not possible, but a literal in a skin
            // default would be: the skin structs hold no literal either.
            found.extend(source.literals().into_iter().filter_map(|(line_number, literal)| {
                looks_like_utility_classes(literal)
                    .then(|| format!("KIT-011: {}:{line_number}: utility classes in a headless part: {literal:?}", source.path))
            }));
            found
        })
        .collect();
    assert_no_violations(violations);
}

/// Whether a string literal is a run of Tailwind utilities (two or more tokens naming a
/// well-known utility prefix). Inline `style` strings (`display:flex;...`) and prose do not match.
fn looks_like_utility_classes(literal: &str) -> bool {
    const PREFIXES: [&str; 14] = [
        "bg-", "text-", "rounded", "px-", "py-", "p-", "m-", "gap-", "flex", "size-", "w-", "h-",
        "shadow", "border",
    ];
    literal
        .split_whitespace()
        .filter(|token| PREFIXES.iter().any(|prefix| token.starts_with(prefix)))
        .count()
        >= 2
}

/// `KIT-011`: the styled part uses utilities that resolve to semantic tokens: no palette colours
/// and no arbitrary values, except Tailwind's `data-[state=...]` variant.
#[test]
fn styled_parts_use_semantic_tokens_only() {
    const PALETTE: [&str; 22] = [
        "red", "orange", "amber", "yellow", "lime", "green", "emerald", "teal", "cyan", "sky",
        "blue", "indigo", "violet", "purple", "fuchsia", "pink", "rose", "slate", "gray", "zinc",
        "neutral", "stone",
    ];
    let mut violations = Vec::new();
    for source in styled_parts() {
        for (line_number, literal) in source.literals() {
            for token in literal.split_whitespace() {
                let utility = token.rsplit(':').next().unwrap_or(token);
                // `data-[state=x]:` and its `group-` form are the data-attribute variant; any other
                // bracket is an arbitrary value.
                let without_state_variants = token
                    .split("data-[state=")
                    .enumerate()
                    .map(|(index, part)| {
                        if index == 0 {
                            part
                        } else {
                            part.split_once(']').map_or(part, |(_, rest)| rest)
                        }
                    })
                    .collect::<String>();
                let arbitrary = without_state_variants.contains('[');
                let palette = PALETTE.iter().any(|colour| {
                    utility
                        .split('-')
                        .skip(1)
                        .collect::<Vec<_>>()
                        .windows(2)
                        .any(|pair| {
                            pair[0] == *colour && pair[1].chars().all(|c| c.is_ascii_digit())
                        })
                }) || ["white", "black"].iter().any(|colour| {
                    utility.ends_with(&format!("-{colour}")) && !utility.starts_with("outline-")
                });
                if arbitrary || palette {
                    violations.push(format!(
                        "KIT-011: {}:{line_number}: `{token}` is {} (use a semantic token)",
                        source.path,
                        if arbitrary {
                            "an arbitrary value"
                        } else {
                            "a palette colour"
                        }
                    ));
                }
            }
        }
    }
    assert_no_violations(violations);
}

/// `KIT-012`: a styled component takes no `class` or `style` pass-through.
#[test]
fn styled_components_take_no_class_or_style() {
    let mut violations = Vec::new();
    for source in styled_parts() {
        let production = source.text.split("#[cfg(test)]").next().unwrap_or_default();
        for signature in production.split("#[component]").skip(1) {
            let parameters = signature.split(") ->").next().unwrap_or_default();
            for name in ["class", "style"] {
                if parameters.contains(&format!("{name}:"))
                    || parameters.contains(&format!("{name}: "))
                {
                    violations.push(format!(
                        "KIT-012: {}: a #[component] takes a `{name}` parameter",
                        source.path
                    ));
                }
            }
        }
    }
    assert_no_violations(violations);
}

/// `KIT-040`: the breakpoints are defined in `width.rs` and nowhere else, and no component
/// measures the DOM (Blitz panics on rect reads inside handlers; a guest has no DOM).
#[test]
fn breakpoints_live_in_width_rs_and_nothing_measures_the_dom() {
    const MEASURING: [&str; 10] = [
        "get_client_rect",
        "getBoundingClientRect",
        "onmounted",
        "MountedData",
        "get_scroll_offset",
        "get_scroll_size",
        "get_raw_element",
        "use_window",
        "web_sys",
        "innerWidth",
    ];
    let mut violations = Vec::new();
    for source in all_sources() {
        if source.path != "width.rs" {
            violations.extend(source.violations("KIT-040", |line| {
                ["600.0", "840.0", "600_", "840_"]
                    .iter()
                    .find(|literal| line.contains(**literal))
                    .map(|literal| format!("breakpoint literal `{literal}` outside width.rs"))
            }));
        }
        violations.extend(source.violations("KIT-040", |line| {
            MEASURING
                .iter()
                .find(|api| line.contains(**api))
                .map(|api| format!("`{api}` measures the DOM or asks the window"))
        }));
    }
    assert_no_violations(violations);
}

/// `KIT-052`: `tabindex` (which panics Blitz) is set only by the opt-in `Split::keyboard` path,
/// and nothing moves focus programmatically.
#[test]
fn tabindex_is_only_the_keyboard_opt_in() {
    let mut violations = Vec::new();
    for source in all_sources() {
        // The test harness lists `tabindex` among the attributes a guest may name (`KIT-053`).
        if source.path == "testing.rs" {
            continue;
        }
        violations.extend(source.violations("KIT-052", |line| {
            if line.contains("tabindex") && source.path != "headless/sections.rs" {
                return Some("`tabindex` outside the Sections opt-in".into());
            }
            if line.contains("let tabindex") && !line.contains(".keyboard") {
                return Some("`tabindex` not gated on the `keyboard` prop".into());
            }
            ["autofocus", "set_focus", ".focus(", "focus_next"]
                .iter()
                .find(|call| line.contains(**call))
                .map(|call| format!("`{call}` moves focus"))
        }));
    }
    assert_no_violations(violations);
}

/// `KIT-071`: tests are deterministic: no sleeps, timers, clocks, threads or async runtimes, in
/// the harness and the tests alike (hence the whole file, not the production half).
#[test]
fn tests_neither_sleep_nor_wait() {
    // This file spells the forbidden names, so it is not scanned.
    let mut violations = Vec::new();
    for source in all_sources()
        .iter()
        .filter(|source| source.path != "conventions.rs")
    {
        for (index, line) in source.text.lines().enumerate() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            for banned in [
                "sleep",
                "Instant::",
                "SystemTime",
                "tokio",
                "thread::",
                "timeout",
                "interval",
            ] {
                if line.contains(banned) {
                    violations.push(format!(
                        "KIT-071: {}:{}: `{banned}`",
                        source.path,
                        index + 1
                    ));
                }
            }
        }
    }
    assert_no_violations(violations);
}

/// `KIT-080`: every component has a specimen in the showcase, and a framed specimen has both a
/// compact and an expanded frame. `NavButton` is a leaf button shown inline and `ListRow` is shown
/// inside the `ListDetails` frames, so neither has a frame of its own.
#[test]
fn every_component_has_a_specimen_at_both_widths() {
    const UNFRAMED: [&str; 2] = ["NavButton", "ListRow"];
    let showcase = read(&crate_root().join("../dayframe_showcase/kit.rs"));
    let mut violations = Vec::new();
    for (_, pascal) in components() {
        if !showcase.text.contains(&format!("{pascal} {{")) {
            violations.push(format!(
                "KIT-080: the showcase has no specimen of `{pascal}`"
            ));
            continue;
        }
        if UNFRAMED.contains(&pascal.as_str()) {
            continue;
        }
        let frames: Vec<_> = showcase
            .text
            .lines()
            .filter(|line| line.contains(&format!("Frame {{ title: \"{pascal}")))
            .collect();
        for width in ["COMPACT_PX", "EXPANDED_PX"] {
            if !frames.iter().any(|frame| frame.contains(width)) {
                violations.push(format!("KIT-080: no `{pascal}` frame at {width}"));
            }
        }
    }
    assert_no_violations(violations);
}

/// Every rule a source cites (`KIT-0xx`) is a rule the README defines, so a renumbering or a
/// removed rule cannot leave a dangling reference behind.
#[test]
fn cited_rules_exist_in_the_readme() {
    let readme = read(&crate_root().join("README.md"));
    let defined = |rule: &str| readme.text.contains(&format!("**{rule}**"));
    let mut violations = Vec::new();
    for source in all_sources()
        .iter()
        .filter(|source| source.path != "conventions.rs")
    {
        for (index, line) in source.text.lines().enumerate() {
            for (at, _) in line.match_indices("KIT-") {
                let rule: String = line[at..].chars().take(7).collect();
                if !defined(&rule) {
                    violations.push(format!(
                        "{}:{}: cites {rule}, which the README does not define",
                        source.path,
                        index + 1
                    ));
                }
            }
        }
    }
    assert_no_violations(violations);
}

/// `KIT-090`: a headless module whose docs mention Blitz (a renderer limit shaped it) records the
/// limit under `# Renderer limits`, so callers never rediscover it.
#[test]
fn a_module_that_mentions_blitz_documents_it_under_renderer_limits() {
    let violations = headless_parts()
        .iter()
        .filter(|source| {
            let production = source.text.split("#[cfg(test)]").next().unwrap_or_default();
            production.contains("Blitz") && !production.contains("# Renderer limits")
        })
        .map(|source| {
            format!(
                "KIT-090: {} mentions Blitz without a `# Renderer limits` section",
                source.path
            )
        })
        .collect();
    assert_no_violations(violations);
}
