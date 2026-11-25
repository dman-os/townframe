//! Mechanical checks of the README's structural conventions (`KIT-0xx`), run as ordinary tests.
//!
//! `guardrails.toml` already refuses a dependency on any workspace crate (`allowed_dependencies`
//! is empty) and compiles the crate for all three targets; what it cannot see is how the sources
//! are laid out and what they name. These tests read the sources of this crate (and the specimens
//! crate that holds the specimens) as text, with comments stripped so a doc comment may still explain
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

/// The root class of a component: `ct-` and its name in kebab case (`KIT-201`).
fn root_class(snake: &str) -> String {
    format!("ct-{}", snake.replace('_', "-"))
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

/// `KIT-002`, `KIT-072`, `KIT-200`: a component is `headless/<name>.rs` (public) plus a stylesheet
/// (`styles/kit/<name>.css`), and nothing else visual: no styled Rust part beside it. The headless
/// part is declared and re-exported at the root, has tests, and the README lists it.
#[test]
fn a_component_is_a_public_headless_module_and_a_skin() {
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
        if !crate_root()
            .join("styles/kit")
            .join(format!("{snake}.css"))
            .exists()
        {
            violations.push(format!(
                "KIT-200: `{snake}` has no stylesheet `styles/kit/{snake}.css`"
            ));
        }
        if crate_root().join(format!("{snake}.rs")).exists() {
            violations.push(format!(
                "KIT-200: `{snake}` has a styled Rust part; a component is its headless part and its stylesheet"
            ));
        }
        if !lib
            .text
            .contains(&format!("pub use headless::{snake}::{pascal};"))
        {
            violations.push(format!(
                "KIT-002: lib.rs does not re-export `headless::{snake}::{pascal}`"
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
        // The tests may sit in `headless/<name>/tests.rs` instead of inline.
        let external = crate_root().join("headless").join(&snake).join("tests.rs");
        let external = tests.contains("mod tests;") && read(&external).text.contains("#[test]");
        if !tests.contains("#[test]") && !external {
            violations.push(format!("KIT-072: headless/{snake}.rs has no test"));
        }
    }
    assert_no_violations(violations);
}

/// `KIT-010`, `KIT-200`: a headless part names no class but its root (`ct-<name>`), holds no
/// utility strings and has no skin prop.
#[test]
fn headless_parts_hold_no_styling() {
    let violations = headless_parts()
        .iter()
        .flat_map(|source| {
            let own_root = Path::new(&source.path)
                .file_stem()
                .and_then(|stem| stem.to_str())
                .map(root_class);
            let mut found = source.violations("KIT-200", |line| {
                let class_literal = (line.contains("class: \"") || line.contains("class:\""))
                    && !own_root.as_ref().is_some_and(|root| {
                        line.contains(&format!("class: \"{root}\""))
                    });
                let skin = line.contains("Skin");
                (class_literal || skin).then(|| format!("a headless part names a class other than its root, or a skin: {}", line.trim()))
            });
            found.extend(
                source
                    .literals()
                    .into_iter()
                    .filter(|(_, literal)| looks_like_utility_classes(literal))
                    .map(|(line_number, literal)| {
                        format!(
                            "KIT-200: {}:{line_number}: utility classes in a headless part: {literal:?}",
                            source.path
                        )
                    }),
            );
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

/// `KIT-200`: a component takes no `class` or `style` pass-through, so a caller cannot bring its
/// own look to it: appearance is the stylesheet's and the variants are enum props.
#[test]
fn components_take_no_class_or_style_pass_through() {
    let mut violations = Vec::new();
    for source in headless_parts() {
        let production = source.text.split("#[cfg(test)]").next().unwrap_or_default();
        for signature in production.split("#[component]").skip(1) {
            let parameters = signature.split(") ->").next().unwrap_or_default();
            for name in ["class", "style"] {
                if parameters.contains(&format!("{name}:")) {
                    violations.push(format!(
                        "KIT-200: {}: a #[component] takes a `{name}` parameter",
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
    const MEASURING: [&str; 9] = [
        "get_client_rect",
        "getBoundingClientRect",
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
        // The harness hands handlers an element backing; it measures nothing.
        if source.path == "testing.rs" {
            continue;
        }
        violations.extend(source.violations("KIT-040", |line| {
            MEASURING
                .iter()
                // `headless.rs` holds the roving-focus helper, which keeps element handles to
                // focus them and never measures through them.
                .filter(|api| !(source.path == "headless.rs" && **api == "MountedData"))
                .find(|api| line.contains(**api))
                .map(|api| format!("`{api}` measures the DOM or asks the window"))
        }));
    }
    assert_no_violations(violations);
}

/// `KIT-052`: `tabindex` (which panics Blitz) is set only by the `Split::renderer` path
/// (`Renderer::takes_focus`), by the roving widgets (`Tabs`, `ScrubStrip`) on a line that names
/// `takes_focus`, and by the modal panels (`Sheet`, `Drawer`), which take `-1` and are focused on
/// mount by `headless::focus_on_mount`. Focus moves programmatically only in `headless.rs`
/// (`focus_on_mount` and the roving-focus ring).
#[test]
fn tabindex_is_only_the_renderer_focus_gate() {
    let mut violations = Vec::new();
    for source in all_sources() {
        // The test harness lists `tabindex` among the attributes a guest may name (`KIT-053`).
        if source.path == "testing.rs" {
            continue;
        }
        violations.extend(source.violations("KIT-052", |line| {
            let modal_panel =
                ["headless/sheet.rs", "headless/drawer.rs"].contains(&source.path.as_str());
            let roving =
                ["headless/tabs.rs", "headless/scrub_strip.rs"].contains(&source.path.as_str());
            if roving && line.contains("tabindex") && !line.contains("takes_focus") {
                return Some("`tabindex` not gated on `Renderer::takes_focus`".into());
            }
            if line.contains("tabindex")
                && source.path != "headless/sections.rs"
                && !modal_panel
                && !roving
            {
                return Some("`tabindex` outside the Sections renderer gate".into());
            }
            if source.path == "headless.rs" {
                return None;
            }
            if line.contains("let tabindex") && !line.contains(".takes_focus()") {
                return Some("`tabindex` not gated on `Renderer::takes_focus`".into());
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

/// `KIT-204`: every component has a specimen page, `clay_dioxus_specimens/specimens/<name>.rs`, that
/// mounts it. A component that is laid out against its host's width (the ones that take a
/// `WidthClass` or a split extent) shows a compact and an expanded frame, because the kit never
/// measures and a frame is the host that hands the component its class.
#[test]
fn every_component_has_a_specimen() {
    const FRAMED: [&str; 5] = ["app_bar", "drawer", "list_details", "sections", "sheet"];
    let mut violations = Vec::new();
    for (snake, pascal) in components() {
        let page = crate_root()
            .join("../clay_dioxus_specimens/specimens")
            .join(format!("{snake}.rs"));
        let text = match std::fs::read_to_string(&page) {
            Ok(text) => text,
            Err(_) => {
                violations.push(format!(
                    "KIT-204: the specimens crate has no specimen page `specimens/{snake}.rs`"
                ));
                continue;
            }
        };
        if !text.contains(&format!("{pascal} {{")) {
            violations.push(format!(
                "KIT-204: the specimen page of `{pascal}` never mounts it"
            ));
        }
        if FRAMED.contains(&snake.as_str()) {
            for width in ["COMPACT_PX", "EXPANDED_PX"] {
                if !text.contains(width) {
                    violations.push(format!("KIT-204: no `{pascal}` frame at {width}"));
                }
            }
        }
    }
    assert_no_violations(violations);
}

/// `KIT-200`: a headless part sets no inline style but custom properties (`--ct-*`), which its
/// stylesheet reads. (The harness refuses the same at run time.)
#[test]
fn contract_parts_set_only_custom_properties_inline() {
    let mut violations = Vec::new();
    for source in headless_parts() {
        violations.extend(source.violations("KIT-200", |line| {
            let inline = line.contains("style:") || line.contains("\"style\":");
            (inline && !line.contains("\"--ct-")).then(|| {
                format!(
                    "inline style that is not a `--ct-*` custom property: {}",
                    line.trim()
                )
            })
        }));
    }
    assert_no_violations(violations);
}

/// `KIT-201`, `KIT-202`, `KIT-203`: a contract component's skin is a stylesheet keyed on its root
/// class, imported into the `kit` layer, with no `!important` (layers order the cascade, not
/// weight), no `@apply`, no fetching `url()`, and a `prefers-reduced-motion` guard wherever it
/// transitions or animates. The generated bundle carries it, so a skin edited without
/// regenerating the bundle fails here.
#[test]
fn contract_skins_are_stylesheets_in_the_kit_layer() {
    let entry = read(&crate_root().join("tailwind.css"));
    let bundle = read(&crate_root().join("assets/tailwind.css"));
    let mut violations = Vec::new();
    for (snake, _) in components() {
        let root = root_class(&snake);
        let path = format!("styles/kit/{snake}.css");
        let css = read(&crate_root().join(&path));
        if !entry
            .text
            .contains(&format!("@import \"./{path}\" layer(kit);"))
        {
            violations.push(format!(
                "KIT-202: tailwind.css does not import {path} into `layer(kit)`"
            ));
        }
        if !css.text.contains(&format!(".{root}")) {
            violations.push(format!("KIT-201: {path} never styles `.{root}`"));
        }
        if !bundle.text.contains(&format!(".{root}")) {
            violations.push(format!(
                "KIT-202: assets/tailwind.css has no `.{root}`: regenerate the bundle"
            ));
        }
        for banned in ["!important", "@apply", "url("] {
            if css.text.contains(banned) {
                violations.push(format!("KIT-202: {path} uses `{banned}`"));
            }
        }
        let animates = css.text.contains("transition") || css.text.contains("animation");
        if animates && !css.text.contains("prefers-reduced-motion") {
            violations.push(format!(
                "KIT-203: {path} animates without a `prefers-reduced-motion` guard"
            ));
        }
    }
    assert_no_violations(violations);
}

/// `KIT-206`: the identity sheet is imported into the `identity` layer, declared between `kit` and
/// `user`, and every component token a skin reads (`--ct-<component>-radius|border|shadow|fill`)
/// is listed in `styles/tokens.css`, so the theme surface cannot grow unrecorded.
#[test]
fn component_tokens_are_listed_and_the_identity_layer_sits_between_kit_and_user() {
    let entry = read(&crate_root().join("tailwind.css"));
    let bundle = read(&crate_root().join("assets/tailwind.css"));
    let mut violations = Vec::new();
    for (what, text) in [
        ("tailwind.css", &entry.text),
        ("assets/tailwind.css", &bundle.text),
    ] {
        if !text.contains("@layer reset, tokens, kit, identity, user;") {
            violations.push(format!(
                "KIT-206: {what} does not declare `@layer reset, tokens, kit, identity, user;`"
            ));
        }
    }
    if !entry
        .text
        .contains("@import \"./styles/identity/daytxt.css\" layer(identity);")
    {
        violations.push(
            "KIT-206: tailwind.css does not import the identity sheet into `layer(identity)`"
                .into(),
        );
    }
    let listed = read(&crate_root().join("styles/tokens.css")).text;
    for entry in std::fs::read_dir(crate_root().join("styles/kit")).expect("the skins directory") {
        let path = entry.expect("a directory entry").path();
        let css = read(&path);
        for (at, _) in css.text.match_indices("var(--ct-") {
            let name: String = css.text[at + 4..]
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '-')
                .collect();
            let kind = name.rsplit('-').next().unwrap_or_default();
            let is_component_token = ["radius", "border", "divider", "shadow", "fill"]
                .contains(&kind)
                && !name.starts_with("--ct-dimension-")
                && !name.starts_with("--ct-color-")
                && !name.starts_with("--ct-elevation-");
            if is_component_token && !listed.contains(&name) {
                violations.push(format!(
                    "KIT-206: {} reads `{name}`, which styles/tokens.css does not list",
                    css.path
                ));
            }
        }
    }
    assert_no_violations(violations);
}

/// `KIT-102`: one z-order scale. A skin never writes a number as a `z-index`: it names a layer
/// token (`var(--ct-layer-sheet)`), and every such token exists in `semantic.tokens.json`, whose
/// scale runs content, raised, app bar, drawer, sheet, toast, gesture, lowest first. (A sheet
/// opened from the sidebar covers it, so the sheet is above the drawer.)
#[test]
fn skins_stack_on_the_layer_scale_and_never_name_a_z_index_number() {
    const SCALE: [&str; 7] = [
        "content", "raised", "app-bar", "drawer", "sheet", "toast", "gesture",
    ];
    let tokens: serde_json::Value =
        serde_json::from_str(&read(&crate_root().join("styles/tokens/semantic.tokens.json")).text)
            .expect("semantic tokens are JSON");
    let layers = tokens["layer"].as_object().expect("a layer token group");
    let mut previous = None;
    for name in SCALE {
        let z = layers[name]["$value"]
            .as_i64()
            .unwrap_or_else(|| panic!("layer.{name} is a number"));
        assert!(
            previous < Some(z),
            "{name} is not above the layer before it"
        );
        previous = Some(z);
    }
    assert_eq!(
        layers.keys().filter(|key| !key.starts_with('$')).count(),
        SCALE.len(),
        "a layer token this test does not know, or the reverse"
    );
    let mut violations = Vec::new();
    for entry in std::fs::read_dir(crate_root().join("styles/kit")).expect("the skins directory") {
        let path = entry.expect("directory entry").path();
        let css = read(&path);
        for (index, line) in css.text.lines().enumerate() {
            let Some((_, value)) = line.trim().split_once("z-index:") else {
                continue;
            };
            let value = value.trim().trim_end_matches(';');
            let token = value
                .strip_prefix("var(--ct-layer-")
                .and_then(|rest| rest.strip_suffix(')'));
            if !token.is_some_and(|name| SCALE.contains(&name)) {
                violations.push(format!(
                    "KIT-102: {}:{}: `z-index: {value}` is not a layer token",
                    css.path,
                    index + 1
                ));
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
