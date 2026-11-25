// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/getFiletypeFromFileName.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the extension table is a sorted slice generated from the
// upstream object literal (later duplicate keys win, as in JavaScript). The
// module-level mutable custom-extension map and its version counter become an
// explicit `CustomExtensions` value the caller owns, because Rust has no
// ambient mutable module state; `replaceCustomExtensions`, `setCustomExtension`,
// `getCustomExtensionsVersion` and `getCustomExtensionsMap` are methods on it.
// The two regexes (`/\.([^/\\]+\.[^/\\]+)$/`, `/\.([^.]+)$/`) and the
// `split(/[/\\]/)` are hand scanners.

use std::collections::HashMap;

use crate::types::options::ExtensionFormatMap;
use crate::types::patch::SupportedLanguages;

/// `EXTENSION_TO_FILE_FORMAT`, sorted by key for binary search.
pub static EXTENSION_TO_FILE_FORMAT: &[(&str, &str)] = &[
    ("1c", "1c"),
    ("Appraisals", "ruby"),
    ("BUCK", "python"),
    ("BUILD", "python"),
    ("Berksfile", "ruby"),
    ("Brewfile", "ruby"),
    ("CMakeLists.txt", "cmake"),
    ("CODEOWNERS", "codeowners"),
    ("COMMIT_EDITMSG", "git-commit"),
    ("Capfile", "ruby"),
    ("Dangerfile", "ruby"),
    ("Dockerfile", "dockerfile"),
    ("Gemfile", "ruby"),
    ("Guardfile", "ruby"),
    ("Makefile", "makefile"),
    ("P", "prolog"),
    ("PACKAGE", "python"),
    ("R", "r"),
    ("Rakefile", "ruby"),
    ("Thorfile", "ruby"),
    ("Tiltfile", "python"),
    ("Vagrantfile", "ruby"),
    ("WORKSPACE", "python"),
    ("abap", "abap"),
    ("ada", "ada"),
    ("adb", "ada"),
    ("ado", "stata"),
    ("adoc", "asciidoc"),
    ("ads", "ada"),
    ("apl", "apl"),
    ("applescript", "applescript"),
    ("ara", "ara"),
    ("arb", "ruby"),
    ("as", "actionscript-3"),
    ("asciidoc", "asciidoc"),
    ("asm", "asm"),
    ("astro", "astro"),
    ("awk", "awk"),
    ("bal", "ballerina"),
    ("bas", "vb"),
    ("bash", "zsh"),
    ("bat", "cmd"),
    ("bazel", "python"),
    ("be", "berry"),
    ("beancount", "beancount"),
    ("bib", "bibtex"),
    ("bicep", "bicep"),
    ("blade.php", "blade"),
    ("bsl", "bsl"),
    ("builder", "ruby"),
    ("bxl", "python"),
    ("bzl", "python"),
    ("bzlmod", "python"),
    ("c", "c"),
    ("cairo", "cairo"),
    ("cbl", "cobol"),
    ("cc", "cpp"),
    ("cdc", "cdc"),
    ("cfg", "ini"),
    ("cjs", "javascript"),
    ("cl", "lisp"),
    ("clar", "clarity"),
    ("clj", "clojure"),
    ("cljc", "clojure"),
    ("cljs", "clojure"),
    ("cls", "tex"),
    ("cmake", "cmake"),
    ("cmd", "cmd"),
    ("cob", "cobol"),
    ("cobol", "cobol"),
    ("coffee", "coffeescript"),
    ("component.html", "angular-html"),
    ("component.ts", "angular-ts"),
    ("conf", "nginx"),
    ("config", "ssh-config"),
    ("cpp", "cpp"),
    ("cql", "cql"),
    ("cr", "crystal"),
    ("cs", "csharp"),
    ("cshtml", "razor"),
    ("css", "css"),
    ("csv", "csv"),
    ("cts", "typescript"),
    ("cue", "cue"),
    ("cxx", "cpp"),
    ("cyp", "cypher"),
    ("cypher", "cypher"),
    ("d", "d"),
    ("dart", "dart"),
    ("dax", "dax"),
    ("desktop", "desktop"),
    ("device", "systemd"),
    ("diff", "diff"),
    ("dm", "dream-maker"),
    ("do", "stata"),
    ("dockerfile", "dockerfile"),
    ("dta", "stata"),
    ("edge", "edge"),
    ("el", "emacs-lisp"),
    ("elm", "elm"),
    ("env", "dotenv"),
    ("erb", "erb"),
    ("erl", "erlang"),
    ("ex", "elixir"),
    ("exs", "elixir"),
    ("f", "fortran-fixed-form"),
    ("f03", "f03"),
    ("f08", "f08"),
    ("f18", "f18"),
    ("f77", "f77"),
    ("f90", "fortran-free-form"),
    ("f95", "fortran-free-form"),
    ("feature", "gherkin"),
    ("fish", "fish"),
    ("fnl", "fennel"),
    ("for", "fortran-fixed-form"),
    ("frag", "glsl"),
    ("fs", "fsharp"),
    ("fsi", "fsharp"),
    ("fsx", "fsharp"),
    ("ftl", "ftl"),
    ("fx", "hlsl"),
    ("gd", "gdscript"),
    ("gdshader", "gdshader"),
    ("gemspec", "ruby"),
    ("git-rebase-todo", "git-rebase"),
    ("gjs", "glimmer-js"),
    ("gleam", "gleam"),
    ("glsl", "glsl"),
    ("gnuplot", "gnuplot"),
    ("go", "go"),
    ("gp", "gnuplot"),
    ("gql", "graphql"),
    ("graphql", "graphql"),
    ("groovy", "groovy"),
    ("gs", "genie"),
    ("gts", "glimmer-ts"),
    ("gvy", "groovy"),
    ("h", "objective-cpp"),
    ("hack", "hack"),
    ("haml", "haml"),
    ("handlebars", "handlebars"),
    ("hbs", "handlebars"),
    ("hcl", "hcl"),
    ("hh", "cpp"),
    ("hjson", "hjson"),
    ("hlsl", "hlsl"),
    ("hpp", "cpp"),
    ("hrl", "erlang"),
    ("hs", "haskell"),
    ("htaccess", "apache"),
    ("htm", "html"),
    ("html", "html"),
    ("http", "http"),
    ("hx", "haxe"),
    ("hxml", "hxml"),
    ("hy", "hy"),
    ("imba", "imba"),
    ("ini", "ini"),
    ("j2", "jinja"),
    ("jade", "pug"),
    ("java", "java"),
    ("jbuilder", "ruby"),
    ("jinja", "jinja"),
    ("jinja2", "jinja"),
    ("jison", "jison"),
    ("jl", "julia"),
    ("js", "javascript"),
    ("json", "json"),
    ("json5", "json5"),
    ("jsonc", "jsonc"),
    ("jsonl", "jsonl"),
    ("jsonnet", "jsonnet"),
    ("jssm", "jssm"),
    ("jsx", "jsx"),
    ("kql", "kusto"),
    ("kt", "kotlin"),
    ("kts", "kts"),
    ("lean", "lean4"),
    ("less", "less"),
    ("lhs", "haskell"),
    ("libsonnet", "jsonnet"),
    ("liquid", "liquid"),
    ("lisp", "lisp"),
    ("lit", "lit"),
    ("ll", "llvm"),
    ("log", "log"),
    ("logo", "logo"),
    ("lsp", "lisp"),
    ("ltx", "tex"),
    ("lua", "lua"),
    ("luau", "luau"),
    ("m", "wolfram"),
    ("makefile", "makefile"),
    ("markdown", "markdown"),
    ("marko", "marko"),
    ("mat", "matlab"),
    ("md", "markdown"),
    ("mdc", "mdc"),
    ("mdx", "mdx"),
    ("mediawiki", "wikitext"),
    ("mermaid", "mermaid"),
    ("mips", "mipsasm"),
    ("mjs", "javascript"),
    ("mk", "makefile"),
    ("ml", "ocaml"),
    ("mli", "ocaml"),
    ("mll", "ocaml"),
    ("mly", "ocaml"),
    ("mm", "objective-cpp"),
    ("mmd", "mermaid"),
    ("mojo", "mojo"),
    ("move", "move"),
    ("mts", "typescript"),
    ("nar", "narrat"),
    ("nb", "wolfram"),
    ("nf", "nextflow"),
    ("nim", "nim"),
    ("nimble", "nim"),
    ("nims", "nim"),
    ("nix", "nix"),
    ("nu", "nushell"),
    ("odin", "odin"),
    ("p", "pascal"),
    ("p6", "raku"),
    ("pas", "pascal"),
    ("patch", "diff"),
    ("pcss", "postcss"),
    ("php", "php"),
    ("phtml", "php"),
    ("pl", "prolog"),
    ("pl6", "raku"),
    ("pls", "plsql"),
    ("plt", "gnuplot"),
    ("pm", "perl"),
    ("po", "po"),
    ("podspec", "ruby"),
    ("polar", "polar"),
    ("pot", "pot"),
    ("potx", "potx"),
    ("pp", "puppet"),
    ("pq", "powerquery"),
    ("pqm", "powerquery"),
    ("prisma", "prisma"),
    ("pro", "prolog"),
    ("properties", "properties"),
    ("proto", "protobuf"),
    ("ps1", "powershell"),
    ("psd1", "powershell"),
    ("psm1", "powershell"),
    ("pug", "pug"),
    ("purs", "purescript"),
    ("py", "python"),
    ("pyi", "python"),
    ("pyw", "python"),
    ("ql", "ql"),
    ("qml", "qml"),
    ("qmldir", "qmldir"),
    ("qss", "qss"),
    ("r", "r"),
    ("rabl", "ruby"),
    ("rake", "ruby"),
    ("raku", "raku"),
    ("razor", "razor"),
    ("rb", "ruby"),
    ("rbw", "ruby"),
    ("reg", "reg"),
    ("regex", "regexp"),
    ("rel", "rel"),
    ("res", "gdresource"),
    ("rest", "http"),
    ("rkt", "racket"),
    ("rktl", "racket"),
    ("rq", "sparql"),
    ("rs", "rust"),
    ("rst", "rst"),
    ("ru", "ruby"),
    ("s", "riscv"),
    ("sas", "sas"),
    ("sass", "sass"),
    ("sc", "scala"),
    ("scala", "scala"),
    ("scm", "scheme"),
    ("scpt", "applescript"),
    ("scss", "scss"),
    ("sdbl", "sdbl"),
    ("service", "systemd"),
    ("sh", "zsh"),
    ("shader", "shaderlab"),
    ("shadergraph", "shader"),
    ("sky", "python"),
    ("sld", "scheme"),
    ("socket", "systemd"),
    ("sol", "solidity"),
    ("soy", "soy"),
    ("sparql", "sparql"),
    ("spl", "splunk"),
    ("sql", "sql"),
    ("ss", "scheme"),
    ("st", "smalltalk"),
    ("star", "python"),
    ("sty", "tex"),
    ("styl", "stylus"),
    ("stylus", "stylus"),
    ("sv", "system-verilog"),
    ("svelte", "svelte"),
    ("svh", "system-verilog"),
    ("swift", "swift"),
    ("t", "perl"),
    ("talon", "talonscript"),
    ("tasl", "tasl"),
    ("tcl", "tcl"),
    ("templ", "templ"),
    ("tex", "tex"),
    ("tf", "tf"),
    ("tfvars", "tfvars"),
    ("timer", "systemd"),
    ("toml", "toml"),
    ("tres", "gdresource"),
    ("trigger", "apex"),
    ("ts", "typescript"),
    ("tsp", "typespec"),
    ("tsv", "tsv"),
    ("tsx", "tsx"),
    ("ttl", "turtle"),
    ("twig", "twig"),
    ("typ", "typst"),
    ("v", "verilog"),
    ("vala", "vala"),
    ("vapi", "vala"),
    ("vb", "vb"),
    ("vbs", "vb"),
    ("vert", "glsl"),
    ("vh", "verilog"),
    ("vhd", "vhdl"),
    ("vhdl", "vhdl"),
    ("vim", "vimscript"),
    ("vine.ts", "vue-vine"),
    ("vue", "vue"),
    ("vv", "v"),
    ("vy", "vyper"),
    ("wasm", "wasm"),
    ("wat", "wasm"),
    ("wgsl", "wgsl"),
    ("wiki", "wikitext"),
    ("wit", "wit"),
    ("wl", "wolfram"),
    ("wy", "文言"),
    ("xml", "xml"),
    ("xsl", "xsl"),
    ("xslt", "xsl"),
    ("yaml", "yaml"),
    ("yml", "yml"),
    ("zig", "zig"),
    ("zs", "zenscript"),
    ("zsh", "zsh"),
    ("🔥", "mojo"),
];

fn builtin_format(key: &str) -> Option<&'static str> {
    EXTENSION_TO_FILE_FORMAT
        .binary_search_by_key(&key, |&(name, _)| name)
        .ok()
        .map(|index| EXTENSION_TO_FILE_FORMAT[index].1)
}

/// User-registered extension overrides (`CUSTOM_EXTENSION_TO_FILE_FORMAT`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CustomExtensions {
    map: HashMap<String, SupportedLanguages>,
    version: u32,
}

impl CustomExtensions {
    /// `replaceCustomExtensions`: replaces the whole map when `version` is newer
    /// than the current one; returns whether it did.
    pub fn replace(&mut self, version: u32, map: &ExtensionFormatMap) -> bool {
        if version <= self.version {
            return false;
        }
        self.map = map
            .iter()
            .filter_map(|(key, lang)| lang.clone().map(|lang| (key.clone(), lang)))
            .collect();
        self.version = version;
        true
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    /// `setCustomExtension`: returns false when the mapping is unchanged.
    pub fn set(&mut self, key: &str, lang: &str) -> bool {
        if self.map.get(key).is_some_and(|existing| existing == lang) {
            return false;
        }
        self.map.insert(key.to_owned(), lang.to_owned());
        self.version += 1;
        true
    }

    /// `getCustomExtensionsMap`
    pub fn to_map(&self) -> HashMap<String, SupportedLanguages> {
        self.map.clone()
    }
}

/// `getFiletypeFromFileName` without custom extensions.
pub fn get_filetype_from_file_name(file_name: &str) -> SupportedLanguages {
    get_filetype_with_custom(file_name, &CustomExtensions::default())
}

/// `getFiletypeFromFileName`.
pub fn get_filetype_with_custom(file_name: &str, custom: &CustomExtensions) -> SupportedLanguages {
    if let Some(format) = match_filetype_from_name(file_name, custom) {
        return format;
    }

    // Special files are keyed by their exact name (Dockerfile, CMakeLists.txt), but callers
    // routinely pass a repository-relative path. Retry on the basename so `docker/Dockerfile`
    // resolves the same way a top-level `Dockerfile` does. The retry only runs once the full
    // path has matched nothing, so real extensions still win (e.g. `docker/Dockerfile.ts`).
    let base_name = file_name.rsplit(['/', '\\']).next().unwrap_or(file_name);
    if base_name == file_name {
        String::from("text")
    } else {
        match_filetype_from_name(base_name, custom).unwrap_or_else(|| String::from("text"))
    }
}

fn match_filetype_from_name(
    file_name: &str,
    custom: &CustomExtensions,
) -> Option<SupportedLanguages> {
    if let Some(lang) = custom.map.get(file_name) {
        return Some(lang.clone());
    }
    // Handle special files without extensions first
    if let Some(format) = builtin_format(file_name) {
        return Some(format.to_owned());
    }

    // Try compound extensions first (e.g., .blade.php, .component.ts)
    if let Some(compound) = compound_extension(file_name) {
        if let Some(lang) = custom.map.get(compound) {
            return Some(lang.clone());
        }
        if let Some(format) = builtin_format(compound) {
            return Some(format.to_owned());
        }
    }

    // Fall back to simple extension
    let simple = simple_extension(file_name).unwrap_or("");
    if let Some(lang) = custom.map.get(simple) {
        return Some(lang.clone());
    }
    builtin_format(simple).map(str::to_owned)
}

/// `fileName.match(/\.([^/\\]+\.[^/\\]+)$/)?.[1]`: the text after the leftmost
/// `.` whose remainder has no path separator and contains an inner `.` with at
/// least one character on each side.
fn compound_extension(file_name: &str) -> Option<&str> {
    file_name.match_indices('.').find_map(|(index, _)| {
        let tail = &file_name[index + 1..];
        if tail.contains(['/', '\\']) {
            return None;
        }
        let has_inner_dot = tail
            .char_indices()
            .any(|(offset, ch)| ch == '.' && offset > 0 && offset + 1 < tail.len());
        has_inner_dot.then_some(tail)
    })
}

/// `fileName.match(/\.([^.]+)$/)?.[1]`
fn simple_extension(file_name: &str) -> Option<&str> {
    let (_, tail) = file_name.rsplit_once('.')?;
    (!tail.is_empty()).then_some(tail)
}
