use std::collections::BTreeMap;
use std::collections::BTreeSet;

use proc_macro2::Span;
use quote::ToTokens;
use serde::Serialize;
use syn::spanned::Spanned;
use syn::visit::Visit;

use crate::guardrails::Finding;

#[derive(Debug, Clone, Serialize)]
pub struct Metric {
    pub path: String,
    pub module: String,
    pub test: bool,
    pub generated: bool,
    pub definitions: usize,
    pub public_items: usize,
    pub longest_fn_lines: usize,
    pub longest_match_arm_lines: usize,
    pub inline_test_mod_lines: usize,
    pub methods: BTreeMap<String, usize>,
}

pub fn is_generated_source(text: &str) -> bool {
    text.lines().take(5).any(|line| line.contains("@generated"))
}

pub fn scan(path: &str, text: &str) -> (Vec<Metric>, Vec<Finding>) {
    if is_generated_source(text) {
        return (
            vec![Metric {
                path: path.into(),
                module: "module".into(),
                test: false,
                generated: true,
                definitions: 0,
                public_items: 0,
                longest_fn_lines: 0,
                longest_match_arm_lines: 0,
                inline_test_mod_lines: 0,
                methods: BTreeMap::new(),
            }],
            Vec::new(),
        );
    }
    let parsed = match syn::parse_file(text) {
        Ok(parsed) => parsed,
        Err(error) => {
            return (
                Vec::new(),
                vec![Finding::new(
                    "ERROR_PARSE",
                    path,
                    "file",
                    error.span().start().line,
                    format!("cannot inventory Rust syntax: {error}"),
                )],
            );
        }
    };
    let test = is_test_path(path);
    let mut scanner = Scanner {
        path,
        symbol: "module".into(),
        test,
        longest_fn_lines: 0,
        longest_match_arm_lines: 0,
        findings: BTreeMap::new(),
    };
    let mut metrics = Vec::new();
    modules(&mut scanner, &parsed.items, "module", test, &mut metrics);
    scanner.visit_file(&parsed);
    if let Some(root_metric) = metrics.first_mut() {
        root_metric.longest_fn_lines = scanner.longest_fn_lines;
        root_metric.longest_match_arm_lines = scanner.longest_match_arm_lines;
    }
    (metrics, scanner.findings.into_values().collect())
}

pub fn is_test_path(path: &str) -> bool {
    path.split('/')
        .any(|part| matches!(part, "tests" | "test" | "e2e" | "test2"))
        || path.ends_with("/tests.rs")
        || path.ends_with("/test.rs")
}

fn span_lines(span: Span) -> usize {
    span.end()
        .line
        .saturating_sub(span.start().line)
        .saturating_add(1)
}

fn modules(
    scanner: &mut Scanner<'_>,
    items: &[syn::Item],
    name: &str,
    test: bool,
    metrics: &mut Vec<Metric>,
) {
    let mut metric = Metric {
        path: scanner.path.into(),
        module: name.into(),
        test,
        generated: false,
        definitions: 0,
        public_items: 0,
        longest_fn_lines: 0,
        longest_match_arm_lines: 0,
        inline_test_mod_lines: 0,
        methods: BTreeMap::new(),
    };
    for item in items {
        match item {
            syn::Item::Use(_) | syn::Item::ExternCrate(_) => continue,
            syn::Item::Impl(item) => {
                let owner = item.self_ty.to_token_stream().to_string();
                *metric.methods.entry(owner).or_default() += item.items.len();
            }
            syn::Item::Mod(item) => {
                metric.definitions += 1;
                if matches!(item.vis, syn::Visibility::Public(_)) {
                    metric.public_items += 1;
                }
                let child_test = test || is_test(&item.attrs) || item.ident == "tests";
                if let Some((_, content)) = &item.content {
                    if !test && child_test {
                        let lines = span_lines(item.span());
                        metric.inline_test_mod_lines = metric.inline_test_mod_lines.max(lines);
                        scanner.budget(
                            "ERROR_INLINE_TEST_MOD",
                            &format!("{name}::{}", item.ident),
                            item.span(),
                            lines,
                            300,
                            "move large inline #[cfg(test)] modules to a sibling file so test and production edits do not conflict",
                        );
                    }
                    modules(
                        scanner,
                        content,
                        &format!("{name}::{}", item.ident),
                        child_test,
                        metrics,
                    );
                }
            }
            syn::Item::Trait(item) => {
                metric.definitions += 1;
                if matches!(item.vis, syn::Visibility::Public(_)) {
                    metric.public_items += 1;
                }
                metric
                    .methods
                    .insert(format!("trait {}", item.ident), item.items.len());
            }
            _ => {
                metric.definitions += 1;
                if item
                    .to_token_stream()
                    .into_iter()
                    .next()
                    .is_some_and(|token| token.to_string() == "pub")
                {
                    metric.public_items += 1;
                }
            }
        }
    }
    let budget = if test { 100 } else { 40 };
    scanner.budget(
        "ERROR_MODULE_ITEMS",
        name,
        items.first().map_or(Span::call_site(), Spanned::span),
        metric.definitions,
        budget,
        "split responsibilities; imports and impl blocks do not count",
    );
    for (owner, count) in &metric.methods {
        scanner.budget(
            "ERROR_TYPE_ITEMS",
            &format!("{name}::{owner}"),
            Span::call_site(),
            *count,
            if test { 80 } else { 40 },
            "reduce the type/trait surface, not just the number of impl blocks",
        );
    }
    metrics.push(metric);
}

fn is_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("test")
            || (attr.path().is_ident("cfg")
                && attr.meta.to_token_stream().to_string().contains("test"))
            || attr
                .path()
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "test")
    })
}

struct Scanner<'a> {
    path: &'a str,
    symbol: String,
    test: bool,
    longest_fn_lines: usize,
    longest_match_arm_lines: usize,
    findings: BTreeMap<(String, String), Finding>,
}

impl Scanner<'_> {
    fn flag(&mut self, code: &str, span: Span, message: &str) {
        let key = (code.to_string(), self.symbol.clone());
        if let Some(finding) = self.findings.get_mut(&key) {
            finding.actual += 1;
        } else {
            self.findings.insert(
                key,
                Finding::new(code, self.path, &self.symbol, span.start().line, message),
            );
        }
    }

    fn budget(
        &mut self,
        code: &str,
        symbol: &str,
        span: Span,
        actual: usize,
        limit: usize,
        message: &str,
    ) {
        if actual > limit {
            let mut finding = Finding::new(code, self.path, symbol, span.start().line, message);
            finding.actual = actual;
            finding.limit = limit;
            self.findings.insert((code.into(), symbol.into()), finding);
        }
    }

    fn scoped(&mut self, name: String, test: bool, body: impl FnOnce(&mut Self)) {
        let previous = std::mem::replace(&mut self.symbol, name);
        let previous_test = std::mem::replace(&mut self.test, test);
        body(self);
        self.symbol = previous;
        self.test = previous_test;
    }

    fn forbidden(&mut self, name: &str, span: Span) {
        if matches!(
            name,
            "tokio::sync::Mutex"
                | "tokio::sync::RwLock"
                | "async_lock::Mutex"
                | "async_lock::RwLock"
                | "dashmap::DashMap"
                | "dashmap::DashSet"
                | "DHashMap"
        ) {
            self.flag(
                "ERROR_SHARED_ASYNC_STATE",
                span,
                "use single-owner state/message passing; do not mechanically replace async locks with blocking locks",
            );
        }
    }

    fn check_fn_span(&mut self, span: Span) {
        let lines = span_lines(span);
        self.longest_fn_lines = self.longest_fn_lines.max(lines);
        let limit = if self.test { 260 } else { 160 };
        let symbol = self.symbol.clone();
        self.budget(
            "ERROR_FN_LENGTH",
            &symbol,
            span,
            lines,
            limit,
            "extract focused helpers so independent changes do not collide inside one large function",
        );
    }
}

impl<'ast> Visit<'ast> for Scanner<'_> {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        let child_test = self.test || is_test(&item.attrs) || item.ident == "tests";
        self.scoped(
            format!("{}::{}", self.symbol, item.ident),
            child_test,
            |scanner| syn::visit::visit_item_mod(scanner, item),
        );
    }

    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        let trait_name = item.trait_.as_ref().map_or(String::new(), |(_, name, _)| {
            format!(" as {}", name.to_token_stream())
        });
        self.scoped(
            format!(
                "{}::{}{}",
                self.symbol,
                item.self_ty.to_token_stream(),
                trait_name
            ),
            self.test,
            |scanner| syn::visit::visit_item_impl(scanner, item),
        );
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        let fn_test = self.test || is_test(&item.attrs);
        self.scoped(
            format!("{}::{}", self.symbol, item.sig.ident),
            fn_test,
            |scanner| {
                scanner.check_fn_span(item.block.span());
                syn::visit::visit_item_fn(scanner, item);
            },
        );
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        let fn_test = self.test || is_test(&item.attrs);
        self.scoped(
            format!("{}::{}", self.symbol, item.sig.ident),
            fn_test,
            |scanner| {
                scanner.check_fn_span(item.block.span());
                syn::visit::visit_impl_item_fn(scanner, item);
            },
        );
    }

    fn visit_expr_match(&mut self, expr: &'ast syn::ExprMatch) {
        let limit = if self.test { 160 } else { 90 };
        for arm in &expr.arms {
            let lines = span_lines(arm.body.span());
            self.longest_match_arm_lines = self.longest_match_arm_lines.max(lines);
            let pat = arm.pat.to_token_stream().to_string();
            let short_pat: String = pat.chars().take(48).collect();
            let symbol = format!("{}::match({})", self.symbol, short_pat);
            self.budget(
                "ERROR_MATCH_ARM_LENGTH",
                &symbol,
                arm.span(),
                lines,
                limit,
                "delegate large match arms to dedicated handlers to avoid dispatch-hub merge conflicts",
            );
        }
        syn::visit::visit_expr_match(self, expr);
    }

    fn visit_attribute(&mut self, attr: &'ast syn::Attribute) {
        if attr.path().is_ident("allow") {
            self.flag(
                "ERROR_ALLOW_ATTRIBUTE",
                attr.span(),
                "use a narrow #[expect(..., reason = \"...\")] so obsolete suppressions fail",
            );
        }
        if attr.path().is_ident("expect") {
            let mut reason = false;
            let _parsed = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("reason") {
                    reason = !meta
                        .value()?
                        .parse::<syn::LitStr>()?
                        .value()
                        .trim()
                        .is_empty();
                }
                Ok(())
            });
            if !reason {
                self.flag(
                    "ERROR_LINT_REASON",
                    attr.span(),
                    "give the lint expectation a nonempty reason",
                );
            }
        }
        if attr.path().is_ident("ignore") {
            let has_reason = match &attr.meta {
                syn::Meta::NameValue(nv) => {
                    if let syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(lit),
                        ..
                    }) = &nv.value
                    {
                        !lit.value().trim().is_empty()
                    } else {
                        false
                    }
                }
                _ => false,
            };
            if !has_reason {
                self.flag(
                    "ERROR_IGNORED_TEST",
                    attr.span(),
                    "do not silently remove test coverage; require an explicit, reasoned exception",
                );
            }
        }
        syn::visit::visit_attribute(self, attr);
    }

    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        fn paths(tree: &syn::UseTree, prefix: String, out: &mut Vec<String>) {
            match tree {
                syn::UseTree::Path(path) => {
                    paths(&path.tree, format!("{prefix}{}::", path.ident), out)
                }
                syn::UseTree::Name(name) => out.push(format!("{prefix}{}", name.ident)),
                syn::UseTree::Rename(name) => out.push(format!("{prefix}{}", name.ident)),
                syn::UseTree::Group(group) => {
                    for item in &group.items {
                        paths(item, prefix.clone(), out);
                    }
                }
                syn::UseTree::Glob(_) => {}
            }
        }
        let mut imports = Vec::new();
        paths(&item.tree, String::new(), &mut imports);
        for import in imports {
            self.forbidden(&import, item.span());
        }
        if matches!(item.vis, syn::Visibility::Public(_))
            && item.tree.to_token_stream().to_string().contains('*')
        {
            self.flag(
                "ERROR_GLOB_EXPORT",
                item.span(),
                "export named items so interface changes have an explicit source",
            );
        }
        syn::visit::visit_item_use(self, item);
    }

    fn visit_path(&mut self, path: &'ast syn::Path) {
        let name = path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>()
            .join("::");
        self.forbidden(&name, path.span());
        syn::visit::visit_path(self, path);
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        if call.method == "ok"
            && let syn::Expr::MethodCall(receiver) = &*call.receiver
            && receiver.method == "send"
        {
            self.flag(
                "ERROR_SEND_DISCARDED",
                call.span(),
                "handle send failure; broadcast with zero listeners needs a specific exemption",
            );
        }
        syn::visit::visit_expr_method_call(self, call);
    }

    fn visit_lit_str(&mut self, literal: &'ast syn::LitStr) {
        let value = literal.value();
        if value.starts_with("/tmp/") && value.len() > 5 {
            self.flag(
                "ERROR_FIXED_TEMP_PATH",
                literal.span(),
                "use a per-run temporary directory instead of shared process-global state",
            );
        }
        if self.test
            && literal
                .value()
                .parse::<std::net::SocketAddr>()
                .is_ok_and(|address| address.port() != 0)
        {
            self.flag(
                "ERROR_FIXED_TEST_PORT",
                literal.span(),
                "bind port zero and retain the listener; reserving then reopening a port races",
            );
        }
        syn::visit::visit_lit_str(self, literal);
    }
}

pub fn symbol_fingerprints(text: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let Ok(parsed) = syn::parse_file(text) else {
        return out;
    };
    collect_symbol_fingerprints("module", &parsed.items, &mut out);
    out
}

fn collect_symbol_fingerprints(
    prefix: &str,
    items: &[syn::Item],
    out: &mut BTreeMap<String, String>,
) {
    for item in items {
        match item {
            syn::Item::Fn(item) => {
                let key = format!("{prefix}::fn {}", item.sig.ident);
                let hash = blake3::hash(item.to_token_stream().to_string().as_bytes())
                    .to_hex()
                    .to_string();
                out.insert(key, hash);
            }
            syn::Item::Struct(item) => {
                let key = format!("{prefix}::struct {}", item.ident);
                let hash = blake3::hash(item.to_token_stream().to_string().as_bytes())
                    .to_hex()
                    .to_string();
                out.insert(key, hash);
            }
            syn::Item::Enum(item) => {
                let key = format!("{prefix}::enum {}", item.ident);
                let hash = blake3::hash(item.to_token_stream().to_string().as_bytes())
                    .to_hex()
                    .to_string();
                out.insert(key, hash);
            }
            syn::Item::Trait(item) => {
                let key = format!("{prefix}::trait {}", item.ident);
                let hash = blake3::hash(item.to_token_stream().to_string().as_bytes())
                    .to_hex()
                    .to_string();
                out.insert(key, hash);
            }
            syn::Item::Impl(item) => {
                let ty = item.self_ty.to_token_stream().to_string();
                for impl_item in &item.items {
                    if let syn::ImplItem::Fn(method) = impl_item {
                        let key = format!("{prefix}::impl {ty}::fn {}", method.sig.ident);
                        let hash = blake3::hash(method.to_token_stream().to_string().as_bytes())
                            .to_hex()
                            .to_string();
                        out.insert(key, hash);
                    }
                }
            }
            syn::Item::Mod(item) => {
                if let Some((_, content)) = &item.content {
                    collect_symbol_fingerprints(&format!("{prefix}::{}", item.ident), content, out);
                }
            }
            _ => {}
        }
    }
}

pub fn changed_symbols(base_text: &str, current_text: &str) -> BTreeSet<String> {
    let base = symbol_fingerprints(base_text);
    let current = symbol_fingerprints(current_text);
    let mut changed = BTreeSet::new();
    for (symbol, hash) in &current {
        if base.get(symbol) != Some(hash) {
            changed.insert(symbol.clone());
        }
    }
    for symbol in base.keys() {
        if !current.contains_key(symbol) {
            changed.insert(symbol.clone());
        }
    }
    changed
}
