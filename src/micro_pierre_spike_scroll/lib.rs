//! Pierre scroll spike (docs/scratch/pierre-spikes.md, "P2 scroll"): a micro that virtualizes a
//! synthetic plain-text file at the geometry `@pierre/diffs` uses, to find out what the Clay
//! presentation wire must carry for it. The model owns all geometry: 20 px rows, a window of
//! `ceil(ceil((viewport + 2 * overscroll) / 20) / chunk) * chunk + chunk` lines whose start is
//! quantized to `chunk`-line boundaries (`VirtualizedFile.ts:1178-1226`, `constants.ts:60-62`,
//! overscroll `Virtualizer.ts:25`), positioned by an absolutely placed window inside a content
//! box of the full file height. The DOM is read only through the `scroll` event payload.
use clay_mdk::Context;
use clay_mdk::Error;
use dioxus::prelude::*;

pub const ID: &str = "dayframe.pierre_spike_scroll";
pub const PACKAGE_VERSION: u32 = 1;
pub const AUTHORITY: &str = "org.example.dayframe.pierre_spike_scroll";
pub const VIEW: &str = "view";
pub const PANELS_BINDING: &str = "panels";

/// `constants.ts:62` (`lineHeight`).
const LINE_HEIGHT: f64 = 20.0;
/// The window geometry profiles: `(name, overscroll px on each side of the viewport, chunk lines)`.
/// `pierre` is the file viewer's (`Virtualizer.ts:25` overscroll 1000, `constants.ts:60`
/// `hunkLineCount` 50: about 200 rows); `code-view` is the code view's (`CodeView.ts:763`
/// overscroll 200, `constants.ts:67-70` `hunkLineCount` 1: about 51 rows). The default is
/// `code-view`, the largest profile whose first window still fits the surface budgets with a
/// few spans per line (see the `budgets` test).
const PROFILES: [(&str, f64, usize); 3] = [
    ("pierre", 1000.0, 50),
    ("mid", 500.0, 10),
    ("code-view", 200.0, 1),
];
const DEFAULT_PROFILE: usize = 2;
/// The fixed height of the scroller the spike renders, until the first scroll event reports the
/// real client height.
const SCROLLER_HEIGHT: f64 = 600.0;

pub fn manifest() -> clay_mdk::MicroManifest {
    use clay_mdk::Access;
    use clay_mdk::AttachmentDeclaration;
    use clay_mdk::BindingResolution;
    use clay_mdk::ContributionDeclaration;
    use clay_mdk::Entrypoints;
    use clay_mdk::MicroManifest;
    use clay_mdk::Necessity;
    use clay_mdk::Nsid;
    use clay_mdk::PublicationDeclaration;
    use clay_mdk::PublicationKind;
    use clay_mdk::Request;
    MicroManifest {
        entrypoints: Entrypoints::micro_client(),
        publications: vec![PublicationDeclaration {
            name: VIEW.into(),
            kind: PublicationKind::Presentation,
            contract: None,
        }],
        requires: vec![],
        configuration_requests: vec![],
        collection_contracts: vec![],
        collection_requests: vec![Request {
            binding: PANELS_BINDING.into(),
            contract: Nsid::literal(micro_layout_manifest::PANELS),
            access: Some(Access::Write),
            necessity: Necessity::Optional,
            resolution: BindingResolution::Configured,
            min_revision: None,
        }],
        contributions: vec![ContributionDeclaration {
            id: "layout-panel".into(),
            binding: PANELS_BINDING.into(),
            key: ID.into(),
            payload: serde_json::json!({
                "title": "Pierre scroll spike",
                "content": { "$attachment": 0 },
            }),
            attachments: vec![AttachmentDeclaration {
                index: 0,
                publication: VIEW.into(),
            }],
        }],
        extra: Default::default(),
    }
}

pub fn sidecar() -> clay_mdk::Sidecar {
    clay_mdk::Sidecar::new(ID, PACKAGE_VERSION, manifest()).with_authority(AUTHORITY)
}

async fn activate(context: Context) -> Result<(), Error> {
    context.publish_component(VIEW, view)?;
    Ok(())
}

clay_mdk::export_micro!(activate, manifest);

/// The window `(first line, line count)` for a scroll position: centred on the viewport, the
/// start floored to a chunk boundary, clamped to the file (`VirtualizedFile.ts:1178-1226`).
fn window(scroll_top: f64, viewport: f64, total_lines: usize, profile: usize) -> (usize, usize) {
    let (_, overscroll, chunk) = PROFILES[profile];
    let wanted = ((viewport + 2.0 * overscroll) / LINE_HEIGHT).ceil() as usize;
    let total = wanted.div_ceil(chunk) * chunk + chunk;
    if total_lines <= chunk {
        return (0, total_lines);
    }
    let center = ((scroll_top + viewport / 2.0) / LINE_HEIGHT).floor() as usize;
    let center_hunk = center / chunk;
    let ideal = center_hunk as i64 - (total / chunk / 2) as i64;
    let hunks_in_file = total_lines.div_ceil(chunk) as i64;
    let start = ideal.clamp(0, hunks_in_file) as usize * chunk;
    let clamped = if ideal < 0 {
        (total as i64 + ideal * chunk as i64) as usize
    } else {
        total
    };
    (start, clamped.min(total_lines.saturating_sub(start)))
}

/// Deterministic plain-text content of one line.
fn line_text(line: usize) -> String {
    let mixed = (line as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32;
    format!("let v{line} = step({}) ^ 0x{mixed:08x};", line % 97)
}

#[component]
fn Row(line: usize, tokens: usize) -> Element {
    let text = line_text(line);
    rsx! {
        div { style: "height:20px;line-height:20px;white-space:pre;font-family:monospace;font-size:12px",
            span { style: "display:inline-block;width:8ch;text-align:right;opacity:0.5", "{line + 1}" }
            if tokens == 1 {
                span { "{text}" }
            } else {
                // Many tiny spans per line stand in for a highlighted row's token runs.
                for token in 0..tokens {
                    span { key: "{token}", "{text.get(token % 8..).unwrap_or(&text)} " }
                }
            }
        }
    }
}

/// The rows of the current window, positioned by the model at `start * 20` px.
#[component]
fn Window(start: usize, count: usize, tokens: usize) -> Element {
    rsx! {
        div {
            "data-testid": "window",
            "data-value": "{start}",
            "data-state": "{count}",
            style: "position:absolute;left:0;right:0;top:{start as f64 * LINE_HEIGHT}px",
            for line in start..start + count {
                Row { key: "{line}", line, tokens }
            }
        }
    }
}

fn view() -> Element {
    let mut total_lines = use_signal(|| 1_000_000_usize);
    let mut tokens = use_signal(|| 1_usize);
    let mut viewport = use_signal(|| SCROLLER_HEIGHT);
    let mut profile = use_signal(|| DEFAULT_PROFILE);
    let mut range = use_signal(|| window(0.0, SCROLLER_HEIGHT, 1_000_000, DEFAULT_PROFILE));

    let (start, count) = range();
    let total_height = total_lines() as f64 * LINE_HEIGHT;
    let tokens_now = tokens();
    rsx! {
        div { style: "display:flex;flex-direction:column;gap:8px",
            div { style: "display:flex;gap:8px;flex-wrap:wrap",
                for lines in [1_000_000_usize, 3_000_000] {
                    button {
                        key: "{lines}",
                        "data-testid": "lines-{lines}",
                        onclick: move |_| {
                            total_lines.set(lines);
                            range.set(window(0.0, viewport(), lines, profile()));
                        },
                        "{lines} lines"
                    }
                }
                for (index, (name, _, _)) in PROFILES.into_iter().enumerate() {
                    button {
                        key: "{name}",
                        "data-testid": "profile-{name}",
                        onclick: move |_| {
                            profile.set(index);
                            range.set(window(0.0, viewport(), total_lines(), index));
                        },
                        "{name} window"
                    }
                }
                for per_line in [1_usize, 3, 18, 38] {
                    button {
                        key: "{per_line}",
                        "data-testid": "tokens-{per_line}",
                        onclick: move |_| tokens.set(per_line),
                        "{per_line} spans/line"
                    }
                }
            }
            div {
                "data-testid": "scroller",
                style: "height:{SCROLLER_HEIGHT}px;overflow-y:auto;position:relative;border:1px solid gray",
                onscroll: move |event| {
                    let data = event.data();
                    let client = f64::from(data.client_height());
                    if client != *viewport.peek() {
                        viewport.set(client);
                    }
                    let next = window(data.scroll_top(), client, *total_lines.peek(), *profile.peek());
                    if next != *range.peek() {
                        range.set(next);
                    }
                },
                div { style: "position:relative;height:{total_height}px",
                    Window { start, count, tokens: tokens_now }
                }
            }
        }
    }
}

/// What one window commit costs against the surface tree's budgets (`clay_surface` `tree.rs`),
/// measured by replaying the Dioxus diff the guest would ship through `SurfaceTree::apply`: the
/// same validation the host runs on every `surface.apply`.
#[cfg(all(test, not(target_os = "wasi")))]
mod budgets {
    use super::*;
    use clay_surface::AttributeValue;
    use clay_surface::Mutation;
    use clay_surface::SurfaceTree;
    use dioxus_core::BorrowedAttributeValue;
    use dioxus_core::ElementId;
    use dioxus_core::ScopeId;
    use dioxus_core::VirtualDom;
    use dioxus_core::WriteMutations;
    use std::cell::Cell;

    /// Dioxus `WriteMutations` to wire `Mutation`s, one to one (the guest's `WireMutations`).
    #[derive(Default)]
    struct Recorder(Vec<Mutation>);

    impl WriteMutations for Recorder {
        fn push_id(&mut self, id: ElementId) {
            self.0.push(Mutation::PushId(id.raw() as u32));
        }
        fn set_id(&mut self, id: ElementId) {
            self.0.push(Mutation::SetId(id.raw() as u32));
        }
        fn child(&mut self, index: usize) {
            self.0.push(Mutation::Child(index as u32));
        }
        fn pop(&mut self) {
            self.0.push(Mutation::Pop);
        }
        fn create_element(&mut self, tag: &str, ns: Option<&str>) {
            self.0
                .push(Mutation::CreateElement((tag.into(), ns.map(Into::into))));
        }
        fn create_text(&mut self, value: &str) {
            self.0.push(Mutation::CreateText(value.into()));
        }
        fn clone(&mut self) {
            self.0.push(Mutation::CloneNode);
        }
        fn append_children(&mut self, m: usize) {
            self.0.push(Mutation::AppendChildren(m as u32));
        }
        fn replace_with(&mut self, m: usize) {
            self.0.push(Mutation::ReplaceWith(m as u32));
        }
        fn insert_after(&mut self, m: usize) {
            self.0.push(Mutation::InsertAfter(m as u32));
        }
        fn insert_before(&mut self, m: usize) {
            self.0.push(Mutation::InsertBefore(m as u32));
        }
        fn set_attribute(
            &mut self,
            name: &str,
            ns: Option<&str>,
            value: BorrowedAttributeValue<'_>,
        ) {
            let value = match value {
                BorrowedAttributeValue::Text(text) => AttributeValue::Text(text.into()),
                BorrowedAttributeValue::Float(float) => AttributeValue::Float(float),
                BorrowedAttributeValue::Int(int) => AttributeValue::Int(int),
                BorrowedAttributeValue::Bool(bool) => AttributeValue::Boolean(bool),
                BorrowedAttributeValue::None => AttributeValue::None,
                BorrowedAttributeValue::Any(_) => unreachable!("`Any` never crosses the wire"),
            };
            self.0.push(Mutation::SetAttribute((
                name.into(),
                ns.map(Into::into),
                value,
            )));
        }
        fn set_text(&mut self, value: &str) {
            self.0.push(Mutation::SetText(value.into()));
        }
        fn add_event_listener(&mut self, name: &str) {
            self.0.push(Mutation::AddEventListener(name.into()));
        }
        fn remove_event_listener(&mut self, name: &str) {
            self.0.push(Mutation::RemoveEventListener(name.into()));
        }
        fn remove(&mut self) {
            self.0.push(Mutation::Remove);
        }
    }

    thread_local! {
        static PROPS: Cell<(usize, usize, usize)> = const { Cell::new((0, 0, 1)) };
    }

    fn app() -> Element {
        let (start, count, tokens) = PROPS.get();
        rsx! { Window { start, count, tokens } }
    }

    /// One committed batch against a fresh tree, or the first window and then a move to the
    /// next one. Returns, per commit, `(mutations, retained nodes, result)`.
    fn commits(
        first: (usize, usize, usize),
        next: Option<(usize, usize, usize)>,
    ) -> Vec<(usize, usize, Result<u64, String>)> {
        PROPS.set(first);
        let mut dom = VirtualDom::new(app);
        let mut tree = SurfaceTree::<()>::new();
        let mut out = Vec::new();
        let mut commit = |recorder: Recorder, tree: &mut SurfaceTree<()>| {
            let result = tree.apply(&recorder.0).map_err(|error| error.to_string());
            out.push((recorder.0.len(), tree.node_count(), result));
        };
        let mut recorder = Recorder::default();
        dom.rebuild(&mut recorder);
        commit(recorder, &mut tree);
        if let Some(next) = next {
            PROPS.set(next);
            dom.mark_dirty(ScopeId::APP);
            let mut recorder = Recorder::default();
            dom.render_immediate(&mut recorder);
            commit(recorder, &mut tree);
        }
        out
    }

    /// The largest first window that commits, for `tokens` spans per line.
    fn max_first_window(tokens: usize) -> usize {
        (1..=400)
            .take_while(|&count| commits((0, count, tokens), None)[0].2.is_ok())
            .last()
            .unwrap_or(0)
    }

    #[test]
    fn print_window_budgets() {
        for tokens in [1, 3, 18, 38] {
            let max = max_first_window(tokens);
            let per_row =
                (commits((0, 6, tokens), None)[0].0 - commits((0, 2, tokens), None)[0].0) / 4;
            let nodes_per_row =
                (commits((0, 6, tokens), None)[0].1 - commits((0, 2, tokens), None)[0].1) / 4;
            let pierre = &commits((0, 200, tokens), None)[0];
            // One chunk move (50 lines) of the largest window that commits at all.
            let shift = commits((100, max.max(1), tokens), Some((150, max.max(1), tokens)));
            println!(
                "BUDGET tokens={tokens} mutations_per_row={per_row} nodes_per_row={nodes_per_row} \
                 max_first_window_rows={max} pierre200: mutations={} result={:?} \
                 shift50_of_{max}: mutations={} nodes_after={} result={:?}",
                pierre.0, pierre.2, shift[1].0, shift[1].1, shift[1].2,
            );
        }
    }
}
