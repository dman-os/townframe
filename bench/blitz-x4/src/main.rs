//! X4 native receiver measurement (docs/scratch/pierre-spikes.md, "X4 receivers").
//!
//! Input: the wire batches the browser run captured (`window.__clayBatches`, in `direct.js`'s flat
//! form) as `{"mark": N, "batches": [[ops, strings], ...]}`; batches before `mark` build the
//! surface, the rest are the scroll commits (50-row window shifts). Each mode replays them into a
//! fresh headless Blitz document and times every commit; nothing is painted, and the scroller's
//! scroll offset is not moved (the browser run's scrollTop is not replayed).
//!
//! - `direct`: `DioxusState::writer` over a host-owned container; the batch drives Blitz's
//!   mutator through the Dioxus stack machine. No retained tree, no host `VirtualDom`.
//! - `today`: what `clay_dioxus` does: `SurfaceTree::apply`, then a host `VirtualDom` re-renders
//!   `SurfaceRoot` and its mutations are written into the same kind of document.
//!
//! Output: one line per commit, `mode rep commit ops tree_ms diff_ms apply_ms resolve_ms`.
use blitz_dom::BaseDocument;
use blitz_dom::DocumentConfig;
use blitz_dom::LocalName;
use blitz_dom::QualName;
use blitz_dom::StyleThreading;
use blitz_dom::ns;
use blitz_traits::shell::ColorScheme;
use blitz_traits::shell::Viewport;
use clay_surface::ElementHandles;
use clay_surface::EventResponse;
use clay_surface::Mutation;
use clay_surface::SurfaceProjection;
use clay_surface::SurfaceRoot;
use clay_surface::SurfaceTree;
use dioxus::prelude::*;
use dioxus_core::ScopeId;
use dioxus_native_dom::DioxusDocument;
use dioxus_native_dom::DioxusState;
use dioxus_native_dom::MutationBuffer;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

const NONE: u32 = u32::MAX;

fn decode(ops: &[u32], strings: &[u16]) -> Vec<Mutation<()>> {
    let mut out = Vec::new();
    let mut at = 0;
    let sub = |o: u32, l: u32| String::from_utf16(&strings[o as usize..(o + l) as usize]).expect("blob is UTF-16");
    while at < ops.len() {
        let op = ops[at];
        at += 1;
        let mut arg = || {
            let v = ops[at];
            at += 1;
            v
        };
        match op {
            0 => out.push(Mutation::PushId(arg())),
            1 => out.push(Mutation::SetId(arg())),
            2 => out.push(Mutation::Child(arg())),
            3 => out.push(Mutation::Pop),
            4 => {
                let tag = sub(arg(), arg());
                let (o, l) = (arg(), arg());
                out.push(Mutation::CreateElement((tag, (l != NONE).then(|| sub(o, l)))));
            }
            5 => out.push(Mutation::CreateText(sub(arg(), arg()))),
            6 => out.push(Mutation::CloneNode),
            7 => out.push(Mutation::AppendChildren(arg())),
            8 => out.push(Mutation::ReplaceWith(arg())),
            9 => out.push(Mutation::InsertAfter(arg())),
            10 => out.push(Mutation::InsertBefore(arg())),
            11 => {
                use clay_surface::AttributeValue as V;
                let name = sub(arg(), arg());
                let (o, l) = (arg(), arg());
                let ns = (l != NONE).then(|| sub(o, l));
                let value = match arg() {
                    0 => V::Text(sub(arg(), arg())),
                    1 => V::Boolean(true),
                    _ => V::None,
                };
                out.push(Mutation::SetAttribute((name, ns, value)));
            }
            12 => out.push(Mutation::SetText(sub(arg(), arg()))),
            13 => out.push(Mutation::AddEventListener(sub(arg(), arg()))),
            14 => out.push(Mutation::RemoveEventListener(sub(arg(), arg()))),
            15 => out.push(Mutation::Remove),
            other => panic!("unknown op {other}"),
        }
    }
    out
}

fn config(sequential: bool) -> DocumentConfig {
    DocumentConfig {
        viewport: Some(Viewport::new(1280, 900, 1.0, ColorScheme::Light)),
        style_threading: if sequential { StyleThreading::Sequential } else { StyleThreading::Parallel },
        ..Default::default()
    }
}

fn qn(name: &str) -> QualName {
    QualName { prefix: None, ns: ns!(html), local: LocalName::from(name) }
}

#[derive(Clone)]
struct Shared(Arc<Mutex<SurfaceTree<()>>>);

fn app() -> Element {
    let shared = use_context::<Shared>();
    let projection = use_hook(|| SurfaceProjection::<()> {
        tree: shared.0.clone(),
        dispatch: Rc::new(|_| EventResponse::default()),
        component: Rc::new(|_| rsx! {}),
        handles: ElementHandles::default(),
    });
    let stamp = shared.0.lock().unwrap().root_stamp();
    rsx! { SurfaceRoot::<()> { projection, stamp } }
}

/// Live nodes under the document root: a check that both modes built the same tree.
fn count(doc: &BaseDocument) -> usize {
    fn walk(doc: &BaseDocument, id: blitz_dom::NodeId) -> usize {
        let node = doc.get_node(id).expect("child exists");
        1 + node.children.iter().map(|&c| walk(doc, c)).sum::<usize>()
    }
    walk(doc, doc.root_node().id)
}

fn ms(since: Instant) -> f64 {
    since.elapsed().as_secs_f64() * 1000.0
}

fn run_direct(batches: &[Vec<Mutation<()>>], mark: usize, rep: usize, sequential: bool) {
    let mut doc = BaseDocument::new(config(sequential));
    let container = {
        let mut m = doc.mutate();
        let html = m.create_element(qn("html"), vec![]);
        let root = m.doc.root_node().id;
        m.append_children(root, &[html]);
        let body = m.create_element(qn("body"), vec![]);
        m.append_children(html, &[body]);
        let container = m.create_element(qn("main"), vec![]);
        m.append_children(body, &[container]);
        container
    };
    let mut state = DioxusState::create(container);
    for (index, batch) in batches.iter().enumerate() {
        let applied = Instant::now();
        {
            let mut writer = state.writer(&mut doc);
            clay_surface::replay(batch, &mut writer).expect("replay");
        }
        let apply_ms = ms(applied);
        let resolved = Instant::now();
        doc.resolve(0.0);
        let resolve_ms = ms(resolved);
        if index + 1 == batches.len() {
            eprintln!("direct rep {rep}: {} nodes after the last batch", count(&doc));
        }
        if index >= mark {
            println!("direct {rep} {} {} 0 0 {apply_ms:.3} {resolve_ms:.3}", index - mark, batch.len());
        }
    }
}

fn run_today(batches: &[Vec<Mutation<()>>], mark: usize, rep: usize, sequential: bool) {
    let tree = Arc::new(Mutex::new(SurfaceTree::<()>::new()));
    let vdom = VirtualDom::new(app);
    vdom.provide_root_context(Shared(tree.clone()));
    let mut doc = DioxusDocument::new(vdom, config(sequential));
    doc.initial_build();
    for (index, batch) in batches.iter().enumerate() {
        let started = Instant::now();
        tree.lock().unwrap().apply(batch).expect("tree accepts the batch");
        let tree_ms = ms(started);
        let started = Instant::now();
        doc.vdom.mark_dirty(ScopeId::APP);
        let mut buffer = MutationBuffer::default();
        doc.vdom.render_immediate(&mut buffer);
        let diff_ms = ms(started);
        let started = Instant::now();
        {
            let mut inner = doc.inner.borrow_mut();
            let mut writer = doc.vdom_state.writer(&mut inner);
            buffer.replay(&mut writer);
        }
        let apply_ms = ms(started);
        let started = Instant::now();
        doc.inner.borrow_mut().resolve(0.0);
        let resolve_ms = ms(started);
        if index + 1 == batches.len() {
            eprintln!("today rep {rep}: {} nodes after the last batch", count(&doc.inner.borrow()));
        }
        if index >= mark {
            println!("today {rep} {} {} {tree_ms:.3} {diff_ms:.3} {apply_ms:.3} {resolve_ms:.3}", index - mark, batch.len());
        }
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().expect("usage: blitz_x4 <batches.json> [reps] [--sequential] [--only direct|today]");
    let rest: Vec<String> = args.collect();
    let reps: usize = rest.iter().find_map(|a| a.parse().ok()).unwrap_or(5);
    let sequential = rest.iter().any(|a| a == "--sequential");
    let only = rest.iter().position(|a| a == "--only").map(|i| rest[i + 1].clone());
    let parsed: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(path).expect("batches file")).expect("JSON");
    let mark = parsed["mark"].as_u64().expect("mark") as usize;
    let batches: Vec<Vec<Mutation<()>>> = parsed["batches"]
        .as_array()
        .expect("batches")
        .iter()
        .map(|b| {
            let ops: Vec<u32> = b[0].as_array().expect("ops").iter().map(|v| v.as_u64().expect("op") as u32).collect();
            let strings: Vec<u16> = b[1].as_str().expect("strings").encode_utf16().collect();
            decode(&ops, &strings)
        })
        .collect();
    eprintln!("{} batches, mark {mark}, {} commits, sequential={sequential}", batches.len(), batches.len() - mark);
    for rep in 0..reps {
        if only.as_deref() != Some("today") {
            run_direct(&batches, mark, rep, sequential);
        }
        if only.as_deref() != Some("direct") {
            run_today(&batches, mark, rep, sequential);
        }
    }
}
