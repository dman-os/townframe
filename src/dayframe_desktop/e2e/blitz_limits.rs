//! Pins of open Blitz limits, at the document level (no window, no accessibility bus, no GPU), so
//! the day an upstream fix lands the pin that covers it changes and says so. Each test states
//! what it pins and what flips when the limit is fixed. See `.agents/skills/blitz-dioxus-native-limits`.
use anyrender_blitz::ImageRenderer;
use anyrender_vello_cpu_blitz::VelloCpuImageRenderer;
use blitz_dom::DocumentConfig;
use blitz_html::HtmlDocument;
use blitz_html::HtmlProvider;
use blitz_traits::shell::ColorScheme;
use blitz_traits::shell::Viewport;
use std::sync::Arc;

/// The stylesheet the apps ship (`ui::FoundationStyles` links the same file).
const TAILWIND: &str = include_str!("../../ui/assets/tailwind.css");

/// Side of the square viewport and canvas the paint pins draw into.
const SIZE: u32 = 100;

fn parse(html: &str) -> HtmlDocument {
    HtmlDocument::from_html(
        html,
        DocumentConfig {
            viewport: Some(Viewport::new(SIZE, SIZE, 1.0, ColorScheme::Light)),
            html_parser_provider: Some(Arc::new(HtmlProvider) as _),
            ..Default::default()
        },
    )
}

/// Parse `body` under the shipped stylesheet, resolve style and layout, paint it with the CPU
/// renderer and return an RGBA lookup `(x, y) -> [r, g, b, a]`.
fn paint(body: &str) -> impl Fn(u32, u32) -> [u8; 4] {
    let mut document = parse(&format!(
        "<html><head><style>{TAILWIND}</style></head><body style=\"margin:0;background:white\">{body}</body></html>"
    ));
    document.resolve(0.0);
    let mut pixels = Vec::new();
    VelloCpuImageRenderer::new(SIZE, SIZE).render_to_vec(
        |scene| blitz_paint::paint_scene(scene, &mut document, 1.0, SIZE, SIZE, 0, 0),
        &mut pixels,
    );
    move |x, y| {
        let at = ((y * SIZE + x) * 4) as usize;
        pixels[at..at + 4].try_into().expect("four channels")
    }
}

const WHITE: [u8; 4] = [255, 255, 255, 255];

/// S1 saw the native showcase panic at startup when an AT-SPI bus was live: the platform adapter
/// asks for the accessibility tree (`InitialTreeRequested`, handled in `blitz-shell`'s
/// `application.rs`) before the first style resolution, and `BaseDocument::build_accessibility_tree`
/// reads every element's primary style through `ElementStyles::primary()`, which unwraps a `None`
/// on an element that has not been styled yet (`blitz-dom/src/accessibility.rs`,
/// `is_hidden_from_accessibility_tree`, via stylo `data.rs:187`).
///
/// This pins the panic on an unresolved document and that a resolved one is fine. A fix is either
/// `get_primary()` in `is_hidden_from_accessibility_tree` (treat an unstyled element as visible)
/// or `resolve()` before `build_accessibility_tree()` in the `InitialTreeRequested` arm; either
/// way the first assertion below flips and this test must be inverted.
#[test]
fn building_the_accessibility_tree_before_the_first_style_resolution_panics() {
    let tree_panics = |resolve: bool| {
        let mut document = parse("<html><body><div><button>x</button></div></body></html>");
        if resolve {
            document.resolve(0.0);
        }
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            document.build_accessibility_tree();
        }))
        .is_err()
    };
    assert!(
        tree_panics(false),
        "blitz-dom no longer panics building the tree of an unresolved document: invert this pin"
    );
    assert!(
        !tree_panics(true),
        "a resolved document must build its accessibility tree"
    );
}

/// S1 reported that `border` utilities drew nothing in the native showcase. Painted headlessly
/// with the shipped stylesheet and the CPU renderer they do: `border` draws all four sides, and
/// `border-b-2 border-l-2` (the `NavButton` chevron) draws only those two, in `currentcolor`.
/// So the limit is not in style resolution or in `blitz-paint`'s scene; it is downstream of the
/// scene, in the window renderer (the native app paints with `anyrender_vello_hybrid`, which has
/// no headless image renderer) or in the Dioxus asset pipeline that processes the stylesheet. This
/// test is the regression guard for what is proven (the scene is right); a windowed GPU
/// reproduction is still owed before the limit can be pinned, and when it is, its pin belongs
/// beside this one.
#[test]
fn tailwind_borders_paint_their_sides_on_the_cpu_renderer() {
    let all_sides =
        paint(r#"<div class="border border-border" style="width:80px;height:80px"></div>"#);
    let border = all_sides(0, 40);
    assert_ne!(border, WHITE, "the left edge of `border` must be drawn");
    assert_eq!(all_sides(40, 0), border, "same colour on every side");
    assert_eq!(all_sides(40, 40), WHITE, "the inside stays unpainted");

    let two_sides = paint(
        r#"<div class="border-b-2 border-l-2 border-current" style="width:80px;height:80px;color:rgb(0,128,0)"></div>"#,
    );
    let green = [0, 128, 0, 255];
    assert_eq!(two_sides(0, 40), green, "left side");
    assert_eq!(two_sides(40, 79), green, "bottom side");
    assert_eq!(two_sides(40, 0), WHITE, "no top side");
}

/// S1 reported that a rounded `overflow-hidden` box did not clip its children in the native
/// showcase. Painted headlessly (the showcase `Frame` structure: a rounded clipping box around
/// another rounded clipping box) the corner is clipped and the centre shows the child, so, as with
/// borders, the scene is right and the limit is in the window renderer or asset pipeline. Same
/// status as the border guard above: this proves the scene, a windowed GPU pin is still owed.
#[test]
fn a_rounded_overflow_hidden_box_clips_its_child_on_the_cpu_renderer() {
    let frame = paint(
        r#"<div class="overflow-hidden rounded-lg bg-border p-px" style="width:100px;height:100px"><div class="relative h-full w-full overflow-hidden rounded-lg bg-surface-sunken"><div style="width:100px;height:100px;background:rgb(255,0,0)"></div></div></div>"#,
    );
    assert_eq!(frame(0, 0), WHITE, "the outer corner is clipped away");
    assert_eq!(frame(2, 2), WHITE, "the inner corner is clipped away too");
    assert_eq!(
        frame(50, 50),
        [255, 0, 0, 255],
        "the child fills the middle"
    );
}

/// K7: what keyboard focus the pinned Blitz gives host code. The pins below are the evidence for
/// the "Keyboard focus" section of `.agents/skills/blitz-dioxus-native-limits` and for the K7
/// ledger entry; each states what flips when upstream (or our fork) changes.
mod focus {
    use blitz_dom::Document as _;
    use blitz_dom::DocumentConfig;
    use blitz_traits::events::BlitzKeyEvent;
    use blitz_traits::events::DomEventKind;
    use blitz_traits::events::KeyState;
    use blitz_traits::events::UiEvent;
    use dioxus::prelude::*;
    use dioxus_html::Code;
    use dioxus_html::Key;
    use dioxus_html::Location;
    use dioxus_html::Modifiers;
    use dioxus_html::NamedKey;
    use dioxus_native_dom::DioxusDocument;
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::future::Future as _;
    use std::rc::Rc;

    /// What the page under test records: the event log, each element's mounted handle by id, and
    /// whether the dialog's keydown handler traps Tab itself (`preventDefault` plus `set_focus`).
    #[derive(Default)]
    struct Rec {
        log: RefCell<Vec<String>>,
        mounted: RefCell<HashMap<&'static str, Rc<MountedData>>>,
        trap: Cell<bool>,
    }

    fn app() -> Element {
        let rec = consume_context::<Rc<Rec>>();
        let log = |rec: &Rc<Rec>, what: &'static str, id: &'static str| {
            rec.log.borrow_mut().push(format!("{what}:{id}"));
        };
        // One element per id; `tabindex` and an optional `inert` come from the call site.
        macro_rules! el {
            ($tag:ident, $id:literal $(, $attr:ident = $val:expr)*) => {{
                let (r1, r2, r3, r4, r5) = (rec.clone(), rec.clone(), rec.clone(), rec.clone(), rec.clone());
                rsx! {
                    $tag {
                        id: $id,
                        $($attr: $val,)*
                        onmounted: move |e: MountedEvent| { r1.mounted.borrow_mut().insert($id, e.data()); },
                        onfocus: move |_| log(&r2, "focus", $id),
                        onblur: move |_| log(&r3, "blur", $id),
                        onfocusin: move |_| log(&r4, "focusin", $id),
                        onfocusout: move |_| log(&r5, "focusout", $id),
                    }
                }
            }};
        }
        let outside = el!(button, "outside");
        let first = el!(button, "first");
        let target = el!(div, "target", tabindex = "-1");
        let positive = el!(div, "positive", tabindex = "5");
        let last = el!(button, "last");
        let inert_btn = el!(button, "inert-btn");
        let trap = rec.clone();
        rsx! {
            {outside}
            div {
                id: "dialog",
                onkeydown: move |e: KeyboardEvent| {
                    if trap.trap.get() && e.key() == Key::Named(NamedKey::Tab) {
                        e.prevent_default();
                        let ids = ["first", "last"];
                        let to = if e.modifiers().shift() { ids[1] } else { ids[0] };
                        let handle = trap.mounted.borrow()[to].clone();
                        // The call runs `doc_mut()` now (inside the handler), the future is trivial.
                        drop(handle.set_focus(true));
                        trap.log.borrow_mut().push(format!("trap->{to}"));
                    }
                },
                {first}
                {target}
                {positive}
                {last}
            }
            div { inert: true, {inert_btn} }
        }
    }

    fn page() -> (DioxusDocument, Rc<Rec>) {
        let rec = Rc::new(Rec::default());
        let vdom = VirtualDom::new(app).with_root_context(Rc::clone(&rec));
        let mut doc = DioxusDocument::new(vdom, DocumentConfig::default());
        doc.initial_build();
        (doc, rec)
    }

    /// The id of the focused element; `None` when nothing is (the engine then reports the root
    /// element, which carries no id).
    fn focused(doc: &DioxusDocument) -> Option<String> {
        let inner = doc.inner();
        let node = inner.get_node(inner.get_focussed_node_id()?)?;
        node.attr(blitz_dom::LocalName::from("id"))
            .map(str::to_owned)
    }

    fn set_focus(rec: &Rec, id: &str, focus: bool) {
        let handle = rec.mounted.borrow()[id].clone();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            std::pin::pin!(handle.set_focus(focus)).poll(&mut cx),
            std::task::Poll::Ready(Ok(()))
        ));
    }

    fn tab(doc: &mut DioxusDocument, shift: bool) {
        let event = BlitzKeyEvent {
            key: Key::Named(NamedKey::Tab),
            code: Code::Tab,
            modifiers: if shift {
                Modifiers::SHIFT
            } else {
                Modifiers::empty()
            },
            location: Location::Standard,
            is_auto_repeating: false,
            is_composing: false,
            state: KeyState::Pressed,
            text: None,
        };
        doc.handle_ui_event(UiEvent::KeyDown(event));
    }

    /// Q1: host Dioxus code can focus and blur an element through `MountedData::set_focus`
    /// (`dioxus-native-dom/src/events.rs:299` calling `BaseDocument::set_focus_to`,
    /// `blitz-dom/src/document.rs:1702`), and a `tabindex="-1"` element, which Tab skips, is
    /// focusable that way (`set_focus_to` never consults `is_focussable`). Blur only clears when
    /// the element is the focused one. Both calls dispatch NO focus events (`events.rs:302` and
    /// `:306`: "TODO: queue focus events somehow"): the pin asserts the log stays empty, so when
    /// the TODO is fixed this flips and the capability may stop synthesising them.
    #[test]
    fn set_focus_moves_and_clears_focus_but_dispatches_no_focus_events() {
        let (doc, rec) = page();
        assert_eq!(focused(&doc), None);
        set_focus(&rec, "target", true);
        assert_eq!(focused(&doc).as_deref(), Some("target"));
        set_focus(&rec, "first", true);
        assert_eq!(focused(&doc).as_deref(), Some("first"));
        // Blur of an element that is not focused is a no-op.
        set_focus(&rec, "target", false);
        assert_eq!(focused(&doc).as_deref(), Some("first"));
        set_focus(&rec, "first", false);
        assert_eq!(focused(&doc), None);
        assert!(
            rec.log.borrow().is_empty(),
            "programmatic focus now dispatches events ({:?}): drop the synthesis in the capability",
            rec.log.borrow()
        );
    }

    /// Q2/Q3: Tab is the only path that dispatches `blur`, `focusout`, `focus`, `focusin`
    /// (`blitz-dom/src/events/focus.rs:5-40`, called from `events/keyboard.rs:26`), in that order,
    /// `blur`/`focusout` on the element losing focus and `focus`/`focusin` on the one gaining it.
    /// Order is document order (`traversal.rs:195`), wrapping at the end; `tabindex="-1"` is
    /// skipped; a positive `tabindex` is NOT hoisted (`node/element.rs:634-668`: `index >= 0`);
    /// `inert` is not honoured (`inert-btn` sits inside `<div inert>` and is still reached); and
    /// `get_focussed_node_id` falls back to the root element when nothing is focused
    /// (`document.rs:635-638`), so Tab with nothing focused starts from the root and lands on
    /// the first stop (an earlier reading of this test claimed it does nothing; the run refuted it).
    #[test]
    fn tab_walks_document_order_across_the_whole_document_and_dispatches_focus_events() {
        let (mut doc, rec) = page();
        tab(&mut doc, false);
        assert_eq!(
            focused(&doc).as_deref(),
            Some("outside"),
            "Tab with nothing focused starts from the root element and lands on the first stop"
        );
        assert_eq!(
            rec.log.borrow()[..],
            ["focus:outside", "focusin:outside"],
            "no blur/focusout reaches the page for the root it left"
        );
        rec.log.borrow_mut().clear();
        let mut order = Vec::new();
        for _ in 0..6 {
            tab(&mut doc, false);
            order.push(focused(&doc).unwrap());
        }
        assert_eq!(
            order,
            ["first", "positive", "last", "inert-btn", "outside", "first"],
            "document order, no tabindex=-1 stop, positive tabindex not hoisted, inert not skipped, wraps"
        );
        tab(&mut doc, true);
        assert_eq!(
            focused(&doc).as_deref(),
            Some("outside"),
            "Shift+Tab reverses"
        );

        let log = rec.log.borrow();
        let first_hop: Vec<_> = log.iter().take(4).map(String::as_str).collect();
        assert_eq!(
            first_hop,
            [
                "blur:outside",
                "focusout:outside",
                "focus:first",
                "focusin:first"
            ],
            "Tab from `outside` to `first`"
        );
    }

    /// Q3: constraining Tab to a subtree. There is no engine support (see the pin above), but the
    /// key handler can do it: a `keydown` handler that calls `prevent_default` cancels the engine's
    /// Tab traversal (`events/driver.rs:355`, `dioxus_document.rs:356`) and may move focus itself
    /// with `set_focus`, called synchronously inside the handler without a RefCell panic. Keydown
    /// targets the focused element and bubbles (`driver.rs:224`, `blitz-traits events.rs:436`), so
    /// a handler on the dialog sees it as long as focus is inside. This is the FocusScope shape
    /// available today; it needs the scope to know its own first and last tabbable (guest-side
    /// bookkeeping), not the engine.
    #[test]
    fn a_keydown_handler_can_trap_tab_by_preventing_default_and_set_focus() {
        let (mut doc, rec) = page();
        rec.trap.set(true);
        set_focus(&rec, "last", true);
        tab(&mut doc, false);
        assert_eq!(
            focused(&doc).as_deref(),
            Some("first"),
            "Tab from the last wraps to the first"
        );
        tab(&mut doc, true);
        assert_eq!(
            focused(&doc).as_deref(),
            Some("last"),
            "Shift+Tab from the first wraps to the last"
        );
        assert!(rec.log.borrow().iter().any(|l| l == "trap->first"));
        // Un-trapped, the same Tab walks on to the document's next stop.
        rec.trap.set(false);
        tab(&mut doc, false);
        assert_eq!(focused(&doc).as_deref(), Some("inert-btn"));
    }

    /// Q3/Q5: the engine knows no `transitionend`/`animationend`/`transitionrun` event at all
    /// (`blitz-traits events.rs:203-206` is the focus names; no transition or animation arm in
    /// `DomEventKind`'s `FromStr`), so KIT-108's "neither Blitz nor the guest wire delivers it"
    /// holds for Blitz.
    #[test]
    fn blitz_has_no_transition_or_animation_end_events() {
        for name in [
            "transitionend",
            "animationend",
            "transitionrun",
            "animationstart",
        ] {
            assert!(
                name.parse::<DomEventKind>().is_err(),
                "{name} is now a Blitz event: revisit KIT-108"
            );
        }
        for name in ["focus", "blur", "focusin", "focusout"] {
            assert!(name.parse::<DomEventKind>().is_ok(), "{name}");
        }
    }

    /// Opacity of `#id` after the last style resolution. (The `transition` must stay in the
    /// after-change style: swapping `.a` for `.b` alone drops it and the value jumps.)
    fn opacity(doc: &blitz_html::HtmlDocument, id: &str) -> f32 {
        let node = doc.get_element_by_id(id).expect("element by id");
        doc.get_node(node)
            .unwrap()
            .primary_styles()
            .unwrap()
            .get_effects()
            .opacity
    }

    /// Q4: `:focus` matches the focused element (`blitz-dom/src/stylo.rs:476`), but
    /// `:focus-visible` and `:focus-within` never match (`stylo.rs:477-478`, hard-coded `false`):
    /// `focus-visible:` and `focus-within:` utilities are dead on Blitz, `focus:` works.
    /// `FOCUSRING` is set together with `FOCUS` on every focus, mouse or keyboard
    /// (`node/node.rs:721`), but nothing reads it. When `:focus-visible` or `:focus-within`
    /// starts matching, the corresponding assertion flips.
    #[test]
    fn focus_matches_but_focus_visible_and_focus_within_never_do() {
        let mut doc = super::parse(
            "<html><head><style>\
             .f{opacity:1} .f:focus{opacity:.9} .f:focus-visible{opacity:.8}\
             #w{opacity:1} #w:focus-within{opacity:.7}\
             </style></head><body><div id=w><button id=b class=f>x</button></div></body></html>",
        );
        doc.resolve(0.0);
        assert_eq!(opacity(&doc, "b"), 1.0);
        let button = doc.get_element_by_id("b").unwrap();
        doc.set_focus_to(button);
        doc.resolve(0.0);
        assert_eq!(
            opacity(&doc, "b"),
            0.9,
            ":focus applies, :focus-visible does not (it would be .8, declared later)"
        );
        assert_eq!(opacity(&doc, "w"), 1.0, ":focus-within never matches");
    }

    /// Q5: CSS transitions run. A class change on an element with `transition: opacity 1s linear`
    /// is interpolated across `resolve(now)` calls (`stylo.rs:83-120` advances the animation set
    /// every resolve and marks the node for restyle; `document.rs:2078` keeps the shell redrawing
    /// while any is active). No `transitionend` reaches host code (previous test), so completion
    /// is observable only by sampling style or by a timer.
    #[test]
    fn css_transitions_interpolate_over_resolve_time() {
        let mut doc = super::parse(
            "<html><head><style>\
             .a{opacity:1;transition:opacity 1s linear} .b{opacity:0}\
             </style></head><body><div id=d class=a>x</div></body></html>",
        );
        doc.resolve(0.0);
        assert_eq!(opacity(&doc, "d"), 1.0);
        let node = doc.get_element_by_id("d").unwrap();
        doc.mutate()
            .set_attribute(node, blitz_dom::qual_name!("class"), "a b");
        doc.resolve(0.0);
        doc.resolve(0.5);
        let mid = opacity(&doc, "d");
        assert!(mid > 0.1 && mid < 0.9, "mid-transition opacity {mid}");
        doc.resolve(2.0);
        doc.resolve(2.0);
        assert_eq!(opacity(&doc, "d"), 0.0, "transition finished");
    }
}

/// S8a: which CSS features of Pierre's `style.css` (`packages/diffs/src/style.css`) the pinned
/// Blitz (`blitz-dom` / `stylo_taffy` 0.3.0-beta.4, taffy 0.15) honours, before the diff view's DOM
/// shape is frozen. A test whose feature is missing pins what Blitz does today and names what flips
/// when it lands.
mod pierre_css {
    use super::ImageRenderer as _;
    use blitz_dom::DocumentConfig;
    use blitz_html::HtmlDocument;
    use blitz_html::HtmlProvider;
    use blitz_traits::shell::ColorScheme;
    use blitz_traits::shell::Viewport;
    use std::sync::Arc;

    const WIDTH: u32 = 200;
    const HEIGHT: u32 = 100;

    /// A resolved document for `body` under `css`, in a 200x100 viewport.
    fn document(css: &str, body: &str, scheme: ColorScheme) -> HtmlDocument {
        let mut doc = HtmlDocument::from_html(
            &format!(
                "<html><head><style>{css}</style></head>\
                 <body style=\"margin:0;background:white\">{body}</body></html>"
            ),
            DocumentConfig {
                viewport: Some(Viewport::new(WIDTH, HEIGHT, 1.0, scheme)),
                html_parser_provider: Some(Arc::new(HtmlProvider) as _),
                ..Default::default()
            },
        );
        doc.resolve(0.0);
        doc
    }

    fn light(css: &str, body: &str) -> HtmlDocument {
        document(css, body, ColorScheme::Light)
    }

    /// Absolute `(x, y, width, height)` of `#id` in document space (parent offsets summed).
    fn rect(doc: &HtmlDocument, id: &str) -> (f32, f32, f32, f32) {
        let mut node = doc
            .get_node(doc.get_element_by_id(id).expect("id"))
            .unwrap();
        let size = node.final_layout().size;
        let (mut x, mut y) = (0.0, 0.0);
        loop {
            let at = node.final_layout().location;
            x += at.x;
            y += at.y;
            match node.parent {
                Some(parent) => node = doc.get_node(parent).unwrap(),
                None => return (x, y, size.width, size.height),
            }
        }
    }

    /// Paint the document and return an RGBA lookup.
    fn pixels(doc: &mut HtmlDocument) -> impl Fn(u32, u32) -> [u8; 4] {
        let mut buffer = Vec::new();
        super::VelloCpuImageRenderer::new(WIDTH, HEIGHT).render_to_vec(
            |scene| blitz_paint::paint_scene(scene, doc, 1.0, WIDTH, HEIGHT, 0, 0),
            &mut buffer,
        );
        move |x, y| {
            let at = ((y * WIDTH + x) * 4) as usize;
            buffer[at..at + 4].try_into().expect("four channels")
        }
    }

    fn opacity(doc: &HtmlDocument, id: &str) -> f32 {
        doc.get_node(doc.get_element_by_id(id).unwrap())
            .unwrap()
            .primary_styles()
            .unwrap()
            .get_effects()
            .opacity
    }

    /// `(width, height)` of `#id`.
    fn size(doc: &HtmlDocument, id: &str) -> (f32, f32) {
        let (_, _, w, h) = rect(doc, id);
        (w, h)
    }

    /// `style.css:813-834`: `[data-gutter]` and `[data-content]` are subgrids of `[data-code]`
    /// (`grid-template-rows/columns: subgrid`), so a gutter row and the content row beside it
    /// share one track. `stylo_taffy` maps `Subgrid` to "no template" (`wrapper.rs:791`,
    /// `convert.rs:811`, both `TODO: Implement subgrid and masonry`), so the child grids size
    /// their own implicit tracks. Parent rows are 30 and 10 (from the content cells); a working
    /// subgrid would put the gutter's second cell at y=30 and its columns on the parent's tracks
    /// (second column at x=40). Today the gutter stretches its own two 10px rows over the 40px it
    /// spans (second cell at y=20) and the separator's columns split evenly (x=100). When subgrid
    /// lands both assertions flip.
    #[test]
    fn subgrid_rows_and_columns_are_not_shared_with_the_parent_grid() {
        let rows = light(
            "#code{display:grid;grid-template-columns:40px 1fr;width:200px}\
             #g{display:grid;grid-template-rows:subgrid;grid-column:1;grid-row:span 2}\
             #c{display:grid;grid-template-rows:subgrid;grid-column:2;grid-row:span 2}\
             .a{height:10px}#c1{height:30px}#c2{height:10px}",
            "<div id=code><div id=g><div id=g1 class=a></div><div id=g2 class=a></div></div>\
             <div id=c><div id=c1></div><div id=c2></div></div></div>",
        );
        assert_eq!(rect(&rows, "c2").1, 30.0, "the content rows are 30 then 10");
        assert_eq!(
            rect(&rows, "g2").1,
            20.0,
            "subgrid rows now follow the parent (would be 30): invert this pin"
        );
        let columns = light(
            "#code{display:grid;grid-template-columns:40px 1fr;width:200px}\
             #sep{display:grid;grid-template-columns:subgrid;grid-column:1/-1}\
             #s1{grid-column:1;height:5px}#s2{grid-column:2;height:5px}",
            "<div id=code><div id=sep><div id=s1></div><div id=s2></div></div></div>",
        );
        assert_eq!(
            rect(&columns, "s2").0,
            100.0,
            "subgrid columns now follow the parent (would be 40): invert this pin"
        );
    }

    /// `style.css:735-741, 295-300`: the plain-grid half of Pierre's layout works. A
    /// `minmax(min-content, max-content) 1fr` code grid sizes the number column to its content
    /// and gives the rest to `1fr`; `minmax(0, 1fr)` and `1fr 1fr` (split view) resolve;
    /// `grid-column` and `grid-row: span N` place items. This is the layout the port can lean on.
    #[test]
    fn grid_tracks_minmax_fr_and_row_spans_lay_out() {
        let doc = light(
            "#code{display:grid;grid-auto-flow:dense;grid-template-columns:minmax(min-content,max-content) 1fr;width:200px}\
             #g{grid-column:1;grid-row:span 2;width:30px}#c{grid-column:2;grid-row:span 2;min-width:0}\
             #r1{height:7px}#r2{height:9px}\
             #split{display:grid;grid-template-columns:1fr 1fr;width:200px}\
             #dehy{display:grid;grid-template-columns:minmax(min-content,max-content) minmax(0,1fr);width:200px}",
            "<div id=code><div id=g></div><div id=c><div id=r1></div><div id=r2></div></div></div>\
             <div id=split><div id=l></div><div id=r></div></div>\
             <div id=dehy><div id=d1 style='width:50px'></div><div id=d2></div></div>",
        );
        assert_eq!(rect(&doc, "g").2, 30.0);
        assert_eq!(rect(&doc, "c"), (30.0, 0.0, 170.0, 16.0));
        assert_eq!(rect(&doc, "r"), (100.0, 16.0, 100.0, 0.0));
        assert_eq!(size(&doc, "d2").0, 150.0);
    }

    /// `style.css:12, 92-`: `light-dark()` (28 uses), both directly and inside a custom property
    /// consumed through `var()`, follows the viewport colour scheme.
    #[test]
    fn light_dark_follows_the_viewport_color_scheme() {
        let colors = |scheme| {
            let mut doc = document(
                ":root{color-scheme:light dark;--m:light-dark(black,white)}\
                 #a{width:20px;height:20px;background:light-dark(rgb(255,0,0),rgb(0,0,255))}\
                 #b{width:20px;height:20px;background:var(--m)}",
                "<div id=a></div><div id=b></div>",
                scheme,
            );
            let px = pixels(&mut doc);
            (px(5, 5), px(5, 25))
        };
        assert_eq!(
            colors(ColorScheme::Light),
            ([255, 0, 0, 255], [0, 0, 0, 255])
        );
        assert_eq!(
            colors(ColorScheme::Dark),
            ([0, 0, 255, 255], [255, 255, 255, 255])
        );
    }

    /// `style.css:99-100` and 43 more: `color-mix(in lab, var(--bg) 92%, var(--mixer))` with
    /// variable operands, plus `in srgb`. The srgb mix is exact; the lab mix is checked against the
    /// colour-space maths (red/blue at 50% lands at roughly rgb(193, 0, 136), blue clamped into
    /// gamut) and the 92% white/black mix is a light grey near 232.
    #[test]
    fn color_mix_in_lab_and_srgb_resolves_with_variable_operands() {
        let mut doc = light(
            "#a{width:20px;height:20px;background:color-mix(in lab, rgb(255,0,0) 50%, rgb(0,0,255))}\
             :root{--bg:#fff;--mx:black;--c:color-mix(in lab, var(--bg) 92%, var(--mx))}\
             #b{width:20px;height:20px;background:var(--c)}\
             #c{width:20px;height:20px;background:color-mix(in srgb, rgb(255,0,0) 50%, rgb(0,0,255))}",
            "<div id=a></div><div id=b></div><div id=c></div>",
        );
        let px = pixels(&mut doc);
        assert_eq!(px(5, 45), [128, 0, 128, 255], "srgb mix");
        assert_eq!(px(5, 5), [193, 0, 136, 255], "lab mix of red and blue");
        let grey = px(5, 25);
        assert!(
            (230..=234).contains(&grey[0]) && grey[0] == grey[1] && grey[1] == grey[2],
            "92% white in lab: {grey:?}"
        );
    }

    /// `style.css:932, 1347, 1405`: `lh` (and `ch`, the unit of the number column widths) resolve
    /// against the element's own line-height and font.
    #[test]
    fn lh_and_ch_units_resolve() {
        let doc = light(
            "#a{font:10px monospace;line-height:20px;height:2lh;width:5ch}\
             #b{font:10px monospace;line-height:20px;min-height:1lh}",
            "<div id=a></div><div id=b></div>",
        );
        assert_eq!(size(&doc, "a"), (30.0, 40.0), "2lh = 40, 5ch = 5 * 6px");
        assert_eq!(size(&doc, "b").1, 20.0, "min-height: 1lh");
    }

    /// `style.css:1-3`: `@layer base, theme, rendered, unsafe;` and layered rules. Layer order
    /// (later layer wins regardless of source order) and "unlayered beats layered" both hold.
    #[test]
    fn layers_order_the_cascade() {
        let mut doc = light(
            "@layer a,b;@layer b{.x{background:rgb(255,0,0)}}@layer a{.x{background:rgb(0,0,255)}}\
             .y{background:rgb(0,255,0)}@layer b{.y{background:rgb(255,0,0)}}",
            "<div class=x style='width:20px;height:20px'></div><div class=y style='width:20px;height:20px'></div>",
        );
        let px = pixels(&mut doc);
        assert_eq!(px(5, 5), [255, 0, 0, 255], "layer b is later than a");
        assert_eq!(px(5, 25), [0, 255, 0, 255], "unlayered beats layered");
    }

    /// `style.css:766, 1119, 1182, 1240`: `container-type: inline-size` and `width: 100cqi`
    /// behind `@supports (width: 1cqi)`. `query_container_size` is a stub
    /// (`blitz-dom/src/stylo.rs:1214`, "FIXME: Implement container queries"): `@container` rules
    /// never apply, `cqi` is dropped as an invalid value (the box keeps its auto width) and
    /// `@supports (width: 1cqi)` is false, so Pierre's guarded cqi blocks are skipped whole.
    /// When container queries land these flip.
    #[test]
    fn container_queries_and_cqi_units_are_unsupported() {
        let doc = light(
            "#k{container-type:inline-size;width:120px}\
             @container (min-width:100px){#m{height:7px}}\
             #w{width:50cqi;height:3px}\
             @supports (width:1cqi){#s{height:9px}}",
            "<div id=k><div id=m></div><div id=w></div><div id=s></div></div>",
        );
        assert_eq!(size(&doc, "m").1, 0.0, "@container rule does not apply");
        assert_eq!(size(&doc, "w").0, 120.0, "50cqi would be 60");
        assert_eq!(size(&doc, "s").1, 0.0, "@supports (width: 1cqi) is false");
    }

    /// `style.css:304, 794, 1119, 1240`: the `@supports` conditions Pierre uses. Standard
    /// property tests evaluate (`display:grid`, negation), the vendor probes are false (so the
    /// iOS and Firefox branches are skipped, as wanted), and so is `grid-template-columns: subgrid`.
    #[test]
    fn supports_evaluates_standard_properties_and_rejects_vendor_and_subgrid_probes() {
        let doc = light(
            "@supports (-moz-appearance:none){#moz{height:9px}}\
             @supports (-webkit-touch-callout:none){#ios{height:9px}}\
             @supports (display:grid){#grid{height:9px}}\
             @supports (grid-template-columns:subgrid){#sub{height:9px}}\
             @supports not (width:1px){#not{height:9px}}",
            "<div id=moz></div><div id=ios></div><div id=grid></div><div id=sub></div><div id=not></div>",
        );
        let heights = ["moz", "ios", "grid", "sub", "not"].map(|id| size(&doc, id).1);
        assert_eq!(heights, [0.0, 0.0, 9.0, 0.0, 0.0]);
    }

    /// `style.css:683, 1212, 1229, 1264`: `@media (pointer: fine)` / `(pointer: coarse)`. The
    /// headless device reports a fine pointer with hover and never a coarse one, so the phone
    /// block at `style.css:1229-1264` (gutter columns `34px 34px auto`) can never apply from
    /// the media query: the port must select it with a class/attribute from host knowledge.
    /// (Headless only: whether an Android window reports otherwise is unverified.)
    #[test]
    fn media_pointer_is_fine_and_never_coarse() {
        let doc = light(
            "@media (pointer:fine){#fine{height:9px}}@media (pointer:coarse){#coarse{height:9px}}\
             @media (hover:hover){#hover{height:9px}}@media (min-width:100px){#width{height:9px}}\
             @media (prefers-color-scheme:dark){#dark{height:9px}}",
            "<div id=fine></div><div id=coarse></div><div id=hover></div><div id=width></div><div id=dark></div>",
        );
        let heights = ["fine", "coarse", "hover", "width", "dark"].map(|id| size(&doc, id).1);
        assert_eq!(heights, [9.0, 0.0, 9.0, 9.0, 0.0]);
    }

    /// `style.css:4, 305`: the custom properties and `color-scheme` live under `:host`, which
    /// only matches a shadow host. Blitz renders light DOM and `:host` never matches, so every
    /// Pierre variable defined there is undefined. Scope them to a class on the container root.
    #[test]
    fn host_rules_never_match_in_light_dom() {
        let mut doc = light(
            ":host{background:rgb(255,0,0)}",
            "<div style='height:20px'></div>",
        );
        let px = pixels(&mut doc);
        assert_eq!(px(5, 5), [255, 255, 255, 255]);
    }

    /// `style.css:269, 915, 1691`: `position: sticky` for the file header and the gutter
    /// (`[data-overflow='scroll'] [data-gutter] { position: sticky; left: 0 }`). Blitz maps the
    /// value (`stylo_taffy` `convert.rs:308`) but no offset is applied: after scrolling the
    /// container by 50px a sticky `left: 0` box paints exactly like a static one, off screen. When
    /// sticky lands the sticky box stays at x=0 and this flips.
    #[test]
    fn position_sticky_scrolls_away_like_static() {
        let after_scroll = |position: &str| {
            let mut doc = light(
                &format!(
                    "#s{{width:100px;height:100px;overflow:scroll hidden}}#w{{width:400px;height:20px}}\
                     #st{{position:{position};left:0;width:30px;height:20px;background:rgb(255,0,0)}}"
                ),
                "<div id=s><div id=w><div id=st></div></div></div>",
            );
            let scroller = doc.get_element_by_id("s").unwrap();
            assert_eq!(pixels(&mut doc)(5, 5), [255, 0, 0, 255]);
            doc.scroll_to(scroller, 50.0, 0.0, blitz_dom::ScrollBehavior::Instant);
            doc.resolve(0.0);
            assert_eq!(doc.get_node(scroller).unwrap().scroll_offset().x, 50.0);
            pixels(&mut doc)(5, 5)
        };
        assert_eq!(after_scroll("static"), [255, 255, 255, 255]);
        assert_eq!(
            after_scroll("sticky"),
            [255, 255, 255, 255],
            "sticky now pins to the scrollport: invert this pin"
        );
    }

    /// `style.css:1020, 1435, 1611, 1620`: `white-space: pre | pre-wrap | nowrap | normal`, a
    /// per-column `overflow-x`, and wrapping of long words. Line counts in a 50px box of 6px
    /// glyphs. `pre` keeps newlines and never wraps (its scroll width exceeds the box), `pre-wrap`
    /// wraps, `normal` collapses the newline. `word-break: break-word` (the deprecated alias
    /// Pierre uses at `style.css:1440`) is ignored while `overflow-wrap: break-word | anywhere` and
    /// `word-break: break-all` break the word: use `overflow-wrap` in the port.
    #[test]
    fn white_space_modes_work_and_word_break_break_word_is_ignored() {
        let doc = light(
            "div{width:50px;font:10px monospace;line-height:10px}\
             #pre{white-space:pre}#wrap{white-space:pre-wrap}#normal{white-space:normal}#nowrap{white-space:nowrap}\
             .long{white-space:pre-wrap}#wb{word-break:break-word}#ba{word-break:break-all}\
             #ow{overflow-wrap:break-word}#oa{overflow-wrap:anywhere}",
            "<div id=pre>aaaa bbbb cccc\nd</div><div id=wrap>aaaa bbbb cccc\nd</div>\
             <div id=normal>aaaa bbbb cccc\nd</div><div id=nowrap>aaaa bbbb cccc</div>\
             <div class=long id=wb>aaaaaaaaaaaaaaaaaaaaaaaaaaaa</div><div class=long id=ba>aaaaaaaaaaaaaaaaaaaaaaaaaaaa</div>\
             <div class=long id=ow>aaaaaaaaaaaaaaaaaaaaaaaaaaaa</div><div class=long id=oa>aaaaaaaaaaaaaaaaaaaaaaaaaaaa</div>",
        );
        let lines = |id| (size(&doc, id).1 / 10.0) as u32;
        assert_eq!(lines("pre"), 2);
        assert_eq!(lines("wrap"), 4);
        assert_eq!(lines("normal"), 3);
        assert_eq!(lines("nowrap"), 1);
        let pre = doc.get_node(doc.get_element_by_id("pre").unwrap()).unwrap();
        assert!(
            pre.final_layout().scroll_width() > 0.0,
            "pre overflows sideways"
        );
        assert_eq!(
            lines("wb"),
            1,
            "break-word is honoured now: invert this pin"
        );
        assert_eq!(lines("ba"), 4);
        assert_eq!(lines("ow"), 4);
        assert_eq!(lines("oa"), 4);
    }

    /// `style.css:735-741`: `overflow: scroll clip` (a different value per axis) on the code
    /// grid. Parsed, with `clip` mapped to `hidden` (equivalent for a box that is not a scroll
    /// container on that axis); the content extents still report the overflow in both axes.
    #[test]
    fn two_value_overflow_scroll_clip_is_scroll_by_hidden() {
        let doc = light(
            "#a{width:50px;height:20px;overflow:scroll clip}#in{width:200px;height:50px}",
            "<div id=a><div id=in></div></div>",
        );
        let a = doc.get_node(doc.get_element_by_id("a").unwrap()).unwrap();
        let style = a.primary_styles().unwrap();
        assert_eq!(
            format!(
                "{:?}",
                (style.get_box().overflow_x, style.get_box().overflow_y)
            ),
            "(Scroll, Hidden)"
        );
        assert_eq!(a.final_layout().scroll_width(), 150.0);
    }

    /// `style.css:75, 739`: `tab-size`. Neither `blitz-dom` nor `blitz-paint` mention it
    /// (grep `tab_size`): a tab in `white-space: pre` text is one fixed width whatever `tab-size`
    /// says, so tab-indented code renders with one-space indents. When `tab-size` is honoured the
    /// `tab-size: 8` box becomes wider and this flips.
    #[test]
    fn tab_size_is_ignored() {
        let doc = light(
            "span{font:10px monospace;white-space:pre;display:inline-block}#a{tab-size:2}#b{tab-size:8}",
            "<span id=a>\tx</span><br><span id=b>\tx</span>",
        );
        assert_eq!(
            size(&doc, "a").0,
            size(&doc, "b").0,
            "tab-size now varies: invert this pin"
        );
    }

    /// `style.css:837-851`: `:has()`, used once, inside the `:not(...)` guard that selects the
    /// flex (non-subgrid) layout for `[data-overflow='scroll'] > [data-code]`. The selector
    /// parser rejects `:has` (the document's own `query_selector` errors on it too), and a rule
    /// with an invalid selector is dropped whole: even a `:not(:has(...))` that would match, and
    /// the same rule with `:has` removed (which does apply), differ. `:not()` with plain attribute
    /// lists is fine. When `:has` lands the third and fourth assertions flip.
    #[test]
    fn has_selector_drops_the_whole_rule() {
        let doc = light(
            "div{opacity:1}\
             #h1:has(b){opacity:.5}#h2:has(> b){opacity:.5}\
             #n1:not([data-x],[data-y]) > [data-code]{opacity:.6}\
             #n2:not([data-x],:has(b)) > [data-code]{opacity:.6}\
             #n3:not(:has(> [data-code] > b)) > [data-code]{opacity:.6}",
            "<div id=h1><b></b></div><div id=h2><b></b></div>\
             <div id=n1><div data-code id=n1c></div></div>\
             <div id=n2><div data-code id=n2c></div></div>\
             <div id=n3><div data-code id=n3c></div></div>",
        );
        assert_eq!(opacity(&doc, "n1c"), 0.6, ":not() of attribute selectors");
        assert_eq!(
            opacity(&doc, "h1"),
            1.0,
            ":has(b) would match: invert this pin"
        );
        assert_eq!(
            opacity(&doc, "h2"),
            1.0,
            ":has(> b) would match: invert this pin"
        );
        assert_eq!(opacity(&doc, "n2c"), 1.0, ":not(:has) guard drops the rule");
        assert_eq!(opacity(&doc, "n3c"), 1.0, ":not(:has) guard drops the rule");
        assert!(doc.query_selector("#h1:has(b)").is_err());
    }

    /// Every other selector form `style.css` uses: attribute equality
    /// (`[data-line-type='change-addition']`, 7x), compound attributes (`[data-a][data-b='x']`),
    /// the general sibling combinator (`[data-diffs-header] ~ [data-diff]`), CSS nesting with `&`
    /// (`&[data-line]`, `&::before`, nested descendants: the stylesheet is written in nested
    /// CSS), and `:is()` lists with attribute members.
    #[test]
    fn attribute_sibling_nesting_and_is_selectors_match() {
        let doc = light(
            "div{opacity:1}\
             [data-line-type='change-addition']{opacity:.7}\
             [data-a][data-b='x']{opacity:.8}\
             #sib ~ #sib2{opacity:.4}\
             #nest{ &[data-q]{opacity:.3} [data-in]{opacity:.2} }\
             #isn :is([data-u],[data-v]){opacity:.1}\
             #both:is([data-a],[data-b]):not([data-c]){opacity:.9}",
            "<div id=ad data-line-type='change-addition'></div><div id=ab data-a data-b=x></div>\
             <div id=sib></div><div id=sib2></div>\
             <div id=nest data-q><div id=ni data-in></div></div>\
             <div id=isn><div id=iu data-u></div></div><div id=both data-b></div>",
        );
        for (id, expected) in [
            ("ad", 0.7),
            ("ab", 0.8),
            ("sib2", 0.4),
            ("nest", 0.3),
            ("ni", 0.2),
            ("iu", 0.1),
            ("both", 0.9),
        ] {
            assert_eq!(opacity(&doc, id), expected, "#{id}");
        }
    }

    /// `style.css:939, 1344-1470`: `::before` / `::after` with `content`. A generated block with
    /// `min-width: 3ch` contributes to its host's shrink-to-fit width (2ch + 3ch + 1ch = 36 for
    /// the gutter buffer), and an absolutely positioned `::before` with a background paints
    /// (the `+` / `-` change indicators and the bar indicators). Generated text
    /// (`content: '+'`) is laid out as a pseudo child; its glyphs are not probed here.
    #[test]
    fn before_and_after_pseudo_elements_generate_boxes() {
        let mut doc = light(
            "#g{display:inline-block;padding-left:2ch;padding-right:1ch;font:10px monospace}\
             #g::before{content:'';display:block;min-width:3ch}\
             #ab{position:relative;width:50px;height:20px}\
             #ab::before{content:'+';display:inline-block;width:20px;height:10px;position:absolute;top:0;left:0;background:rgb(255,0,0)}",
            "<div id=g></div><div id=ab></div>",
        );
        assert_eq!(size(&doc, "g").0, 36.0);
        let at = rect(&doc, "ab");
        let px = pixels(&mut doc);
        assert_eq!(px(5, at.1 as u32 + 5), [255, 0, 0, 255]);
    }

    /// `style.css:962-970, 1070-1100`: `repeating-linear-gradient` with `calc()` stops (the
    /// buffer-row hatching), `max(0px, calc(var(--a) - var(--b)))`, the `round()` math function
    /// inside `calc` with `lh` (`calc(1lh / round(1lh / 2px))`), and nested `var()` fallbacks
    /// (`--diffs-scrollbar-gutter`).
    #[test]
    fn gradients_math_functions_and_nested_var_fallbacks_resolve() {
        let mut doc = light(
            "#a{width:20px;height:20px;background-image:repeating-linear-gradient(-45deg,transparent,transparent calc(3px*1.414),rgb(255,0,0) calc(3px*1.414),rgb(255,0,0) calc(4px*1.414));background-size:8px 8px}\
             #b{--g:8px;--s:6px;padding-bottom:max(0px,calc(var(--g) - var(--s)));font:10px monospace;line-height:20px}\
             #c{font:10px monospace;line-height:20px;--r:calc(1lh / round(1lh / 3px));width:var(--r);height:5px}\
             #d{width:var(--no, var(--no2, 17px));height:5px}",
            "<div id=a></div><div id=b>x</div><div id=c></div><div id=d></div>",
        );
        let px = pixels(&mut doc);
        let striped = (0..8)
            .flat_map(|x| (0..8).map(move |y| (x, y)))
            .filter(|&(x, y)| px(x, y)[1] < 100)
            .count();
        assert!(
            (1..64).contains(&striped),
            "the hatching paints some but not all of a tile: {striped}"
        );
        assert_eq!(
            size(&doc, "b").1,
            22.0,
            "max(0, 8px - 6px) of padding + a 20px line"
        );
        assert_eq!(
            size(&doc, "c").0,
            3.0,
            "20px / round(6.67) = 2.86px, laid out as 3"
        );
        assert_eq!(size(&doc, "d").0, 17.0);
    }

    /// `style.css:14-19, 74, 750, 973-1007`: scrollbar styling. `scrollbar-gutter: stable` and
    /// `scrollbar-width` parse but the headless layout reserves no scrollbar space in any
    /// configuration (overlay scrollbars), so Pierre's
    /// `--diffs-scrollbar-gutter` bottom padding is its only gutter. `::-webkit-scrollbar*`
    /// pseudo-elements never apply (no such pseudo in the selector parser: those rules are
    /// dropped), so the transparent-track styling is moot.
    #[test]
    fn scrollbar_gutter_stable_reserves_no_space() {
        let doc = light(
            "#a{width:50px;height:50px;overflow:scroll}\
             #b{width:50px;height:50px;overflow:scroll;scrollbar-gutter:stable;scrollbar-width:thin}\
             #in{width:100px;height:10px}",
            "<div id=a><div id=in></div></div><div id=b><div id=in2 style='width:100px;height:10px'></div></div>",
        );
        for id in ["a", "b"] {
            let node = doc.get_node(doc.get_element_by_id(id).unwrap()).unwrap();
            let bar = node.final_layout().scrollbar_size;
            assert_eq!((bar.width, bar.height), (0.0, 0.0), "#{id}");
        }
    }

    /// `style.css:1021, 1422`: `text-overflow: ellipsis` on nowrap clipped text (separators and
    /// file names). The property parses (computed `ellipsis`) but neither `blitz-dom` nor
    /// `blitz-paint` read it (grep `ellipsis`): the painted text is pixel-identical to the same
    /// box with `text-overflow: clip`. When the ellipsis is drawn the two renders differ.
    #[test]
    fn text_overflow_ellipsis_parses_but_paints_like_clip() {
        let render = |overflow| {
            let mut doc = light(
                &format!(
                    "#b{{width:50px;overflow:hidden;text-overflow:{overflow};white-space:nowrap;font:10px monospace}}"
                ),
                "<div id=b>aaaaaaaaaaaaaaaaaaaaaaaaa</div>",
            );
            let px = pixels(&mut doc);
            (0..HEIGHT)
                .flat_map(|y| (0..WIDTH).map(move |x| (x, y)))
                .map(|(x, y)| px(x, y))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            render("ellipsis"),
            render("clip"),
            "ellipsis is now painted: invert this pin"
        );
        let doc = light("#a{text-overflow:ellipsis}", "<div id=a>x</div>");
        let node = doc.get_node(doc.get_element_by_id("a").unwrap()).unwrap();
        assert_eq!(
            format!(
                "{:?}",
                node.primary_styles()
                    .unwrap()
                    .get_text()
                    .text_overflow
                    .second
            ),
            "Ellipsis"
        );
    }

    /// `style.css:845-860`: Pierre's own non-subgrid layout for scroll mode, the workaround the
    /// port can apply unconditionally (without the `:has()` guard): `[data-code]` is `display:
    /// flex`, `[data-gutter]` a fixed `display: block; flex-shrink: 0` column and `[data-content]`
    /// `flex: 1 0 max-content`. A 100px scroll container holds a 30px gutter and 150px of
    /// non-wrapping content: the gutter keeps 30px and the content takes its max-content width,
    /// overflowing sideways. This is the layout that works on Blitz today.
    #[test]
    fn flex_gutter_and_max_content_column_layout_works_without_subgrid() {
        let doc = light(
            "#code{display:flex;width:100px;overflow:scroll hidden}\
             #gutter{display:block;flex-shrink:0;width:30px}\
             #content{display:block;flex:1 0 max-content;min-width:0}\
             #line{white-space:pre;font:10px monospace;line-height:10px}",
            "<div id=code><div id=gutter></div>\
             <div id=content><div id=line>aaaaaaaaaaaaaaaaaaaaaaaaa</div></div></div>",
        );
        assert_eq!(size(&doc, "gutter").0, 30.0);
        assert_eq!(rect(&doc, "content").0, 30.0);
        let content = size(&doc, "content").0;
        assert!(
            (150.0..=152.0).contains(&content),
            "25 glyphs of ~6px, not the 70px left over: {content}"
        );
    }

    /// `style.css` colour handling: `oklch()` colours parse and paint (Pierre only mentions it
    /// in comments, but themes may carry it).
    #[test]
    fn oklch_colors_paint() {
        let mut doc = light(
            "#a{width:20px;height:20px;background:oklch(0.7 0.2 30)}",
            "<div id=a></div>",
        );
        assert_eq!(pixels(&mut doc)(5, 5), [255, 97, 77, 255]);
    }
}
