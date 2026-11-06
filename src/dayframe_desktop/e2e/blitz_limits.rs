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
