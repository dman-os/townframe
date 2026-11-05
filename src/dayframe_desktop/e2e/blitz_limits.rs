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
