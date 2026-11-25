mod diff_fixtures;
mod diffs;
mod material_colors;

use clay_dioxus_specimens::FoundationSection;
use material_colors::ColorFoundation;

#[cfg(not(any(feature = "desktop", target_arch = "wasm32")))]
compile_error!(
    "claytxt_showcase builds natively with the `desktop` feature and for the browser (wasm32) with the `web` feature"
);

use clay_dioxus_kit::Button;
use clay_dioxus_kit::ButtonVariant;
use clay_dioxus_kit::ScrubStrip;
use clay_dioxus_kit::TabItem;
use clay_dioxus_kit::TabPanel;
use clay_dioxus_kit::Tabs;
use clay_dioxus_kit::ThemeMode;
use clay_dioxus_kit::ThemeRoot;
#[cfg(feature = "desktop")]
use dioxus::desktop::tao::event::Event as DesktopEvent;
#[cfg(feature = "desktop")]
use dioxus::desktop::tao::event::WindowEvent as DesktopWindowEvent;
#[cfg(feature = "desktop")]
use dioxus::desktop::use_wry_event_handler;
#[cfg(feature = "desktop")]
use dioxus::desktop::DesktopContext;
use dioxus::prelude::*;
#[cfg(feature = "desktop")]
use dioxus_native::winit::event::WindowEvent as NativeWindowEvent;
#[cfg(feature = "desktop")]
use dioxus_native::winit::window::Window;
use pierre_diffs_dioxus::use_scroll_root;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::rc::Rc;
#[cfg(feature = "desktop")]
use std::sync::Arc;

const MATERIAL_THEME_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../clay_dioxus_kit/styles/material-theme.json"
));

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct MaterialTheme {
    schemes: BTreeMap<String, BTreeMap<String, String>>,
    palettes: BTreeMap<String, BTreeMap<String, String>>,
}

/// The renderer the showcase runs under, which decides whether the kit may move focus and set
/// `tabindex` (`KIT-052`): Blitz when `DAYTXT_RENDERER=blitz`, otherwise a browser engine (Wry
/// or the web build). The specimen pages read it from context (`App` provides it).
fn renderer() -> clay_dioxus_kit::Renderer {
    #[cfg(not(target_arch = "wasm32"))]
    if std::env::var("DAYTXT_RENDERER").as_deref() == Ok("blitz") {
        return clay_dioxus_kit::Renderer::Blitz;
    }
    clay_dioxus_kit::Renderer::Browser
}

/// Wry unless `DAYTXT_RENDERER=blitz`. `dioxus::launch` would pick Blitz whenever any crate in
/// the build turns on `dioxus/native` (feature unification), so each renderer is named.
#[cfg(feature = "desktop")]
fn main() {
    match std::env::var("DAYTXT_RENDERER").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("wry") => dioxus::LaunchBuilder::desktop()
            .with_cfg(
                dioxus::desktop::Config::new()
                    .with_window(dioxus::desktop::WindowBuilder::new().with_title("Theme lab"))
                    .with_menu(None),
            )
            .launch(App),
        Ok("blitz") => dioxus_native::launch(App),
        other => panic!("DAYTXT_RENDERER must be `wry` or `blitz`, got {other:?}"),
    }
}

/// The browser's launch: the page's own DOM is the renderer, so there is nothing to choose.
#[cfg(target_arch = "wasm32")]
fn main() {
    console_error_panic_hook::set_once();
    dioxus::launch(App);
}

/// One of the lab's start-up choices: the environment variable `env` on desktop, so a screenshot
/// run needs no click, and the `query` parameter of the page's URL in the browser (for example
/// `?tab=diffs&diffs_view=pr84`), where a process has no environment.
fn lab_setting(env: &str, query: &str) -> Option<String> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _: &str = query;
        std::env::var(env).ok()
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _: &str = env;
        let search = web_sys::window()
            .expect("the lab runs in a window")
            .location()
            .search()
            .expect("a window's location has a search string");
        web_sys::UrlSearchParams::new_with_str(&search)
            .expect("a location's search string is a valid query")
            .get(query)
    }
}

/// The width the lab's sections lay out in, kept current as the window resizes.
#[cfg(feature = "desktop")]
fn use_available_width() -> Signal<f64> {
    let native_window: Option<Arc<dyn Window>> = use_hook(try_consume_context);
    let desktop_window: Option<DesktopContext> = use_hook(try_consume_context);
    let initial_width = native_window
        .as_ref()
        .map(showcase_content_width)
        .or_else(|| desktop_window.as_ref().map(desktop_content_width))
        .expect("showcase must run with a native or desktop window context");
    let mut available_width = use_signal(|| initial_width);
    if let Some(window) = native_window {
        dioxus_native::use_window_event(move |event, _| match event {
            NativeWindowEvent::SurfaceResized(_) | NativeWindowEvent::ScaleFactorChanged { .. } => {
                available_width.set(showcase_content_width(&window));
            }
            _ => {}
        });
    }
    if let Some(window) = desktop_window {
        use_wry_event_handler(move |event, _| {
            if matches!(
                event,
                DesktopEvent::WindowEvent {
                    event: DesktopWindowEvent::Resized(_)
                        | DesktopWindowEvent::ScaleFactorChanged { .. },
                    ..
                }
            ) {
                available_width.set(desktop_content_width(&window));
            }
        });
    }
    available_width
}

/// The width the lab's sections lay out in, kept current as the window resizes.
#[cfg(target_arch = "wasm32")]
fn use_available_width() -> Signal<f64> {
    use wasm_bindgen::closure::Closure;
    use wasm_bindgen::JsCast;

    fn window_content_width() -> f64 {
        content_width(
            web_sys::window()
                .expect("the lab runs in a window")
                .inner_width()
                .expect("a window has an inner width")
                .as_f64()
                .expect("a window's inner width is a number"),
        )
    }
    let mut available_width = use_signal(window_content_width);
    // Registered once and removed when the root unmounts: the closure lives exactly as long as
    // its listener.
    let listener = use_hook(|| {
        let listener = Closure::<dyn FnMut()>::new(move || {
            available_width.set(window_content_width());
        });
        web_sys::window()
            .expect("the lab runs in a window")
            .add_event_listener_with_callback("resize", listener.as_ref().unchecked_ref())
            .expect("a window takes a resize listener");
        Rc::new(listener)
    });
    use_drop(move || {
        web_sys::window()
            .expect("the lab runs in a window")
            .remove_event_listener_with_callback(
                "resize",
                listener.as_ref().as_ref().unchecked_ref(),
            )
            .expect("a window drops a resize listener");
    });
    available_width
}

#[component]
fn App() -> Element {
    let theme: MaterialTheme = serde_json::from_str(MATERIAL_THEME_JSON)
        .expect("material-theme.json must match the Material theme schema");
    let mut mode = use_signal(|| ThemeMode::Dark);
    // `DAYTXT_LAB_TAB=scrub-strip` opens that tab, so a screenshot run needs no click.
    let mut tab = use_signal(|| {
        lab_setting("DAYTXT_LAB_TAB", "tab")
            .map(|value| {
                LabTab::from_value(&value)
                    .unwrap_or_else(|| panic!("DAYTXT_LAB_TAB: no tab named {value:?}"))
            })
            .unwrap_or(LabTab::Button)
    });
    let available_width = use_available_width();
    // The page's scroller is the document: it is the one scroller that gets momentum scrolling in
    // every engine (WebKitGTK gives none to a nested `overflow: auto` element). `main` carries
    // `data-dioxus-scroll-proxy`, which makes it stand in for the document's scroller in the
    // Dioxus fork's interpreter (the document's scroll and the viewport's size arrive as `main`'s
    // `onscroll` and `onresize`, and its mounted scroll commands drive the document). A viewer
    // that lays out in the page (the PR diff's) is driven by it: the handle takes `main`'s events
    // and the sections reach it by context.
    let scroll_root = use_scroll_root();
    use_context_provider(|| scroll_root.clone());
    use_context_provider(|| LabViewport(available_width));
    let diffs_view = use_signal(diffs::DiffsView::initial);
    use_context_provider(|| diffs_view);
    let photo_preloads = use_memo(|| {
        (0..36)
            .map(|index| {
                let period = if index < 20 { "recent" } else { "earlier" };
                format!("https://picsum.photos/seed/townframe-{period}-{index}/320/320")
            })
            .collect::<Vec<_>>()
    });
    use_context_provider(|| Signal::new(BTreeMap::<String, Rc<MountedData>>::new()));
    use_context_provider(renderer);
    let items = LabTab::ALL.map(|tab| TabItem::new(tab.value(), tab.label()));

    rsx! {
        for src in photo_preloads.read().iter() {
            document::Link {
                rel: "preload",
                r#as: "image",
                href: src.clone(),
            }
        }
        // `overflow-anchor` off on the document's scroller (the root element's value governs the
        // viewport) so the engine does not move the position the diff's scroll fix corrects.
        document::Style { "html {{ overflow-anchor: none; }}" }
        ThemeRoot {
            family: "material".to_string(),
            mode: mode(),
            main {
                class: "box-border bg-background p-6 font-sans text-foreground lg:p-12",
                "data-dioxus-scroll-proxy": "document",
                onmounted: {
                    let scroll_root = scroll_root.clone();
                    move |event| scroll_root.onmounted(event)
                },
                onscroll: {
                    let scroll_root = scroll_root.clone();
                    move |event| scroll_root.onscroll(event)
                },
                onresize: {
                    let scroll_root = scroll_root.clone();
                    move |event| scroll_root.onresize(event)
                },
                h1 {
                    class: "mb-2 text-4xl font-bold leading-tight tracking-tight",
                    "claytxt kit showcase"
                }
                p {
                    class: "mb-6 max-w-3xl text-foreground-muted leading-relaxed",
                    "One page per kit component, each showing every variant and state, then the foundations and the diff view. Every cell is vocabulary a theme can restyle: see docs/design-language/styling-contract.md, and apply a theme from the theme lab micro."
                }
                div { class: "mb-6",
                    Button {
                        variant: ButtonVariant::Outline,
                        onclick: move |()| {
                            mode.set(match mode() {
                                ThemeMode::Light => ThemeMode::Dark,
                                ThemeMode::Dark => ThemeMode::Light,
                            });
                        },
                        "Toggle {mode:?} mode"
                    }
                }
                Tabs {
                    items: items.to_vec(),
                    value: tab().value().to_string(),
                    on_value_change: move |value: String| {
                        tab.set(LabTab::from_value(&value).expect("the tab list is LabTab::ALL"));
                    },
                    label: "Showcase pages".to_string(),
                    renderer: renderer(),
                    for each in LabTab::ALL {
                        TabPanel { key: "{each.value()}", value: each.value().to_string(),
                            if tab() == each {
                                // The PR diff lists its own files in its index.
                                if !(each == LabTab::Diffs && diffs_view() == diffs::DiffsView::Pr84) {
                                    ThemeLabNav { viewport: (available_width(), 860.0), tab: each }
                                }
                                {each.page(theme.clone(), available_width())}
                            }
                        }
                    }
                }
            }
        }
    }
}

/// The window size the showcase's overlays (the index) lay out against, from the root.
#[derive(Clone, Copy)]
struct LabViewport(Signal<f64>);

/// The showcase's pages, one tab each: a specimen page per kit component, then the foundations
/// and the diff view.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum LabTab {
    Button,
    Tabs,
    Card,
    Separator,
    Alert,
    Chip,
    ScrubStrip,
    NavButton,
    AppBar,
    ListRow,
    PanelPage,
    Sheet,
    Drawer,
    Sections,
    ListDetails,
    Tokens,
    Colors,
    Diffs,
}

impl LabTab {
    const ALL: [LabTab; 18] = [
        LabTab::Button,
        LabTab::Tabs,
        LabTab::Card,
        LabTab::Separator,
        LabTab::Alert,
        LabTab::Chip,
        LabTab::ScrubStrip,
        LabTab::NavButton,
        LabTab::AppBar,
        LabTab::ListRow,
        LabTab::PanelPage,
        LabTab::Sheet,
        LabTab::Drawer,
        LabTab::Sections,
        LabTab::ListDetails,
        LabTab::Tokens,
        LabTab::Colors,
        LabTab::Diffs,
    ];

    /// The tab's `value`, also what `DAYTXT_LAB_TAB` and the page's `?tab=` name.
    fn value(self) -> &'static str {
        match self {
            LabTab::Button => "button",
            LabTab::Tabs => "tabs",
            LabTab::Card => "card",
            LabTab::Separator => "separator",
            LabTab::Alert => "alert",
            LabTab::Chip => "chip",
            LabTab::ScrubStrip => "scrub-strip",
            LabTab::NavButton => "nav-button",
            LabTab::AppBar => "app-bar",
            LabTab::ListRow => "list-row",
            LabTab::PanelPage => "panel-page",
            LabTab::Sheet => "sheet",
            LabTab::Drawer => "drawer",
            LabTab::Sections => "sections",
            LabTab::ListDetails => "list-details",
            LabTab::Tokens => "tokens",
            LabTab::Colors => "colors",
            LabTab::Diffs => "diffs",
        }
    }

    fn from_value(value: &str) -> Option<LabTab> {
        LabTab::ALL.into_iter().find(|tab| tab.value() == value)
    }

    fn label(self) -> &'static str {
        match self {
            LabTab::Button => "Button",
            LabTab::Tabs => "Tabs",
            LabTab::Card => "Card",
            LabTab::Separator => "Separator",
            LabTab::Alert => "Alert",
            LabTab::Chip => "Chip",
            LabTab::ScrubStrip => "ScrubStrip",
            LabTab::NavButton => "NavButton",
            LabTab::AppBar => "AppBar",
            LabTab::ListRow => "ListRow",
            LabTab::PanelPage => "PanelPage",
            LabTab::Sheet => "Sheet",
            LabTab::Drawer => "Drawer",
            LabTab::Sections => "Sections",
            LabTab::ListDetails => "ListDetails",
            LabTab::Tokens => "Tokens",
            LabTab::Colors => "Colors",
            LabTab::Diffs => "Diffs",
        }
    }

    /// The sections of the tab's page, by the `FoundationSection` id they mount under. A page
    /// with more than one gets the index strip (`ThemeLabNav`); the `ScrubStrip` page mounts its
    /// own, over the letters.
    fn sections(self) -> &'static [(&'static str, &'static str)] {
        match self {
            LabTab::Button => &[
                ("button-variants", "Variants"),
                ("button-states", "States"),
                ("button-label-styles", "Label styles"),
                ("button-toggle", "Toggle"),
            ],
            LabTab::Tabs => &[
                ("tabs-horizontal", "Horizontal"),
                ("tabs-vertical", "Vertical"),
            ],
            LabTab::Card => &[
                ("card-variants", "Variants"),
                ("card-statuses", "Statuses"),
                ("card-parts", "Parts"),
            ],
            LabTab::Separator => &[
                ("separator-orientations", "Orientations"),
                ("separator-roles", "Roles"),
            ],
            LabTab::Alert => &[
                ("alert-statuses", "Statuses"),
                ("alert-dismissible", "Dismissible"),
            ],
            LabTab::Chip => &[
                ("chip-statuses", "Statuses"),
                ("chip-interactive", "Interactive"),
            ],
            LabTab::ScrubStrip => &[],
            LabTab::NavButton => &[("nav-button-modes", "Modes")],
            LabTab::AppBar => &[("app-bar-slots", "Slots")],
            LabTab::ListRow => &[("list-row-states", "States")],
            LabTab::PanelPage => &[("panel-page-scroll", "Scroll")],
            LabTab::Sheet => &[("sheet-widths", "Widths")],
            LabTab::Drawer => &[("drawer-widths", "Widths")],
            LabTab::Sections => &[("sections-widths", "Resizable panes")],
            LabTab::ListDetails => &[("list-details-widths", "Width classes")],
            LabTab::Tokens => &[
                ("typography", "Typography"),
                ("spacing", "Spacing"),
                ("shape", "Shape & borders"),
                ("surfaces", "Surfaces & elevation"),
                ("states", "Interaction states"),
            ],
            LabTab::Colors => &[("colors", "Existing colors")],
            LabTab::Diffs => &[("diffs", "Diff view")],
        }
    }

    fn page(self, theme: MaterialTheme, available_width: f64) -> Element {
        match self {
            LabTab::Button => rsx! { clay_dioxus_specimens::ButtonSpecimens {} },
            LabTab::Tabs => rsx! { clay_dioxus_specimens::TabsSpecimens {} },
            LabTab::Card => rsx! { clay_dioxus_specimens::CardSpecimens {} },
            LabTab::Separator => rsx! { clay_dioxus_specimens::SeparatorSpecimens {} },
            LabTab::Alert => rsx! { clay_dioxus_specimens::AlertSpecimens {} },
            LabTab::Chip => rsx! { clay_dioxus_specimens::ChipSpecimens {} },
            LabTab::ScrubStrip => {
                rsx! { clay_dioxus_specimens::ScrubStripSpecimens { viewport: (available_width, 860.0) } }
            }
            LabTab::NavButton => rsx! { clay_dioxus_specimens::NavButtonSpecimens {} },
            LabTab::AppBar => rsx! { clay_dioxus_specimens::AppBarSpecimens {} },
            LabTab::ListRow => rsx! { clay_dioxus_specimens::ListRowSpecimens {} },
            LabTab::PanelPage => rsx! { clay_dioxus_specimens::PanelPageSpecimens {} },
            LabTab::Sheet => rsx! { clay_dioxus_specimens::SheetSpecimens {} },
            LabTab::Drawer => rsx! { clay_dioxus_specimens::DrawerSpecimens {} },
            LabTab::Sections => rsx! { clay_dioxus_specimens::SectionsSpecimens {} },
            LabTab::ListDetails => rsx! { clay_dioxus_specimens::ListDetailsSpecimens {} },
            LabTab::Tokens => rsx! { clay_dioxus_specimens::TokenFoundations {} },
            LabTab::Colors => rsx! {
                FoundationSection {
                    id: "colors".to_string(),
                    title: "Existing colors".to_string(),
                    description: "Material remains the baseline palette. Every source palette and generated scheme is rendered from the unchanged JSON fixture.".to_string(),
                    ColorFoundation { theme }
                }
            },
            LabTab::Diffs => rsx! {
                FoundationSection {
                    id: "diffs".to_string(),
                    title: "Diff view".to_string(),
                    description: "pierre_diffs_dioxus::FileDiffView, a plain Dioxus component over the Pierre port, in two views under one options bar: sample specimens (a split Rust diff, a unified TypeScript diff with a line annotation, a virtualized three-thousand-line diff) and a real 599-file PR diff. Syntax colours need the highlights lexer, which is not wired yet, so tokens render as plain text.".to_string(),
                    diffs::DiffsFoundation {}
                }
            },
        }
    }
}

#[cfg(feature = "desktop")]
fn logical_size(window: &Arc<dyn Window>) -> (f64, f64) {
    let scale = window.scale_factor();
    let size = window.surface_size();
    (size.width as f64 / scale, size.height as f64 / scale)
}

#[cfg(feature = "desktop")]
fn showcase_content_width(window: &Arc<dyn Window>) -> f64 {
    content_width(logical_size(window).0)
}

#[cfg(feature = "desktop")]
fn desktop_content_width(window: &DesktopContext) -> f64 {
    let width = window
        .inner_size()
        .to_logical::<f64>(window.scale_factor())
        .width;
    content_width(width)
}

fn content_width(width: f64) -> f64 {
    width - if width >= 1024.0 { 96.0 } else { 48.0 }
}

/// The index strip of a page with more than one section: it jumps between the sections the page
/// lists, by the `FoundationSection` id they mount under. The `ScrubStrip` page mounts its own.
#[component]
fn ThemeLabNav(viewport: (f64, f64), tab: LabTab) -> Element {
    let anchors = use_context::<Signal<BTreeMap<String, Rc<MountedData>>>>();
    let sections = tab.sections();
    if sections.len() < 2 {
        return VNode::empty();
    }
    rsx! {
        // Keyed by tab: each tab is its own index, so a tab's gesture state and selection never
        // carry over to a list of another length.
        ScrubStrip {
            key: "{tab.value()}",
            viewport,
            items: sections.iter().map(|(_, label)| *label).collect::<Vec<_>>(),
            renderer: renderer(),
            label: "Sections of this page".to_string(),
            on_select: move |index: usize| clay_dioxus_specimens::jump_to(anchors, sections[index].0.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_tab_has_a_unique_value_that_names_it() {
        for tab in LabTab::ALL {
            assert_eq!(LabTab::from_value(tab.value()), Some(tab));
        }
        let mut values: Vec<_> = LabTab::ALL.iter().map(|tab| tab.value()).collect();
        values.sort_unstable();
        values.dedup();
        assert_eq!(values.len(), LabTab::ALL.len());
    }

    #[test]
    fn every_page_but_the_scrub_strip_lists_a_section() {
        for tab in LabTab::ALL {
            assert_eq!(
                tab.sections().is_empty(),
                tab == LabTab::ScrubStrip,
                "{tab:?}"
            );
        }
    }
}
