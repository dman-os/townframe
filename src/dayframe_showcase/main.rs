#[cfg(feature = "desktop")]
use dioxus::desktop::tao::event::Event as DesktopEvent;
#[cfg(feature = "desktop")]
use dioxus::desktop::tao::event::WindowEvent as DesktopWindowEvent;
#[cfg(feature = "desktop")]
use dioxus::desktop::use_wry_event_handler;
#[cfg(feature = "desktop")]
use dioxus::desktop::DesktopContext;
use dioxus::prelude::*;
use dioxus_native::winit::event::WindowEvent as NativeWindowEvent;
use dioxus_native::winit::window::Window;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use ui::ArcIndex;
use ui::Button;
use ui::ButtonVariant;
use ui::Card;
use ui::CardContent;
use ui::CardHeader;
use ui::ColumnHeight;
use ui::ColumnId;
use ui::ColumnRack;
use ui::ColumnRelation;
use ui::ColumnSection;
use ui::ColumnSource;
use ui::ColumnSpec;
use ui::ColumnState;
use ui::SectionLayout;
use ui::ThemeMode;
use ui::ThemeRoot;
use ui::TileChildren;
use ui::TileContent;
use ui::TileKind;
use ui::TileListSpec;
use ui::TileSpec;

const MATERIAL_THEME_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../ui/styles/material-theme.json"
));

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct MaterialTheme {
    schemes: BTreeMap<String, BTreeMap<String, String>>,
    palettes: BTreeMap<String, BTreeMap<String, String>>,
}

fn main() {
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    let theme: MaterialTheme = serde_json::from_str(MATERIAL_THEME_JSON)
        .expect("material-theme.json must match the Material theme schema");
    let mut mode = use_signal(|| ThemeMode::Dark);
    let native_window: Option<Arc<dyn Window>> = use_hook(try_consume_context);
    #[cfg(feature = "desktop")]
    let desktop_window: Option<DesktopContext> = use_hook(try_consume_context);
    #[cfg(feature = "desktop")]
    let initial_width = native_window
        .as_ref()
        .map(showcase_content_width)
        .or_else(|| desktop_window.as_ref().map(desktop_content_width))
        .expect("showcase must run with a native or desktop window context");
    #[cfg(not(feature = "desktop"))]
    let initial_width = native_window
        .as_ref()
        .map(showcase_content_width)
        .expect("showcase must run with a native window context");
    let mut available_width = use_signal(|| initial_width);
    let photo_preloads = use_memo(|| {
        (0..36)
            .map(|index| {
                let period = if index < 20 { "recent" } else { "earlier" };
                format!("https://picsum.photos/seed/townframe-{period}-{index}/320/320")
            })
            .collect::<Vec<_>>()
    });
    if let Some(window) = native_window {
        dioxus_native::use_window_event(move |event, _| match event {
            NativeWindowEvent::SurfaceResized(_) | NativeWindowEvent::ScaleFactorChanged { .. } => {
                available_width.set(showcase_content_width(&window));
            }
            _ => {}
        });
    }
    #[cfg(feature = "desktop")]
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
    use_context_provider(|| Signal::new(BTreeMap::<String, Rc<MountedData>>::new()));

    rsx! {
        for src in photo_preloads.read().iter() {
            document::Link {
                rel: "preload",
                r#as: "image",
                href: src.clone(),
            }
        }
        ThemeRoot {
            family: "material".to_string(),
            mode: mode(),
            main {
                class: "min-h-screen box-border bg-background p-6 font-sans text-foreground lg:p-12",
                h1 {
                    class: "mb-2 text-4xl font-bold leading-tight tracking-tight",
                    "Dayframe theme lab"
                }
                p {
                    class: "mb-6 max-w-3xl text-foreground-muted leading-relaxed",
                    "A legible reference for the foundations shared by Dayframe. Change the mode, inspect each token family, and use these specimens as the contract for future themes and components."
                }
                button {
                    class: "mb-6 cursor-pointer rounded-md border border-border bg-card px-4 py-2 font-sans text-foreground",
                    onclick: move |_| {
                        mode.set(match mode() {
                            ThemeMode::Light => ThemeMode::Dark,
                            ThemeMode::Dark => ThemeMode::Light,
                        });
                    },
                    "Toggle {mode:?} mode"
                }
                ThemeLabNav { viewport: (available_width(), 860.0) }
                FoundationSection {
                    id: "tile-grammar".to_string(),
                    title: "Feature grammar: Tiles".to_string(),
                    description: "This feature grammar is executable evidence, not part of the foundation token language. Ordered lists combine App, Widget, and Group tiles; content variants and sublist presentation remain feature-specific.".to_string(),
                    TileGrammarFoundation { available_width: available_width() }
                }
                FoundationSection {
                    id: "typography".to_string(),
                    title: "Typography".to_string(),
                    description: "Type tokens establish hierarchy, rhythm, and readable long-form content before components add decoration.".to_string(),
                    TypographyFoundation {}
                }
                FoundationSection {
                    id: "spacing".to_string(),
                    title: "Spacing grid".to_string(),
                    description: "Spacing uses Tailwind's native utilities, driven by the single Dayframe spacing unit.",
                    SpacingFoundation {}
                }
                FoundationSection {
                    id: "shape".to_string(),
                    title: "Shape & borders".to_string(),
                    description: "Radius and border tokens provide the shared geometry for controls, cards, surfaces, and focused states.".to_string(),
                    ShapeFoundation {}
                }
                FoundationSection {
                    id: "surfaces".to_string(),
                    title: "Surfaces & elevation".to_string(),
                    description: "Surface roles and shadows describe how content separates from the canvas without relying on arbitrary colors.".to_string(),
                    SurfaceFoundation {}
                }
                FoundationSection {
                    id: "states".to_string(),
                    title: "Interaction states".to_string(),
                    description: "States are shown as deliberate specimens so themes can be evaluated for focus, selection, feedback, and disabled legibility.".to_string(),
                    StateFoundation {}
                }
                FoundationSection {
                    id: "colors".to_string(),
                    title: "Existing colors".to_string(),
                    description: "Material remains the baseline palette. Every source palette and generated scheme is rendered from the unchanged JSON fixture.".to_string(),
                    ColorFoundation { theme }
                }
            }
        }
    }
}

fn logical_size(window: &Arc<dyn Window>) -> (f64, f64) {
    let scale = window.scale_factor();
    let size = window.surface_size();
    (size.width as f64 / scale, size.height as f64 / scale)
}

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

#[component]
fn ThemeLabNav(viewport: (f64, f64)) -> Element {
    let anchors = use_context::<Signal<BTreeMap<String, Rc<MountedData>>>>();
    let ids = [
        "tile-grammar",
        "typography",
        "spacing",
        "shape",
        "surfaces",
        "states",
        "colors",
    ];
    let labels = vec![
        "Tile grammar",
        "Typography",
        "Spacing",
        "Shape & borders",
        "Surfaces & elevation",
        "Interaction states",
        "Existing colors",
    ];
    rsx! {
        ArcIndex {
            viewport,
            items: labels,
            on_select: move |index| {
                let target = ids.get(index).and_then(|id| anchors.read().get(*id).cloned());
                spawn(async move {
                    if let Some(target) = target {
                        if let Err(error) = target.scroll_to(ScrollBehavior::Instant).await {
                            eprintln!("unable to scroll to showcase section: {error:?}");
                        }
                    }
                });
            },
        }
    }
}

#[component]
fn FoundationSection(id: String, title: String, description: String, children: Element) -> Element {
    let mut anchors = use_context::<Signal<BTreeMap<String, Rc<MountedData>>>>();
    let mount_id = id.clone();
    rsx! {
        section {
            id: "{id}",
            onmounted: move |event| {
                anchors.write().insert(mount_id.clone(), event.data());
            },
            class: "scroll-mt-18 py-8",
            h2 {
                class: "mb-2 text-2xl font-bold leading-tight tracking-tight",
                "{title}"
            }
            p {
                class: "mb-6 max-w-3xl text-foreground-muted leading-normal",
                "{description}"
            }
            {children}
        }
    }
}

#[component]
fn TypographyFoundation() -> Element {
    let specimens = [
        ("Display", "text-4xl font-bold leading-tight tracking-tight"),
        ("Title", "text-2xl font-bold leading-tight tracking-tight"),
        ("Large body", "text-lg font-normal leading-snug"),
        ("Body", "text-base font-normal leading-normal"),
        (
            "Small label",
            "text-sm font-medium leading-snug tracking-wide",
        ),
    ];

    rsx! {
        div {
            class: "grid grid-cols-[repeat(auto-fit,minmax(15rem,1fr))] gap-3",
                    for (name, utility) in specimens {
                Card {
                    CardHeader { strong { "{name}" } }
                    CardContent {
                        p {
                            class: "{utility}",
                            "Aa — Dayframe foundations"
                        }
                        code {
                            class: "mt-3 block text-sm text-foreground-muted",
                            "tw: {utility}"
                        }
                    }
                }
            }
        }
        div {
            class: "mt-4",
            Card {
            CardHeader { strong { "Readable paragraph" } }
            CardContent {
                p {
                    class: "max-w-prose leading-relaxed",
                    "A theme is successful when the hierarchy remains clear before color is applied. This paragraph intentionally demonstrates a comfortable reading measure, relaxed line height, and muted supporting text."
                }
            }
        }
        }
    }
}

#[component]
fn SpacingFoundation() -> Element {
    let spaces = [
        ("space-1", "0.25rem", "p-1"),
        ("space-2", "0.5rem", "p-2"),
        ("space-3", "0.75rem", "p-3"),
        ("space-4", "1rem", "p-4"),
        ("space-5", "1.25rem", "p-5"),
        ("space-6", "1.5rem", "p-6"),
        ("space-8", "2rem", "p-8"),
        ("space-9", "2.25rem", "p-9"),
        ("space-12", "3rem", "p-12"),
    ];

    rsx! {
        Card {
            CardHeader { strong { "Tailwind spacing utilities" } }
            CardContent {
                div {
                class: "grid gap-3",
                    for (_, value, utility) in spaces {
                        div {
                            class: "grid grid-cols-[6rem_1fr_10rem] items-center gap-3",
                            code { "{utility}" }
                            div {
                                class: "h-2 max-w-full rounded-full bg-primary",
                                style: "width:{value};",
                            }
                            small { class: "text-foreground-muted", "{value} · {utility}" }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn ShapeFoundation() -> Element {
    let radii = [
        ("sm", "var(--df-radius-sm)", "rounded-sm"),
        ("md", "var(--df-radius-md)", "rounded-md"),
        ("lg", "var(--df-radius-lg)", "rounded-lg"),
        ("xl", "var(--df-radius-xl)", "rounded-xl"),
        ("pill", "var(--df-radius-pill)", "rounded-full"),
    ];
    let borders = [
        (
            "hairline",
            "var(--df-border-width-hairline)",
            "border-hairline",
        ),
        ("thin", "var(--df-border-width-thin)", "border-thin"),
        ("thick", "var(--df-border-width-thick)", "border-thick"),
    ];

    rsx! {
        div {
            class: "grid grid-cols-[repeat(auto-fit,minmax(14rem,1fr))] gap-3",
            Card {
                CardHeader { strong { "Radius" } }
                CardContent {
                    div { class: "flex flex-wrap gap-3",
                        for (name, _radius, utility) in radii {
                            div {
                                class: "grid size-24 min-h-18 place-items-center rounded border border-border bg-card p-3 text-center text-sm {utility}",
                                "{name} · {utility}"
                            }
                        }
                    }
                }
            }
            Card {
                CardHeader { strong { "Border widths" } }
                CardContent {
                    div { class: "grid gap-3",
                        for (name, width, utility) in borders {
                            div {
                                class: "rounded-sm p-3",
                                style: "border:{width} solid var(--df-color-primary);",
                                "{name} — {width} · {utility}"
                            }
                        }
                    }
                }
            }
        }
    }
}
fn browser_tab(id: &str, title: &str, subtitle: &str, children: Vec<TileSpec>) -> TileSpec {
    let has_children = !children.is_empty();
    let child_count = children.len();
    TileSpec {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind: if has_children {
            TileKind::Group
        } else {
            TileKind::App
        },
        body: Vec::new(),
        content: TileContent::Line,
        children: has_children.then(|| {
            TileChildren::Inline(TileListSpec {
                source: ColumnSource::Child {
                    parent: ColumnId(id.to_string()),
                },
                state: ColumnState::Ready,
                sections: vec![ColumnSection {
                    title: None,
                    layout: SectionLayout::List,
                    tiles: children,
                }],
                total: Some(child_count),
                has_next: false,
            })
        }),
    }
}

fn tile_grammar_columns() -> Vec<ColumnSpec> {
    vec![
        frame_column(),
        home_column(),
        widgets_column(),
        tab_history_column(),
        photos_column(),
    ]
}

fn frame_column() -> ColumnSpec {
    ColumnSpec {
        id: ColumnId("frame".to_string()),
        title: "Frame".to_string(),
        relation: ColumnRelation::Root,
        list: TileListSpec {
            source: ColumnSource::Static,
            state: ColumnState::Ready,
            sections: ('A'..='Z')
                .enumerate()
                .map(|(section_index, letter)| ColumnSection {
                    title: Some(letter.to_string()),
                    layout: SectionLayout::List,
                    tiles: (0..=(section_index % 6))
                        .map(|item_index| frame_tile(letter, item_index))
                        .collect(),
                })
                .collect(),
            total: Some(87),
            has_next: false,
        },
    }
}

fn frame_tile(letter: char, item_index: usize) -> TileSpec {
    let is_folder = letter == 'A' && item_index == 0;
    TileSpec {
        id: format!("frame-{letter}-{item_index}"),
        title: if is_folder {
            "A folder".to_string()
        } else {
            format!("{letter} item {}", item_index + 1)
        },
        subtitle: if is_folder {
            "Group · inline nested list".to_string()
        } else if item_index.is_multiple_of(3) {
            String::new()
        } else {
            format!("App · section {letter}")
        },
        kind: if item_index == 0 {
            TileKind::Group
        } else {
            TileKind::App
        },
        body: if is_folder {
            vec![
                "Opens an inline nested list.".to_string(),
                "The body can contain multiple paragraphs while the tile grows naturally."
                    .to_string(),
            ]
        } else if item_index.is_multiple_of(4) {
            vec![
                "A longer body example that makes the tile grow naturally.".to_string(),
                "This second paragraph demonstrates how additional context can live below the subtitle without changing the tile grammar.".to_string(),
            ]
        } else {
            Vec::new()
        },
        content: TileContent::Line,
        children: is_folder.then(|| TileChildren::Inline(nested_frame_list())),
    }
}

fn nested_frame_list() -> TileListSpec {
    TileListSpec {
        source: ColumnSource::Child {
            parent: ColumnId("frame-A-0".to_string()),
        },
        state: ColumnState::Ready,
        sections: vec![ColumnSection {
            title: None,
            layout: SectionLayout::List,
            tiles: vec![
                TileSpec {
                    id: "a-nested-one".to_string(),
                    title: "Nested one".to_string(),
                    subtitle: String::new(),
                    kind: TileKind::App,
                    body: Vec::new(),
                    content: TileContent::Line,
                    children: Some(TileChildren::Inline(TileListSpec {
                        source: ColumnSource::Child {
                            parent: ColumnId("a-nested-one".to_string()),
                        },
                        state: ColumnState::Ready,
                        sections: vec![ColumnSection {
                            title: None,
                            layout: SectionLayout::List,
                            tiles: vec![TileSpec {
                                id: "a-grandchild".to_string(),
                                title: "Grandchild".to_string(),
                                subtitle: String::new(),
                                kind: TileKind::App,
                                body: vec![
                                    "A second nesting level.".to_string(),
                                    "This grandchild gives the nested body a second paragraph too."
                                        .to_string(),
                                ],
                                content: TileContent::Line,
                                children: None,
                            }],
                        }],
                        total: Some(1),
                        has_next: false,
                    })),
                },
                TileSpec {
                    id: "a-nested-two".to_string(),
                    title: "Nested two".to_string(),
                    subtitle: "Inline child".to_string(),
                    kind: TileKind::App,
                    body: Vec::new(),
                    content: TileContent::Line,
                    children: None,
                },
            ],
        }],
        total: Some(2),
        has_next: false,
    }
}

fn home_column() -> ColumnSpec {
    ColumnSpec {
        id: ColumnId("home".to_string()),
        title: "Home".to_string(),
        relation: ColumnRelation::Root,
        list: TileListSpec {
            source: ColumnSource::Paged { cursor: None },
            state: ColumnState::Ready,
            sections: vec![ColumnSection {
                title: Some("Pinned".to_string()),
                layout: SectionLayout::List,
                tiles: vec![
                    TileSpec {
                        id: "manual-order".to_string(),
                        title: "Manual order".to_string(),
                        subtitle: "Group · Overlay sublist".to_string(),
                        kind: TileKind::Group,
                        body: Vec::new(),
                        content: TileContent::Line,
                        children: None,
                    },
                    TileSpec {
                        id: "capture".to_string(),
                        title: "Capture".to_string(),
                        subtitle: "Group · Column sublist".to_string(),
                        kind: TileKind::Group,
                        body: Vec::new(),
                        content: TileContent::Line,
                        children: None,
                    },
                ],
            }],
            total: None,
            has_next: true,
        },
    }
}

fn widgets_column() -> ColumnSpec {
    ColumnSpec {
        id: ColumnId("widgets".to_string()),
        title: "Widgets".to_string(),
        relation: ColumnRelation::Root,
        list: TileListSpec {
            source: ColumnSource::Subscription,
            state: ColumnState::Ready,
            sections: vec![ColumnSection {
                title: Some("Active".to_string()),
                layout: SectionLayout::List,
                tiles: vec![
                    TileSpec {
                        id: "now-playing".to_string(),
                        title: "Now playing".to_string(),
                        subtitle: "Widget · Media · stacked slot".to_string(),
                        body: Vec::new(),
                        kind: TileKind::Widget,
                        content: TileContent::Media,
                        children: None,
                    },
                    TileSpec {
                        id: "scratchpad".to_string(),
                        title: "Scratchpad".to_string(),
                        subtitle: "Widget · StickyNote".to_string(),
                        body: Vec::new(),
                        kind: TileKind::Widget,
                        content: TileContent::StickyNote,
                        children: None,
                    },
                ],
            }],
            total: Some(3),
            has_next: false,
        },
    }
}

fn tab_history_column() -> ColumnSpec {
    ColumnSpec {
        id: ColumnId("tab-history".to_string()),
        title: "Tab history".to_string(),
        relation: ColumnRelation::Root,
        list: TileListSpec {
            source: ColumnSource::Static,
            state: ColumnState::Ready,
            sections: vec![
                ColumnSection {
                    title: Some("Pinned".to_string()),
                    layout: SectionLayout::List,
                    tiles: vec![
                        browser_tab(
                            "pinned-townframe",
                            "Townframe",
                            "github.com/dman-os/townframe",
                            vec![
                                browser_tab(
                                    "pinned-townframe-readme",
                                    "README",
                                    "github.com/dman-os/townframe/README.md",
                                    vec![],
                                ),
                                browser_tab(
                                    "pinned-townframe-issues",
                                    "Issues",
                                    "github.com/dman-os/townframe/issues",
                                    vec![],
                                ),
                            ],
                        ),
                        browser_tab(
                            "pinned-dioxus",
                            "Dioxus",
                            "dioxuslabs.com/learn",
                            vec![
                                browser_tab(
                                    "pinned-dioxus-native",
                                    "Native renderer",
                                    "dioxuslabs.com/learn/0.8/native",
                                    vec![],
                                ),
                                browser_tab(
                                    "pinned-dioxus-rsx",
                                    "RSX reference",
                                    "dioxuslabs.com/learn/0.8/reference/rsx",
                                    vec![],
                                ),
                            ],
                        ),
                    ],
                },
                ColumnSection {
                    title: Some("Today".to_string()),
                    layout: SectionLayout::List,
                    tiles: (1..=8)
                        .map(|index| {
                            browser_tab(
                                &format!("today-{index}"),
                                &format!("Project tab {index}"),
                                &format!("workspace.local/project/{index}"),
                                vec![
                                    browser_tab(
                                        &format!("today-{index}-docs"),
                                        "Documentation",
                                        "docs.example.com/reference",
                                        vec![browser_tab(
                                            &format!("today-{index}-deep"),
                                            "Deep history",
                                            "docs.example.com/reference/details",
                                            vec![],
                                        )],
                                    ),
                                    browser_tab(
                                        &format!("today-{index}-discussion"),
                                        "Discussion",
                                        "community.example.com/thread",
                                        vec![],
                                    ),
                                ],
                            )
                        })
                        .collect(),
                },
                ColumnSection {
                    title: Some("Yesterday".to_string()),
                    layout: SectionLayout::List,
                    tiles: (1..=6)
                        .map(|index| {
                            browser_tab(
                                &format!("yesterday-{index}"),
                                &format!("Research tab {index}"),
                                &format!("search.example.com/query/{index}"),
                                vec![
                                    browser_tab(
                                        &format!("yesterday-{index}-result-a"),
                                        "Result A",
                                        "search.example.com/result/a",
                                        vec![],
                                    ),
                                    browser_tab(
                                        &format!("yesterday-{index}-result-b"),
                                        "Result B",
                                        "search.example.com/result/b",
                                        vec![],
                                    ),
                                    browser_tab(
                                        &format!("yesterday-{index}-archive"),
                                        "Archived branch",
                                        "archive.example.com/topic",
                                        vec![browser_tab(
                                            &format!("yesterday-{index}-archive-deep"),
                                            "Archived detail",
                                            "archive.example.com/topic/detail",
                                            vec![],
                                        )],
                                    ),
                                ],
                            )
                        })
                        .collect(),
                },
            ],
            total: Some(16),
            has_next: false,
        },
    }
}

fn photos_column() -> ColumnSpec {
    ColumnSpec {
        id: ColumnId("photos".to_string()),
        title: "Photos".to_string(),
        relation: ColumnRelation::Root,
        list: TileListSpec {
            source: ColumnSource::Static,
            state: ColumnState::Ready,
            sections: vec![
                ColumnSection {
                    title: Some("Recent".to_string()),
                    layout: SectionLayout::DenseGallery { columns: 4 },
                    tiles: (0..20)
                        .map(|index| TileSpec {
                            id: format!("photo-recent-{index}"),
                            title: String::new(),
                            subtitle: String::new(),
                            kind: TileKind::App,
                            content: TileContent::Thumbnail {
                                src: format!(
                                    "https://picsum.photos/seed/townframe-recent-{index}/320/320"
                                ),
                                alt: format!("Recent photo {}", index + 1),
                            },
                            body: Vec::new(),
                            children: None,
                        })
                        .collect(),
                },
                ColumnSection {
                    title: Some("Earlier".to_string()),
                    layout: SectionLayout::DenseGallery { columns: 4 },
                    tiles: (20..36)
                        .map(|index| TileSpec {
                            id: format!("photo-earlier-{index}"),
                            title: String::new(),
                            subtitle: String::new(),
                            kind: TileKind::App,
                            content: TileContent::Thumbnail {
                                src: format!(
                                    "https://picsum.photos/seed/townframe-earlier-{index}/320/320"
                                ),
                                alt: format!("Earlier photo {}", index - 19),
                            },
                            body: Vec::new(),
                            children: None,
                        })
                        .collect(),
                },
            ],
            total: Some(36),
            has_next: false,
        },
    }
}

#[component]
fn TileGrammarFoundation(available_width: f64) -> Element {
    let columns = tile_grammar_columns();
    rsx! {
        div {
            class: "df-tile-lab",
            p {
                class: "df-tile-lab-note",
                "ColumnRack owns the responsive window; each Column declares its source and loading state while Tile and Widget keep the content grammar stable."
            }
            div {
                class: "mt-6",
                ColumnRack { columns, height: ColumnHeight::Natural, available_width }
            }
        }
    }
}
#[component]
fn SurfaceFoundation() -> Element {
    let surfaces = [
        (
            "canvas",
            "var(--df-color-canvas)",
            "var(--df-shadow-none)",
            "bg-background",
        ),
        (
            "surface",
            "var(--df-color-surface)",
            "var(--df-shadow-none)",
            "bg-card",
        ),
        (
            "sunken",
            "var(--df-color-surface-sunken)",
            "var(--df-shadow-none)",
            "bg-surface-sunken",
        ),
    ];
    let shadows = [
        ("none", "var(--df-shadow-none)", "shadow-none"),
        ("sm", "var(--df-shadow-sm)", "shadow-sm"),
        ("md", "var(--df-shadow-md)", "shadow-md"),
    ];

    rsx! {
        div {
            class: "grid grid-cols-[repeat(auto-fit,minmax(14rem,1fr))] gap-3",
            Card {
                CardHeader { strong { "Surface roles" } }
                CardContent {
                    div { class: "grid gap-3",
                        for (name, background, shadow, utility) in surfaces {
                            div {
                                class: "flex min-h-16 items-end rounded-md p-3",
                                style: "background:{background}; box-shadow:{shadow};",
                                "{name} · {utility}"
                            }
                        }
                    }
                }
            }
            Card {
                CardHeader { strong { "Elevation" } }
                CardContent {
                    div { class: "grid gap-4 rounded-md bg-surface-sunken p-3",
                        for (name, shadow, utility) in shadows {
                            div {
                                class: "rounded-md bg-card p-4",
                                style: "box-shadow:{shadow};",
                                "shadow-{name} · {utility}"
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn StateFoundation() -> Element {
    rsx! {
        div {
            class: "grid grid-cols-[repeat(auto-fit,minmax(14rem,1fr))] gap-3",
            Card {
                CardHeader { strong { "Button variants" } }
                CardContent {
                    div { class: "flex flex-wrap gap-2",
                        Button { variant: ButtonVariant::Default, "Default" }
                        Button { variant: ButtonVariant::Secondary, "Secondary" }
                        Button { variant: ButtonVariant::Outline, "Outline" }
                        Button { variant: ButtonVariant::Ghost, "Ghost" }
                        Button { variant: ButtonVariant::Destructive, "Destructive" }
                    }
                }
            }
            Card {
                CardHeader { strong { "State surfaces" } }
                CardContent {
                    div { class: "grid gap-2",
                        StateRow { name: "Default", detail: "Resting surface", decoration: "background:var(--df-color-surface);" }
                        StateRow { name: "Hover", detail: "Tonal response", decoration: "background:var(--df-color-surface-sunken);" }
                        StateRow { name: "Focus", detail: "Visible keyboard ring", decoration: "background:var(--df-color-surface); outline:var(--df-border-width-thick) solid var(--df-color-focus); outline-offset:2px;" }
                        StateRow { name: "Selected", detail: "Primary emphasis", decoration: "background:var(--df-color-primary); color:var(--df-color-on-primary);" }
                        StateRow { name: "Disabled", detail: "Reduced emphasis", decoration: "background:var(--df-color-surface); opacity:0.38;" }
                        StateRow { name: "Error", detail: "Actionable feedback", decoration: "border-color:var(--df-color-danger); background:var(--df-color-surface);" }
                    }
                }
            }
        }
    }
}

#[component]
fn StateRow(name: &'static str, detail: &'static str, decoration: &'static str) -> Element {
    rsx! {
        div {
            class: "flex items-center justify-between gap-3 rounded-md border border-border p-3",
            style: "{decoration}",
            strong { "{name}" }
            small { style: "opacity:0.8;", "{detail}" }
        }
    }
}

#[component]
fn ColorFoundation(theme: MaterialTheme) -> Element {
    rsx! {
        h3 {
            class: "mb-3 text-lg",
            "Palettes"
        }
        for (palette_name, colors) in theme.palettes {
            section {
                key: "palette-{palette_name}",
                class: "mb-8",
                h4 {
                    class: "mb-3 text-base font-medium",
                    "{palette_name}"
                }
                div {
                    class: "grid grid-cols-[repeat(auto-fill,minmax(11.25rem,1fr))] gap-2",
                    for (name, value) in colors {
                        ColorSwatch {
                            key: "{palette_name}-{name}",
                            name,
                            value,
                            rounded: false,
                        }
                    }
                }
            }
        }
        h3 {
            class: "mt-12 mb-6 text-lg",
            "Schemes"
        }
        for (scheme_name, colors) in theme.schemes {
            section {
                key: "scheme-{scheme_name}",
                class: "mb-8",
                h4 {
                    class: "mb-3 text-base font-medium",
                    "{scheme_name}"
                }
                div {
                class: "grid grid-cols-[repeat(auto-fill,minmax(11.25rem,1fr))] gap-3",
                    for (name, value) in colors {
                        ColorSwatch {
                            key: "{scheme_name}-{name}",
                            name,
                            value,
                            rounded: true,
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn ColorSwatch(name: String, value: String, rounded: bool) -> Element {
    let foreground = readable_text_color(&value);
    let radius_class = if rounded { "rounded-lg" } else { "rounded-sm" };

    rsx! {
        div {
            class: "min-h-18 box-border rounded-md p-3 {radius_class}",
            style: "background:{value}; color:{foreground};",
            div { class: "font-medium", "{name}" }
            div { class: "mt-1 text-sm opacity-80", "{value}" }
        }
    }
}

fn readable_text_color(value: &str) -> &'static str {
    let hex = value.strip_prefix('#').unwrap_or(value);
    if hex.len() != 6 {
        return "#111827";
    }

    let red = u8::from_str_radix(&hex[0..2], 16).expect("Material colors must be hex values");
    let green = u8::from_str_radix(&hex[2..4], 16).expect("Material colors must be hex values");
    let blue = u8::from_str_radix(&hex[4..6], 16).expect("Material colors must be hex values");
    let luminance =
        (0.299 * f64::from(red) + 0.587 * f64::from(green) + 0.114 * f64::from(blue)) / 255.0;

    if luminance > 0.58 {
        "#111827"
    } else {
        "#ffffff"
    }
}
