//! Tiles prototype (FDR 005).
//!
//! Experiment surface for the tile primitive: ordered lists, in-place
//! expand/collapse nesting, stacked carousels, multi-column layout.
//!
//! The data here is an in-memory, dynamic list — nothing persists. That is a
//! deliberate prototype answer to FDR 005 open question 2 (persisted vs
//! ephemeral lists): persistence, ordering policy, and mobile gestures are
//! open questions this prototype exists to probe, not solve.

use dioxus::prelude::*;
use std::time::{Duration, Instant};

/// One tile: a unit of content or action in an ordered list.
#[derive(Clone, Debug, PartialEq)]
pub struct Tile {
    /// Text shown on the row.
    pub title: String,
    /// Optional secondary text rendered inline with the title.
    pub subtitle: String,
    pub content: TileContent,
    pub kind: TileKind,
    /// Nested sublist, expanded/collapsed in place (not a popup).
    pub children: Vec<Tile>,
    /// Stacked tiles occupying one slot; cycled like a carousel.
    pub stack: Vec<Tile>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TileKind {
    /// Opens a surface (daybook, daychat, daybrowser, ...).
    App,
    /// Holds content in place (a widget).
    Widget,
    /// Groups other tiles; usually has children or a stack.
    Group,
}

/// Rendering-neutral content variants for tiles that are not ordinary line tiles.
#[derive(Clone, Debug, PartialEq)]
pub enum TileContent {
    Line,
    Media {
        progress_percent: u8,
    },
    StickyNote {
        text: String,
        accent: String,
    },
}

impl Tile {
    fn app(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            subtitle: String::new(),
            content: TileContent::Line,
            kind: TileKind::App,
            children: vec![],
            stack: vec![],
        }
    }

    fn widget(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            subtitle: String::new(),
            content: TileContent::Line,
            kind: TileKind::Widget,
            children: vec![],
            stack: vec![],
        }
    }

    fn with_subtitle(mut self, subtitle: impl Into<String>) -> Self {
        self.subtitle = subtitle.into();
        self
    }

    fn media(title: impl Into<String>, subtitle: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            subtitle: subtitle.into(),
            content: TileContent::Media {
                progress_percent: 42,
            },
            kind: TileKind::Widget,
            children: vec![],
            stack: vec![],
        }
    }

    fn sticky_note(title: impl Into<String>, text: impl Into<String>, accent: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            subtitle: String::new(),
            content: TileContent::StickyNote {
                text: text.into(),
                accent: accent.into(),
            },
            kind: TileKind::Widget,
            children: vec![],
            stack: vec![],
        }
    }

    fn group(title: impl Into<String>, children: Vec<Tile>) -> Self {
        Self {
            title: title.into(),
            subtitle: String::new(),
            content: TileContent::Line,
            kind: TileKind::Group,
            children,
            stack: vec![],
        }
    }

    /// Build a stack slot: the first tile's row is shown, the rest cycle in.
    fn stacked(tiles: Vec<Tile>) -> Self {
        let mut it = tiles.into_iter();
        let mut head = it.next().unwrap_or_else(|| Tile::widget(""));
        head.stack = it.collect();
        head
    }
}

/// One column of tile lists on desktop.
#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub tiles: Vec<Tile>,
}

/// The demo tile graph: three responsive columns containing line and custom tiles.
pub fn sample_columns() -> Vec<Column> {
    vec![
        Column {
            name: "Frame".into(),
            tiles: vec![
                Tile::group(
                    "Daybook",
                    vec![Tile::app("Drawer"), Tile::app("Timetable").with_subtitle("Daily schedule"), Tile::app("Plugs")],
                ),
                Tile::stacked(vec![
                    Tile::widget("Daychat: dman"),
                    Tile::widget("Daychat: work").with_subtitle("3 active threads"),
                    Tile::widget("Daychat: irl"),
                ]),
                Tile::group(
                    "Daybrowser",
                    vec![Tile::app("Index"), Tile::app("Recent docs")],
                ),
            ],
        },
        Column {
            name: "Home".into(),
            tiles: vec![
                Tile::stacked(vec![
                    Tile::widget("Progress: sync"),
                    Tile::widget("Progress: downloads").with_subtitle("3 items remaining"),
                    Tile::widget("For you"),
                ]),
                Tile::group("Manual order", vec![Tile::app("note alpha"), Tile::app("note zulu")]),
                Tile::group(
                    "Capture",
                    vec![Tile::app("New doc"), Tile::app("Camera"), Tile::app("Mic")],
                ),
            ],
        },
        Column {
            name: "Widgets".into(),
            tiles: vec![
                Tile::stacked(vec![
                    Tile::media("Now playing", "dltzk · Frailty"),
                    Tile::media("Up next", "another artist · another track"),
                    Tile::media("Recently played", "a third artist · a third track"),
                ]),
                Tile::sticky_note(
                    "Scratchpad",
                    "Remember to review the tile interaction grammar.",
                    "#fbbf24",
                ),
                Tile::widget("A deliberately long widget title that should truncate cleanly"),
            ],

        },
    ]
}

/// The prototype surface: a single immersive list of large line tiles.
#[component]
pub fn TilesDemo() -> Element {
    let columns = use_signal(|| sample_columns());
    let mut column_offset = use_signal(|| 0usize);
    let mut column_scroll_lock = use_signal(|| None::<Instant>);
    let column_count = columns.read().len();
    let current_column_offset = *column_offset.read();
    rsx! {
        div {
            role: "main",
            style: "min-height:100vh; box-sizing:border-box; padding:48px 28px 96px; background:#0d0e10; color:#f3f4f6; font-family:system-ui,sans-serif;",
            div {
                class: "column-navigation column-set-{current_column_offset}",
                "aria-label": "column navigation",
                button {
                    class: "column-arrow",
                    "aria-label": "previous columns",
                    onclick: move |_| {
                        let current = *column_offset.read();
                        *column_offset.write() = (current + column_count - 1) % column_count;
                    },
                    "←"
                }
                div {
                    class: "column-pager column-set-{current_column_offset}",
                    role: "group",
                    "aria-label": "visible column set",
                    onwheel: move |event| {
                        let delta = event.data.delta().strip_units();
                        let movement = if delta.x.abs() > delta.y.abs() { delta.x } else { delta.y };
                        if movement == 0.0 {
                            return;
                        }
                        if column_scroll_lock.read().is_some_and(|at| at.elapsed() < Duration::from_millis(300)) {
                            return;
                        }
                        event.stop_propagation();
                        *column_scroll_lock.write() = Some(Instant::now());
                        if movement > 0.0 {
                            let current = *column_offset.read();
                            *column_offset.write() = (current + 1) % column_count;
                        } else if movement < 0.0 {
                            let current = *column_offset.read();
                            *column_offset.write() = (current + column_count - 1) % column_count;
                        }
                    },
                    for index in 0..column_count {
                        button {
                            class: "column-dot",
                            "aria-label": "show column {index + 1}",
                            "aria-current": if index == current_column_offset { "true" } else { "false" },
                            onclick: move |e| {
                                e.stop_propagation();
                                *column_offset.write() = index;
                            },
                            span { "aria-hidden": "true" }
                        }
                    }
                }
                button {
                    class: "column-arrow",
                    "aria-label": "next columns",
                    onclick: move |_| {
                        let current = *column_offset.read();
                        *column_offset.write() = (current + 1) % column_count;
                    },
                    "→"
                }
            }

            div {
                class: "tile-grid column-set-{current_column_offset}",
                "data-column-offset": "{current_column_offset}",
                style: "width:100%; max-width:1200px; margin:0 auto;",
                for i in 0..columns.read().len() {
                    div {
                        class: "tile-column",
                        TileList {
                            key: "{i}",
                            tiles: columns.read()[i].tiles.clone(),
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn TileList(tiles: Vec<Tile>) -> Element {
    rsx! {
        div {
            role: "list",
            style: "display:flex; flex-direction:column; gap:4px;",
            for i in 0..tiles.len() {
                TileRow { key: "{i}", tile: tiles[i].clone() }
            }
        }
    }
}

fn ellipsize(text: &str, max_chars: usize) -> String {
    let mut chars = text.chars();
    let mut result = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        result.push('…');
    }
    result
}


#[component]
fn TileBody(tile: Tile) -> Element {
    let title = ellipsize(&tile.title, 20);
    let subtitle = ellipsize(&tile.subtitle, 24);
    let body = match tile.content {
        TileContent::Line => rsx! {
            span {
                style: "min-width:0; width:100%; display:flex; flex-direction:column; align-items:flex-start; gap:2px; overflow:hidden;",
                span {
                    style: "max-width:100%; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;",
                    "{title}"
                }
                if !subtitle.is_empty() {
                    span {
                        style: "max-width:100%; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:#9ca3af; font-size:0.72em; font-weight:400;",
                        "{subtitle}"
                    }
                }
            }
        },
        TileContent::Media { progress_percent } => rsx! {
            span {
                style: "min-width:0; width:100%; display:flex; flex-direction:column; align-items:flex-start; gap:4px; overflow:hidden;",
                span {
                    style: "max-width:100%; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;",
                    "{title}"
                }
                if !subtitle.is_empty() {
                    span {
                        style: "max-width:100%; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:#9ca3af; font-size:0.72em; font-weight:400;",
                        "{subtitle}"
                    }
                }
                span {
                    style: "width:100%; height:4px; overflow:hidden; border-radius:999px; background:#25282e;",
                    span { style: "display:block; height:100%; width:{progress_percent}%; background:#60a5fa;" }
                }
                span {
                    style: "display:flex; align-items:center; gap:18px; color:#d1d5db; font-size:18px;",
                    span { "⏮" }
                    span { style: "font-size:24px;", "▶" }
                    span { "⏭" }
                }
            }
        },
        TileContent::StickyNote { text, accent } => rsx! {
            span {
                style: "min-width:0; width:100%; display:flex; flex-direction:column; align-items:flex-start; gap:4px; padding:10px 12px; border-left:4px solid {accent}; border-radius:8px; background:#1b1d21; overflow:hidden;",
                span {
                    style: "max-width:100%; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; font-size:0.8em; color:#9ca3af;",
                    "{title}"
                }
                span { style: "max-width:100%; white-space:normal; line-height:1.35;", "{text}" }
            }
        },
    };
    rsx! { {body} }
}


#[component]
fn TileRow(tile: Tile) -> Element {
    let mut expanded = use_signal(|| false);
    let mut stack_index = use_signal(|| 0usize);
    let mut pressed = use_signal(|| false);
    let mut stack_scroll_lock = use_signal(|| None::<Instant>);

    let has_children = !tile.children.is_empty();
    let has_stack = !tile.stack.is_empty();
    let stack_len = tile.stack.len();
    let total_stack_items = stack_len + 1;
    let active_stack_index = *stack_index.read() % total_stack_items;
    let shown_tile = if has_stack {
        tile.stack[*stack_index.read() % stack_len].clone()
    } else {
        tile.clone()
    };
    let shown = shown_tile.title.clone();
    let is_line = matches!(shown_tile.content, TileContent::Line);
    let dot = match tile.kind {
        TileKind::App => "#60a5fa",
        TileKind::Widget => "#34d399",
        TileKind::Group => "#fbbf24",
    };

    rsx! {
        div {
            role: "listitem",
            style: if has_children && *expanded.read() {
                "display:flex; flex-direction:column; background:rgba(96,165,250,0.06); border-radius:12px; padding:4px 0;"
            } else {
                "display:flex; flex-direction:column;"
            },
            div {
                class: if is_line { "tile-line-group" } else { "tile-widget-group" },
                style: if is_line {
                    "display:flex; align-items:center; gap:6px; padding:0 12px;"
                } else {
                    "display:flex; align-items:flex-start; gap:6px; padding:0 12px;"
                },
                button {
                    class: if is_line { "tile-line" } else { "tile-widget" },
                    style: if is_line {
                        "flex:1; min-width:0; min-height:72px; display:flex; align-items:center; gap:18px; padding:8px 4px; border:none; color:inherit; text-align:left; font:inherit; font-size:22px; font-weight:500; letter-spacing:0.01em; cursor:pointer; background:transparent; overflow:hidden; white-space:nowrap;"
                    } else {
                        "flex:1; min-width:0; min-height:144px; display:flex; align-items:stretch; padding:0; border:none; color:inherit; text-align:left; font:inherit; cursor:pointer; background:transparent; overflow:hidden;"
                    },
                    "aria-pressed": if *pressed.read() { "true" } else { "false" },
                    onclick: move |_| {
                        let next = !*pressed.read();
                        *pressed.write() = next;
                    },
                    if is_line {
                        span { style: "width:18px; height:18px; flex:none; box-sizing:border-box; border:2px solid {dot}; border-radius:50%;" }
                    }
                    TileBody { tile: shown_tile.clone() }
                }
                if has_children {
                    button {
                        class: "tile-expand",
                        style: "flex:0 0 48px; width:48px; height:48px; border:none; padding:0; background:transparent; color:#9ca3af; font-size:24px; cursor:pointer;",
                        "aria-label": if *expanded.read() { "collapse {shown}" } else { "expand {shown}" },
                        "aria-expanded": if *expanded.read() { "true" } else { "false" },
                        onclick: move |e| {
                            e.stop_propagation();
                            let next = !*expanded.read();
                            *expanded.write() = next;
                        },
                        if *expanded.read() { "▾" } else { "▸" }
                    }
                }
                if has_stack {
                    div {
                        role: "group",
                        "aria-label": "choose a face for {shown}",
                        onwheel: move |event| {
                            let delta = event.data.delta().strip_units();
                            let movement = if delta.x.abs() > delta.y.abs() { delta.x } else { delta.y };
                            if movement == 0.0 {
                                return;
                            }
                            if stack_scroll_lock.read().is_some_and(|at| at.elapsed() < Duration::from_millis(300)) {
                                return;
                            }
                            event.stop_propagation();
                            *stack_scroll_lock.write() = Some(Instant::now());
                            let current = *stack_index.read() % total_stack_items;
                            if movement > 0.0 {
                                *stack_index.write() = (current + 1) % total_stack_items;
                            } else if movement < 0.0 {
                                *stack_index.write() = (current + total_stack_items - 1) % total_stack_items;
                            }
                        },
                        style: "display:flex; align-items:center; gap:6px; flex:none; align-self:flex-start; padding-top:12px;",
                        for index in 0..total_stack_items {
                            button {
                                class: "stack-dot",
                                "aria-label": "show face {index + 1} of {total_stack_items}",
                                "aria-current": if index == active_stack_index { "true" } else { "false" },
                                style: if index == active_stack_index {
                                    "width:10px; height:10px; padding:0; border:none; border-radius:50%; background:#f3f4f6; cursor:pointer;"
                                } else {
                                    "width:8px; height:8px; padding:0; border:none; border-radius:50%; background:#59616d; cursor:pointer;"
                                },
                                onclick: move |e| {
                                    e.stop_propagation();
                                    *stack_index.write() = index;
                                },
                                span { "aria-hidden": "true" }
                            }
                        }
                    }
                }
            }
            if has_children && *expanded.read() {
                div {
                    role: "list",
                    style: "padding:4px 0; display:flex; flex-direction:column; gap:2px;",
                    for i in 0..tile.children.len() {
                        TileRow { key: "{i}", tile: tile.children[i].clone() }
                    }
                }
            }
        }
    }
}
