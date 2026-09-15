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
    /// The tile's child list and the way this tile shows it. `None` for a leaf.
    pub sublist: Option<Sublist>,
    /// Stacked tiles occupying one slot; cycled like a carousel.
    pub stack: Vec<Tile>,
}

/// A tile's child list, paired with the way that tile shows it.
///
/// The presentation travels with the list rather than being derived from the
/// window, so a tile shows its children the same way wherever it is opened and
/// the choice stays readable in the data instead of buried in a width
/// calculation. `Column` is the primary: it is the only presentation that keeps
/// working as nesting deepens, because each level gets its own viewport rather
/// than indenting into the one above it. `Overlay` is for the case where the
/// child list is short and holding the parent's position matters more than
/// showing a path.
#[derive(Clone, Debug, PartialEq)]
pub struct Sublist {
    pub tiles: Vec<Tile>,
    pub presentation: SublistPresentation,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SublistPresentation {
    /// Beside the parent, in its own column with its own vertical scroll.
    Column,
    /// Over the parent, anchored to the tile.
    Overlay,
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
    Media { progress_percent: u8 },
    StickyNote { text: String, accent: String },
}

impl Tile {
    fn app(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            subtitle: String::new(),
            content: TileContent::Line,
            kind: TileKind::App,
            sublist: None,
            stack: vec![],
        }
    }

    fn widget(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            subtitle: String::new(),
            content: TileContent::Line,
            kind: TileKind::Widget,
            sublist: None,
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
            sublist: None,
            stack: vec![],
        }
    }

    fn sticky_note(
        title: impl Into<String>,
        text: impl Into<String>,
        accent: impl Into<String>,
    ) -> Self {
        Self {
            title: title.into(),
            subtitle: String::new(),
            content: TileContent::StickyNote {
                text: text.into(),
                accent: accent.into(),
            },
            kind: TileKind::Widget,
            sublist: None,
            stack: vec![],
        }
    }

    /// A tile whose children open beside it, in their own column.
    ///
    /// The default for nesting, and the only presentation that survives depth:
    /// a sublist can be arbitrarily long and arbitrarily deep without disturbing
    /// its parent, because it brings its own viewport.
    fn column(title: impl Into<String>, tiles: Vec<Tile>) -> Self {
        Self::with_sublist(
            title,
            Sublist {
                tiles,
                presentation: SublistPresentation::Column,
            },
        )
    }

    /// A tile whose children open over it, anchored to the tile.
    ///
    /// For short child lists where the parent's position is the point: the
    /// children arrive without pushing the rest of the parent's list around.
    fn overlay(title: impl Into<String>, tiles: Vec<Tile>) -> Self {
        Self::with_sublist(
            title,
            Sublist {
                tiles,
                presentation: SublistPresentation::Overlay,
            },
        )
    }

    fn with_sublist(title: impl Into<String>, sublist: Sublist) -> Self {
        Self {
            title: title.into(),
            subtitle: String::new(),
            content: TileContent::Line,
            kind: TileKind::Group,
            sublist: Some(sublist),
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

/// How many columns the strip shows, by window width.
///
/// Three breakpoints and nothing else. No card measures itself, no width is
/// derived from another width, and nothing decides *structure* from pixels: how
/// many columns exist is model state, and this only says how many of them are on
/// screen at once. Everything else about the layout is the browser's business.
fn visible_columns(width: f64) -> usize {
    if width > 900.0 {
        3
    } else if width > 620.0 {
        2
    } else {
        1
    }
}

/// Start with paths already open, from `DAYFRAME_OPEN`.
///
/// Semicolon-separated slots, each `slot:index,index`, where the indices are the
/// tiles opened at each depth. `DAYFRAME_OPEN=0:0,2;1:2` opens the first tile of
/// slot 0 then its third child (Daybook, then Plugs), and the third tile of
/// slot 1 (Capture, in Home).
///
/// This exists because a machine cannot click a Wayland client, so states that
/// need more than one press — and states that need presses in *two* slots at
/// once, which is where the per-slot layout arithmetic goes wrong — would
/// otherwise be reachable only by hand and could be neither checked nor
/// screenshotted. A test fixture: delete it when input can be driven.
fn seeded_path(column_count: usize) -> Vec<Vec<usize>> {
    let mut open = vec![Vec::new(); column_count];
    let Ok(spec) = std::env::var("DAYFRAME_OPEN") else {
        return open;
    };
    for slot in spec.split(';') {
        let Some((index, path)) = slot.split_once(':') else {
            continue;
        };
        let Ok(slot) = index.trim().parse::<usize>() else {
            continue;
        };
        if slot < column_count {
            open[slot] = path
                .split(',')
                .filter_map(|step| step.trim().parse().ok())
                .collect();
        }
    }
    open
}

/// A deliberately long child list: the case that motivated the column
/// presentation.
///
/// Sixty-four entries is past the point where opening inline would shove the
/// parent's siblings off the screen, and past the point where an overlay would
/// fit without becoming its own scrollable surface anyway.
fn plugs() -> Vec<Tile> {
    (0..64)
        .map(|i| {
            let kind = ["index", "ocr", "embed", "llm", "storage"][i % 5];
            Tile::app(format!("{kind}-plug-{i:03}"))
                .with_subtitle(format!("{kind} backend · revision {}", i + 1))
        })
        .collect()
}

/// The demo tile graph.
///
/// A presentation choice only shows its teeth at size: a list of three children
/// looks the same whichever way it opens, and a list of sixty does not. So the
/// sample carries one long sublist (`plugs`), one nesting depth of three
/// (`Daybook` → `Plugs` → a plug), and one short overlay to compare against.
pub fn sample_columns() -> Vec<Column> {
    vec![
        Column {
            name: "Frame".into(),
            tiles: vec![
                Tile::column(
                    "Daybook",
                    vec![
                        // Two levels below the root, so the path is exercised to
                        // the depth where inline expansion starts failing.
                        Tile::column(
                            "Drawer",
                            vec![
                                Tile::app("note alpha"),
                                Tile::app("note zulu").with_subtitle("archived"),
                            ],
                        ),
                        Tile::app("Timetable").with_subtitle("Daily schedule"),
                        Tile::column("Plugs", plugs()),
                    ],
                ),
                Tile::stacked(vec![
                    Tile::widget("Daychat: dman"),
                    Tile::widget("Daychat: work").with_subtitle("3 active threads"),
                    Tile::widget("Daychat: irl"),
                ]),
                Tile::column(
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
                // The short-list case: an overlay keeps the tiles below it where
                // they were, which is the whole argument for not opening inline.
                Tile::overlay(
                    "Manual order",
                    (0..9)
                        .map(|i| Tile::app(format!("step {}", i + 1)))
                        .collect(),
                ),
                Tile::column(
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

#[component]
pub fn TilesDemo(viewport: (f64, f64)) -> Element {
    let columns = use_signal(|| sample_columns());
    let mut column_offset = use_signal(|| 0usize);
    let mut column_scroll_lock = use_signal(|| None::<Instant>);
    // Which tile is open at each depth, per peer column. The strip is derived
    // from this, so the model stays a tree of lists and the flat column list is
    // only ever a rendering of it.
    let mut open = use_signal(move || seeded_path(columns.read().len()));

    let columns_read = columns.read();
    let opened = open.read().clone();
    let strip = strip(&columns_read, &opened);
    let strip_len = strip.len();

    // Rust decides how many columns are on screen; nothing else does, and nothing
    // hides a column behind its back. The window is the only thing rendered, so
    // there is no clipped overflow and no second place a column can be wrong.
    let width = visible_columns(viewport.0);
    let max_offset = strip_len.saturating_sub(width);
    let offset = (*column_offset.read()).min(max_offset);
    let visible: Vec<StripColumn> = strip.into_iter().skip(offset).take(width).collect();

    rsx! {
        div {
            role: "main",
            style: "min-height:100vh; box-sizing:border-box; padding:48px 28px 96px; background:#0d0e10; color:#f3f4f6; font-family:system-ui,sans-serif;",
            if max_offset > 0 {
                div {
                    class: "column-navigation",
                    "aria-label": "column navigation",
                    button {
                        class: "column-arrow",
                        "aria-label": "previous columns",
                        onclick: move |_| {
                            let current = *column_offset.read();
                            *column_offset.write() = current.saturating_sub(1);
                        },
                        "←"
                    }
                    div {
                        class: "column-pager",
                        role: "group",
                        "aria-label": "visible column set",
                        onwheel: move |event| {
                            let delta = event.data().delta().strip_units();
                            let movement = if delta.x.abs() > delta.y.abs() { delta.x } else { delta.y };
                            if movement == 0.0 {
                                return;
                            }
                            if column_scroll_lock.read().is_some_and(|at| at.elapsed() < Duration::from_millis(300)) {
                                return;
                            }
                            event.stop_propagation();
                            *column_scroll_lock.write() = Some(Instant::now());
                            let current = *column_offset.read();
                            if movement > 0.0 {
                                *column_offset.write() = (current + 1).min(max_offset);
                            } else {
                                *column_offset.write() = current.saturating_sub(1);
                            }
                        },
                        for index in 0..strip_len {
                            button {
                                class: "column-dot",
                                "aria-label": "show column {index + 1}",
                                // The whole visible window is current, not just the
                                // column the window starts on.
                                "aria-current": if index >= offset && index < offset + width { "true" } else { "false" },
                                onclick: move |event| {
                                    event.stop_propagation();
                                    *column_offset.write() = index.min(max_offset);
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
                            *column_offset.write() = (current + 1).min(max_offset);
                        },
                        "→"
                    }
                }
            }

            div {
                class: "tile-strip",
                for column in visible {
                    div {
                        key: "{column.origin.0}-{column.origin.1}",
                        class: "tile-column {column_class(&column)}",
                        div {
                            class: "strip-header",
                            if column.depth > 0 {
                                button {
                                    class: "path-back",
                                    "aria-label": "close {column.title}",
                                    onclick: move |_| {
                                        // Closing shortens the strip, which can leave
                                        // the offset past its end. The render clamps
                                        // it, so there is nothing to fix up here.
                                        let (slot, depth) = column.origin;
                                        open.write()[slot].truncate(depth - 1);
                                    },
                                    "←"
                                }
                            }
                            span { class: "strip-title", "{ellipsize(&column.title, 22)}" }
                            span { class: "strip-count", "{column.tiles.len()}" }
                        }
                        div {
                            class: "strip-scroll",
                            TileList {
                                tiles: column.tiles.clone(),
                                depth: column.depth,
                                spawner: column.opens,
                                on_open: move |index: usize| {
                                    let (slot, depth) = column.origin;
                                    // Opened columns go immediately after their
                                    // parent, so the column that just appeared is
                                    // at this position plus one. Widening the
                                    // offset to include it is what makes a press
                                    // always show the thing it opened.
                                    let revealed = strip_index(&open.read(), slot, depth) + 1;
                                    open.write()[slot].truncate(depth);
                                    open.write()[slot].push(index);
                                    // Put the column that just appeared at the trailing
                                    // edge of the window, so a press always shows the
                                    // thing it opened. Out-of-range is fine: the render
                                    // clamps against the strip's real length.
                                    *column_offset.write() = revealed.saturating_sub(width - 1);
                                },
                            }
                        }
                    }
                }
            }
        }
    }
}

/// The class a strip column wears: its surface by depth, plus the corner treatment
/// that joins it to the column it came from.
fn column_class(column: &StripColumn) -> String {
    let mut class = format!("surface-{}", column.depth.min(SURFACES));
    if column.spawned_by.is_some() {
        class.push_str(" column-nested");
    }
    if column.opens.is_some() {
        class.push_str(" column-spawns");
    }
    class
}

/// Where a `(slot, depth)` column sits in the strip: every earlier peer, and
/// everything opened under it, comes first.
fn strip_index(open: &[Vec<usize>], slot: usize, depth: usize) -> usize {
    let prefix: usize = (0..slot).map(|earlier| open[earlier].len() + 1).sum();
    prefix + depth
}

/// The surface tint a column wears. Depth 0 is the strip's own surface; nested
/// columns step through [`SURFACES`], and the tile that spawned a column wears
/// the same number, so one class carries both halves of the join.
fn tint_of(depth: usize) -> usize {
    depth.min(SURFACES)
}

/// How many distinct nested surfaces `tailwind.css` defines.
const SURFACES: usize = 3;

/// One column of the strip.
///
/// A nested list is not a layout problem. It is another column, inserted directly
/// after the column that opened it, and everything the strip already does —
/// paging, responsive visibility, scrolling — then applies to it unchanged. That
/// is why a second nested list in another column cannot break anything: there is
/// no second mechanism for it to disagree with.
#[derive(Clone, Debug, PartialEq)]
struct StripColumn {
    title: String,
    tiles: Vec<Tile>,
    /// 0 for a peer column, deeper for every nested list under it. Drives the
    /// surface tint.
    depth: usize,
    /// The tile in the *previous* column that opened this one, so that tile can
    /// wear this column's surface and the two read as one shape.
    spawned_by: Option<usize>,
    /// The tile in this column that opens the next one.
    opens: Option<usize>,
    /// `(slot, depth)` in the model, so the back control knows what to close.
    origin: (usize, usize),
}

/// Flatten the model into the column list the strip renders.
///
/// Derived, never stored: a column is a function of the tiles it came from and
/// the indices that opened it, so nothing can go stale when a list changes
/// underneath it.
fn strip(columns: &[Column], open: &[Vec<usize>]) -> Vec<StripColumn> {
    let mut out = Vec::new();
    for (slot, column) in columns.iter().enumerate() {
        out.push(StripColumn {
            title: column.name.clone(),
            tiles: column.tiles.clone(),
            depth: 0,
            spawned_by: None,
            opens: open[slot].first().copied(),
            origin: (slot, 0),
        });

        // Walk down the opened path, appending a column per level. Stops early if
        // an index no longer resolves, which can only happen if the sample data
        // changed under a path that was already open.
        let mut tiles = column.tiles.clone();
        for (depth, &index) in open[slot].iter().enumerate() {
            let Some(sublist) = tiles.get(index).and_then(|tile| tile.sublist.as_ref()) else {
                break;
            };
            out.push(StripColumn {
                title: tiles[index].title.clone(),
                tiles: sublist.tiles.clone(),
                depth: depth + 1,
                spawned_by: Some(index),
                opens: open[slot].get(depth + 1).copied(),
                origin: (slot, depth + 1),
            });
            tiles = sublist.tiles.clone();
        }
    }
    out
}

#[component]
pub fn TileList(
    tiles: Vec<Tile>,
    /// This column's depth, so a spawner knows which surface to wear: a tile
    /// wears the tint of the column it opened, not the one it sits in.
    depth: usize,
    /// The tile in this list that opened the next column, if there is one.
    spawner: Option<usize>,
    on_open: EventHandler<usize>,
) -> Element {
    rsx! {
        div {
            role: "list",
            style: "display:flex; flex-direction:column; gap:4px;",
            for i in 0..tiles.len() {
                TileRow {
                    key: "{i}",
                    tile: tiles[i].clone(),
                    index: i,
                    // This tile opened the next level, so it takes that level's
                    // surface: the same class the nested surface itself wears.
                    spawned_surface: (spawner == Some(i)).then(|| tint_of(depth + 1)),
                    on_open,
                }
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
fn TileRow(
    tile: Tile,
    index: usize,
    on_open: EventHandler<usize>,
    /// `Some(n)` when this tile opened the level that wears `surface-n`.
    spawned_surface: Option<usize>,
) -> Element {
    let mut stack_index = use_signal(|| 0usize);
    let mut stack_scroll_lock = use_signal(|| None::<Instant>);

    // Whether this tile has anything to reveal. A tile with a sublist is a button
    // that opens it; a leaf is a plain row. There is no in-between, so there is
    // no button that does nothing and no action to fake.
    //
    // The overlay presentation is modelled but not built, so both presentations
    // open as a nested surface for now. That keeps a tile declaring `Overlay`
    // from being a button that does nothing while it waits.
    let revealable = tile.sublist.is_some();
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

    // The row's own class, gathered here because the spawner surface has to come
    // after the hover rule in the stylesheet to win over it.
    let mut group_class = String::from(if is_line {
        "tile-line-group"
    } else {
        "tile-widget-group"
    });
    if let Some(surface) = spawned_surface {
        group_class = format!("{group_class} surface-{surface}");
    }

    let body = rsx! {
        if is_line {
            span {
                style: "width:18px; height:18px; flex:none; box-sizing:border-box; border:2px solid {dot}; border-radius:50%;"
            }
        }
        TileBody { tile: shown_tile.clone() }
    };

    rsx! {
        div {
            role: "listitem",
            class: "{group_class}",
            style: if is_line {
                "display:flex; align-items:center; gap:6px; padding:0 12px;"
            } else {
                "display:flex; align-items:flex-start; gap:6px; padding:0 12px;"
            },
            if revealable {
                button {
                    class: if is_line { "tile-line" } else { "tile-widget" },
                    style: if is_line {
                        "flex:1; min-width:0; min-height:72px; display:flex; align-items:center; gap:18px; padding:8px 4px; border:none; color:inherit; text-align:left; font:inherit; font-size:22px; font-weight:500; letter-spacing:0.01em; cursor:pointer; background:transparent; overflow:hidden; white-space:nowrap;"
                    } else {
                        "flex:1; min-width:0; min-height:144px; display:flex; align-items:stretch; padding:0; border:none; color:inherit; text-align:left; font:inherit; cursor:pointer; background:transparent; overflow:hidden;"
                    },
                    // The rendered title is ellipsized to fit, so the accessible
                    // name has to come from the aria-label instead — otherwise an
                    // AT reads out the truncation.
                    "aria-label": "{tile.title}",
                    // Whether the level this tile opened is the one on screen is
                    // not something the row can know, so this reports that the
                    // tile opens something rather than claiming a state.
                    "aria-haspopup": "true",
                    onclick: move |_| on_open.call(index),
                    {body}
                }
            } else {
                div {
                    class: if is_line { "tile-line" } else { "tile-widget" },
                    style: if is_line {
                        "flex:1; min-width:0; min-height:72px; display:flex; align-items:center; gap:18px; padding:8px 4px; font-size:22px; font-weight:500; letter-spacing:0.01em; overflow:hidden; white-space:nowrap;"
                    } else {
                        "flex:1; min-width:0; min-height:144px; display:flex; align-items:stretch; overflow:hidden;"
                    },
                    {body}
                }
            }
            if has_stack {
                div {
                    role: "group",
                    "aria-label": "choose a face for {shown}",
                    onwheel: move |event| {
                        let delta = event.data().delta().strip_units();
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
                    for face in 0..total_stack_items {
                        button {
                            class: "stack-dot",
                            "aria-label": "show face {face + 1} of {total_stack_items}",
                            "aria-current": if face == active_stack_index { "true" } else { "false" },
                            style: if face == active_stack_index {
                                "width:10px; height:10px; padding:0; border:none; border-radius:50%; background:#f3f4f6; cursor:pointer;"
                            } else {
                                "width:8px; height:8px; padding:0; border:none; border-radius:50%; background:#59616d; cursor:pointer;"
                            },
                            onclick: move |e| {
                                e.stop_propagation();
                                *stack_index.write() = face;
                            },
                            span { "aria-hidden": "true" }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn titles(strip: &[StripColumn]) -> Vec<&str> {
        strip.iter().map(|column| column.title.as_str()).collect()
    }

    #[test]
    fn the_window_widens_with_the_window() {
        assert_eq!(visible_columns(500.0), 1);
        assert_eq!(visible_columns(800.0), 2);
        assert_eq!(visible_columns(1400.0), 3);
    }

    /// Nothing open: the strip is the peer columns and nothing else.
    #[test]
    fn the_strip_starts_as_the_peer_columns() {
        let strip = strip(&sample_columns(), &[vec![], vec![], vec![]]);
        assert_eq!(titles(&strip), ["Frame", "Home", "Widgets"]);
        assert!(strip.iter().all(|column| column.depth == 0));
        assert!(strip.iter().all(|column| column.opens.is_none()));
    }

    /// A nested list is a column immediately after the one that opened it, and
    /// the two ends of the join point at each other.
    #[test]
    fn a_nested_list_is_a_column_after_its_parent() {
        let strip = strip(&sample_columns(), &[vec![0, 2], vec![], vec![]]);

        assert_eq!(
            titles(&strip),
            ["Frame", "Daybook", "Plugs", "Home", "Widgets"]
        );
        assert_eq!(strip[0].opens, Some(0));
        assert_eq!(strip[1].spawned_by, Some(0));
        assert_eq!(strip[1].opens, Some(2));
        assert_eq!(strip[2].spawned_by, Some(2));
        assert_eq!(strip[2].depth, 2);
        assert_eq!(strip[2].tiles.len(), 64);
    }

    /// The case that broke: a nested list opening in a *second* column must land
    /// after its own parent, not beside the first column's nesting. There is no
    /// second mechanism here for it to disagree with, which is the point.
    #[test]
    fn a_second_column_nesting_lands_after_its_own_parent() {
        let strip = strip(&sample_columns(), &[vec![0], vec![2], vec![]]);

        assert_eq!(
            titles(&strip),
            ["Frame", "Daybook", "Home", "Capture", "Widgets"]
        );
        assert_eq!(strip[1].spawned_by, Some(0));
        assert_eq!(strip[3].spawned_by, Some(2));
        assert_eq!(strip[3].origin, (1, 1));
        // And the columns that were never asked about are untouched.
        assert_eq!(strip[2].tiles.len(), 3);
        assert_eq!(strip[4].tiles.len(), 3);
    }

    /// Strip positions have to match the flattened order, because that is what
    /// decides where the window goes when a column opens.
    #[test]
    fn strip_positions_match_the_flattened_order() {
        let open = vec![vec![0], vec![2], vec![]];
        let strip = strip(&sample_columns(), &open);

        for (position, column) in strip.iter().enumerate() {
            let (slot, depth) = column.origin;
            assert_eq!(strip_index(&open, slot, depth), position);
        }
    }

    /// Opening a column scrolls it into view: the window lands so the new column
    /// is at its trailing edge, where the eye already is.
    #[test]
    fn a_newly_opened_column_lands_inside_the_window() {
        let open = vec![vec![0], vec![2], vec![]];
        let width = 3;

        // Opening slot 1's third tile (Capture) puts its column at strip index 3.
        let revealed = strip_index(&open, 1, 0) + 1;
        assert_eq!(revealed, 3);

        let offset = revealed.saturating_sub(width - 1);
        assert!(offset <= revealed && revealed < offset + width);
    }

    /// An index that no longer resolves stops the walk rather than panicking: the
    /// path is model state and the sample data can change under it.
    #[test]
    fn a_dangling_index_truncates_the_path() {
        let strip = strip(&sample_columns(), &[vec![0, 99], vec![], vec![]]);
        assert_eq!(titles(&strip), ["Frame", "Daybook", "Home", "Widgets"]);
    }
}
