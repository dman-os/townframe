use dioxus::prelude::*;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnId(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnRelation {
    Root,
    Child { parent: ColumnId },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnSource {
    Static,
    Child { parent: ColumnId },
    Paged { cursor: Option<String> },
    Subscription,
    Query { key: String },
    Spatial { region: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnState {
    Unrequested,
    Loading,
    Ready,
    Empty,
    Stale,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnHeight {
    Natural,
    Bounded,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ColumnLayout {
    #[default]
    Linear,
    Canvas,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TileKind {
    App,
    Group,
    Widget,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TileContent {
    Line,
    Media,
    Thumbnail { src: String, alt: String },
    StickyNote,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SectionLayout {
    #[default]
    List,
    DenseGallery {
        columns: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TileChildren {
    Inline(TileListSpec),
    Popup(TileListSpec),
    Column(Box<ColumnSpec>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TileListSpec {
    pub source: ColumnSource,
    pub state: ColumnState,
    pub sections: Vec<ColumnSection>,
    pub total: Option<usize>,
    pub has_next: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TileSpec {
    pub id: String,
    pub title: String,
    pub subtitle: String,
    pub kind: TileKind,
    pub content: TileContent,
    pub body: Vec<String>,
    pub children: Option<TileChildren>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnSection {
    pub title: Option<String>,
    pub layout: SectionLayout,
    pub tiles: Vec<TileSpec>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnSpec {
    pub id: ColumnId,
    pub title: String,
    pub relation: ColumnRelation,
    pub list: TileListSpec,
}

/// Small deterministic fixture used by the native showcase shell.
pub fn sample_column_specs() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            id: ColumnId("frame".to_string()),
            title: "Frame".to_string(),
            relation: ColumnRelation::Root,
            list: TileListSpec {
                source: ColumnSource::Static,
                state: ColumnState::Ready,
                sections: vec![ColumnSection {
                    title: Some("Primary".to_string()),
                    layout: SectionLayout::List,
                    tiles: vec![
                        TileSpec {
                            id: "daybook".to_string(),
                            title: "Daybook".to_string(),
                            subtitle: "Group · Column sublist".to_string(),
                            kind: TileKind::Group,
                            content: TileContent::Line,
                            body: Vec::new(),
                            children: None,
                        },
                        TileSpec {
                            id: "daychat".to_string(),
                            title: "Daychat".to_string(),
                            subtitle: "App · active threads".to_string(),
                            kind: TileKind::App,
                            content: TileContent::Line,
                            body: Vec::new(),
                            children: None,
                        },
                        TileSpec {
                            id: "daybrowser".to_string(),
                            title: "Daybrowser".to_string(),
                            subtitle: "App · recent documents".to_string(),
                            kind: TileKind::App,
                            content: TileContent::Line,
                            body: Vec::new(),
                            children: None,
                        },
                    ],
                }],
                total: Some(3),
                has_next: false,
            },
        },
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
                            content: TileContent::Line,
                            body: Vec::new(),
                            children: None,
                        },
                        TileSpec {
                            id: "capture".to_string(),
                            title: "Capture".to_string(),
                            subtitle: "Group · Column sublist".to_string(),
                            kind: TileKind::Group,
                            content: TileContent::Line,
                            body: Vec::new(),
                            children: None,
                        },
                    ],
                }],
                total: None,
                has_next: true,
            },
        },
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
                            subtitle: "Widget · Media".to_string(),
                            kind: TileKind::Widget,
                            content: TileContent::Media,
                            body: Vec::new(),
                            children: None,
                        },
                        TileSpec {
                            id: "scratchpad".to_string(),
                            title: "Scratchpad".to_string(),
                            subtitle: "Widget · StickyNote".to_string(),
                            kind: TileKind::Widget,
                            content: TileContent::StickyNote,
                            body: Vec::new(),
                            children: None,
                        },
                    ],
                }],
                total: Some(2),
                has_next: false,
            },
        },
    ]
}

#[component]
pub fn ColumnRack(
    columns: Vec<ColumnSpec>,
    height: ColumnHeight,
    #[props(default = 280.0)] min_column_width: f64,
    available_width: f64,
    #[props(default)] layout: ColumnLayout,
) -> Element {
    let column_count = columns.len();
    let visible_count = if column_count == 0 {
        1
    } else {
        (((available_width + 24.0) / (min_column_width + 24.0)).floor() as usize)
            .clamp(1, column_count)
    };
    let mut offset = use_signal(|| 0usize);
    let mut scroll_lock = use_signal(|| None::<Instant>);
    let max_offset = column_count.saturating_sub(visible_count);
    let current_offset = (*offset.read()).min(max_offset);
    let track_style = format!("grid-template-columns:repeat({visible_count},minmax(0,1fr));");
    let visible_columns = columns.into_iter().skip(current_offset).take(visible_count);

    rsx! {
        section {
            class: "df-column-rack",
            "data-layout": layout.as_str(),
            "data-height": height.as_str(),
            if max_offset > 0 {
                div {
                    class: "column-navigation",
                    "aria-label": "column navigation",
                    button {
                        class: "column-arrow",
                        "aria-label": "previous columns",
                        disabled: current_offset == 0,
                        onclick: move |_| {
                            *offset.write() = current_offset.saturating_sub(1);
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
                            event.prevent_default();
                            event.stop_propagation();
                            if scroll_lock
                                .read()
                                .is_some_and(|at| at.elapsed() < Duration::from_millis(300))
                            {
                                return;
                            }
                            *scroll_lock.write() = Some(Instant::now());
                            if movement > 0.0 {
                                *offset.write() = (current_offset + 1).min(max_offset);
                            } else {
                                *offset.write() = current_offset.saturating_sub(1);
                            }
                        },
                        for index in 0..column_count {
                            button {
                                class: "column-dot",
                                "data-window-position": if index < current_offset || index >= current_offset + visible_count { "inactive" } else if visible_count == 1 { "single" } else if index == current_offset { "leading" } else if index == current_offset + visible_count - 1 { "trailing" } else { "middle" },
                                "aria-label": "show column {index + 1}",
                                "aria-current": if index >= current_offset && index < current_offset + visible_count { "true" } else { "false" },
                                "data-selected": if index >= current_offset && index < current_offset + visible_count { "true" } else { "false" },
                                onclick: move |_| {
                                    *offset.write() = index.min(max_offset);
                                },
                                span { "" }
                            }
                        }
                    }
                    button {
                        class: "column-arrow",
                        "aria-label": "next columns",
                        disabled: current_offset == max_offset,
                        onclick: move |_| {
                            *offset.write() = (current_offset + 1).min(max_offset);
                        },
                        "→"
                    }
                }
            }
            div {
                class: "df-column-rack-window",
                style: track_style,
                for column in visible_columns {
                    Column { column }
                }
            }
        }
    }
}

#[component]
fn TileList(list: TileListSpec, depth: usize) -> Element {
    let body = match list.state {
        ColumnState::Unrequested => rsx! {
            div { class: "df-column-state", "Not loaded" }
        },
        ColumnState::Loading => rsx! {
            div { class: "df-column-state", "Loading…" }
        },
        ColumnState::Empty => rsx! {
            div { class: "df-column-state", "Nothing here" }
        },
        ColumnState::Failed => rsx! {
            div { class: "df-column-state df-column-state-error", "Unable to load this list" }
        },
        ColumnState::Ready | ColumnState::Stale => {
            if list.sections.iter().all(|section| section.tiles.is_empty()) {
                rsx! { div { class: "df-column-state", "Nothing here" } }
            } else {
                rsx! {
                    for list_section in list.sections.iter() {
                        div {
                            class: "df-column-section",
                            if let Some(title) = &list_section.title {
                                h3 {
                                    class: "df-column-section-header",
                                    "{title}"
                                }
                            }
                            div {
                                class: match list_section.layout {
                                    SectionLayout::List => "df-column-list",
                                    SectionLayout::DenseGallery { .. } => "df-column-gallery",
                                },
                                style: match list_section.layout {
                                    SectionLayout::List => String::new(),
                                    SectionLayout::DenseGallery { columns } => {
                                        format!(
                                            "grid-template-columns: repeat({}, minmax(0, 1fr));",
                                            columns.max(1)
                                        )
                                    }
                                },
                                role: "list",
                                for tile in list_section.tiles.iter() {
                                    Tile { tile: tile.clone(), depth, layout: list_section.layout }
                                }
                            }
                        }
                    }
                    if list.has_next {
                        div { class: "df-column-pagination", "More available" }
                    }
                }
            }
        }
    };

    rsx! { {body} }
}

#[component]
pub fn Column(column: ColumnSpec) -> Element {
    let list = column.list;
    let state = list.state;
    let source = list.source.clone();
    let total = list.total;

    rsx! {
        article {
            class: "df-column",
            "data-state": state.as_str(),
            "data-relation": column.relation.as_str(),
            "data-source": source.as_str(),
            header {
                class: "df-column-header",
                strong { class: "df-column-title", "{column.title}" }
                span { class: "df-column-count", if let Some(total) = total { "{total}" } else { "…" } }
            }
            div {
                class: "df-column-scroll",
                TileList { list, depth: 0 }
            }
        }
    }
}

#[component]
pub fn Tile(tile: TileSpec, depth: usize, layout: SectionLayout) -> Element {
    let inline_list = match tile.children.clone() {
        Some(TileChildren::Inline(list)) => Some(list),
        Some(TileChildren::Popup(_)) | Some(TileChildren::Column(_)) | None => None,
    };
    let has_inline_children = inline_list.is_some();
    let mut expanded = use_signal(|| false);
    let inline_id = format!("inline-{}", tile.id);

    let body = if matches!(layout, SectionLayout::DenseGallery { .. }) {
        rsx! {
            TileGallery {
                tile: tile.clone(),
                has_inline_children,
                expanded: expanded(),
                inline_id: inline_id.clone(),
                on_toggle: move |event: MouseEvent| {
                    event.stop_propagation();
                    expanded.toggle();
                },
            }
        }
    } else {
        match tile.kind {
            TileKind::Widget => rsx! { Widget { tile: tile.clone() } },
            TileKind::App | TileKind::Group => rsx! {
                TileLine {
                    tile: tile.clone(),
                    has_inline_children,
                    expanded: expanded(),
                    inline_id: inline_id.clone(),
                    on_toggle: move |event: MouseEvent| {
                        event.stop_propagation();
                        expanded.toggle();
                    },
                }
            },
        }
    };
    rsx! {
        div {
            class: if has_inline_children { "df-tile df-tile-inline-parent" } else { "df-tile" },
            "data-kind": tile.kind.as_str(),
            "data-content": tile.content.as_str(),
            "data-expanded": expanded.to_string(),
            "data-depth": depth.min(3).to_string(),
            role: "listitem",
            {body}
            if expanded() {
                if let Some(list) = inline_list.clone() {
                    div {
                        id: inline_id,
                        class: "df-inline-children",
                        TileList { list, depth: depth + 1 }
                    }
                }
            }
        }
    }
}

#[component]
fn TileGallery(
    tile: TileSpec,
    has_inline_children: bool,
    expanded: bool,
    inline_id: String,
    on_toggle: EventHandler<MouseEvent>,
) -> Element {
    rsx! {
        article {
            class: "df-gallery-tile",
            "aria-label": tile.title.clone(),
            div {
                class: "df-gallery-media",
                match &tile.content {
                    TileContent::Thumbnail { src, alt } => rsx! {
                        img { src: src.clone(), alt: alt.clone() }
                    },
                    _ => rsx! {
                        div { class: "df-gallery-placeholder", "Photo" }
                    },
                }
                if has_inline_children {
                    button {
                        class: "df-gallery-toggle",
                        r#type: "button",
                        "aria-label": if expanded { "collapse nested items" } else { "expand nested items" },
                        "aria-expanded": expanded.to_string(),
                        "aria-controls": inline_id,
                        onclick: move |event| on_toggle.call(event),
                        if expanded { "⌃" } else { "⌄" }
                    }
                }
            }
        }
    }
}

#[component]
fn TileLine(
    tile: TileSpec,
    has_inline_children: bool,
    expanded: bool,
    inline_id: String,
    on_toggle: EventHandler<MouseEvent>,
) -> Element {
    rsx! {
        div {
            class: "df-tile-line",
            span { class: "df-tile-leading", "{tile.kind.mark()}" }
            span {
                class: "df-tile-copy",
                strong { class: "df-tile-title", "{tile.title}" }
                if !tile.subtitle.is_empty() {
                    small { class: "df-tile-subtitle", "{tile.subtitle}" }
                }
                for paragraph in tile.body.iter() {
                    p { class: "df-tile-body", "{paragraph}" }
                }
            }
            if has_inline_children {
                button {
                    class: "df-tile-trailing df-tile-toggle",
                    r#type: "button",
                    "aria-label": if expanded { "collapse nested items" } else { "expand nested items" },
                    "aria-expanded": expanded.to_string(),
                    "aria-controls": inline_id,
                    onclick: move |event| on_toggle.call(event),
                    if expanded { "⌃" } else { "⌄" }
                }
            } else if tile.kind == TileKind::Group {
                span { class: "df-tile-trailing", "↗" }
            }
        }
    }
}

#[component]
pub fn Widget(tile: TileSpec) -> Element {
    rsx! {
        div {
            class: "df-widget",
            "data-content": tile.content.as_str(),
            span { class: "df-widget-leading", "{tile.content.mark()}" }
            span {
                class: "df-tile-copy",
                strong { class: "df-tile-title", "{tile.title}" }
                if !tile.subtitle.is_empty() {
                    small { class: "df-tile-subtitle", "{tile.subtitle}" }
                }
                for paragraph in tile.body.iter() {
                    p { class: "df-tile-body", "{paragraph}" }
                }
            }
        }
    }
}

impl ColumnHeight {
    fn as_str(self) -> &'static str {
        match self {
            Self::Natural => "natural",
            Self::Bounded => "bounded",
        }
    }
}

impl ColumnLayout {
    fn as_str(self) -> &'static str {
        match self {
            Self::Linear => "linear",
            Self::Canvas => "canvas",
        }
    }
}

impl ColumnState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unrequested => "unrequested",
            Self::Loading => "loading",
            Self::Ready => "ready",
            Self::Empty => "empty",
            Self::Stale => "stale",
            Self::Failed => "failed",
        }
    }
}

impl ColumnRelation {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::Child { .. } => "child",
        }
    }
}

impl ColumnSource {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Static => "static",
            Self::Child { .. } => "child",
            Self::Paged { .. } => "paged",
            Self::Subscription => "subscription",
            Self::Query { .. } => "query",
            Self::Spatial { .. } => "spatial",
        }
    }
}

impl TileKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::App => "app",
            Self::Group => "group",
            Self::Widget => "widget",
        }
    }

    fn mark(self) -> &'static str {
        match self {
            Self::App => "A",
            Self::Group => "◈",
            Self::Widget => "W",
        }
    }
}

impl TileContent {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Line => "line",
            Self::Media => "media",
            Self::Thumbnail { .. } => "thumbnail",
            Self::StickyNote => "sticky-note",
        }
    }

    fn mark(&self) -> &'static str {
        match self {
            Self::Line => "•",
            Self::Media => "▶",
            Self::Thumbnail { .. } => "▧",
            Self::StickyNote => "✦",
        }
    }
}
