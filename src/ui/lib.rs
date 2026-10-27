//! This crate contains all shared UI for the workspace.

mod foundation;
pub use foundation::FoundationStyles;
pub use foundation::ThemeMode;
pub use foundation::ThemeRoot;
pub use foundation::ThemeStyles;

pub mod components;
pub use components::Button;
pub use components::ButtonSize;
pub use components::ButtonVariant;
pub use components::Card;
pub use components::CardContent;
pub use components::CardHeader;
pub use components::Column;
pub use components::ColumnHeight;
pub use components::ColumnId;
pub use components::ColumnLayout;
pub use components::ColumnRack;
pub use components::ColumnRelation;
pub use components::ColumnSection;
pub use components::ColumnSource;
pub use components::ColumnSpec;
pub use components::ColumnState;
pub use components::SectionLayout;
pub use components::Separator;
pub use components::TabContent;
pub use components::TabList;
pub use components::TabTrigger;
pub use components::Tabs;
pub use components::Tile;
pub use components::TileChildren;
pub use components::TileContent;
pub use components::TileKind;
pub use components::TileListSpec;
pub use components::TileSpec;
pub use components::Widget;
pub use components::sample_column_specs;

/// Tiles prototype (FDR 005).
pub mod tiles;
pub use tiles::TilesDemo;

/// Arc index experiment (Niagara-style smart scroll strip).
pub mod arc_index;
pub use arc_index::ArcIndex;

/// Infinite-canvas probe (throwaway: see the module docs).
pub mod canvas_probe;
pub use canvas_probe::CanvasProbe;
