//! This crate contains all shared UI for the workspace.

mod foundation;
pub use foundation::{FoundationStyles, ThemeMode, ThemeRoot, ThemeStyles};

pub mod components;
pub use components::{
    sample_column_specs, Button, ButtonSize, ButtonVariant, Card, CardContent, CardHeader, Column,
    ColumnHeight, ColumnId, ColumnLayout, ColumnRack, ColumnRelation, ColumnSection, ColumnSource,
    ColumnSpec, ColumnState, SectionLayout, Separator, TabContent, TabList, TabTrigger, Tabs, Tile,
    TileChildren, TileContent, TileKind, TileListSpec, TileSpec, Widget,
};

/// Tiles prototype (FDR 005).
pub mod tiles;
pub use tiles::TilesDemo;

/// Arc index experiment (Niagara-style smart scroll strip).
pub mod arc_index;
pub use arc_index::ArcIndex;

/// Infinite-canvas probe (throwaway: see the module docs).
pub mod canvas_probe;
pub use canvas_probe::CanvasProbe;
