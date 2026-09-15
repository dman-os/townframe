//! This crate contains all shared UI for the workspace.

mod hero;
pub use hero::Hero;

mod navbar;
pub use navbar::Navbar;

/// Tiles prototype (FDR 005).
pub mod tiles;
pub use tiles::TilesDemo;

/// Arc index experiment (Niagara-style smart scroll strip).
pub mod arc_index;
pub use arc_index::ArcIndex;

/// Infinite-canvas probe (throwaway: see the module docs).
pub mod canvas_probe;
pub use canvas_probe::CanvasProbe;
