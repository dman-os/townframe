//! Pierre's diff view as a plain Dioxus component, over the `pierre_diffs` port.
//!
//! [`FileDiffView`] renders a diff with syntax highlighting, split or unified, with line
//! annotations whose content is the caller's own Dioxus `Element`. It uses standard Dioxus only
//! (no JS, no `eval`, no web-sys), so it runs on any Dioxus renderer and inside a micro.
//!
//! With `FileDiffViewProps::virtualized` set the view renders only the rows near the viewport in
//! a scroller of its own, driven by the port's `Virtualizer`: geometry comes from `onresize`,
//! `onvisible` and `onscroll`, scroll writes go through `MountedData`, and timers through a
//! `sleep` function the embedder supplies (see `host.rs`). With `FileDiffViewProps::interaction`
//! the pointer hovers, selects and expands: events reach the port's `InteractionManager` through
//! handlers on the rendered nodes (see `host.rs`, "Pointers").
//!
//! [`CodeViewView`] is Pierre's `CodeView`: many files in one virtualized scroller, each from a
//! patch or from two versions of the file, with the same annotation slots, and a
//! [`ScrollRequest`] that reveals a line or a file, and `on_top_file`, which reports the file at the
//! top of the viewport. It scrolls in a scroller of its own or, with [`ScrollMode::Embedder`], in
//! one the embedder owns and forwards to a [`ScrollRoot`] (`scroll_root.rs`). Its host and the
//! slab's per-frame signal path are in `host/code_view.rs` and `render.rs`; it is view-only for
//! now (see `code_view.rs`).

mod code_view;
mod events;
mod geometry;
mod host;
mod render;
mod scroll_root;
mod slot;
mod view;

pub use code_view::CodeViewFile;
pub use code_view::CodeViewView;
pub use code_view::CodeViewViewProps;
pub use code_view::FileAnnotation;
pub use code_view::Runtime;
pub use code_view::ScrollMode;
pub use code_view::ScrollRequest;
pub use host::Environment;
pub use host::SleepFuture;
pub use pierre_diffs::highlighter_types::DiffsHighlighter;
pub use pierre_diffs::highlighter_types::PlainTextHighlighter;
pub use pierre_diffs::js_collections::PtrEq;
pub use pierre_diffs::managers::interaction_manager::LineHoverHighlight;
pub use pierre_diffs::types::layout::CodeViewLayout;
pub use pierre_diffs::types::options::DiffStyle;
pub use pierre_diffs::types::patch::FileContents;
pub use pierre_diffs::types::view::AnnotationSide;
pub use pierre_diffs::types::view::CodeViewScrollTarget;
pub use pierre_diffs::types::view::DiffLineAnnotation;
pub use pierre_diffs::types::view::SelectedLineRange;
pub use pierre_diffs::types::view::SelectionSide;
pub use pierre_diffs::utils::dom::platform::PlatformInfo;
pub use scroll_root::ScrollRoot;
pub use scroll_root::use_scroll_root;
pub use slot::SlotContent;
pub use view::DiffInput;
pub use view::FileDiffView;
pub use view::FileDiffViewProps;
pub use view::Interaction;
pub use view::Virtualization;
