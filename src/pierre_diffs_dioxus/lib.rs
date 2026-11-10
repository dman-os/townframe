//! Pierre's diff view as a plain Dioxus component, over the `pierre_diffs` port.
//!
//! [`FileDiffView`] renders a diff with syntax highlighting, split or unified, with line
//! annotations whose content is the caller's own Dioxus `Element`. It uses standard Dioxus only
//! (no JS, no `eval`, no web-sys), so it runs on any Dioxus renderer and inside a micro.
//!
//! This is the static step of the host: it renders a non-virtualized `FileDiff`. Observers,
//! virtualization, scroll sync and interaction arrive with the interactive step; the methods of
//! the host they will need are marked `todo!` in `host.rs`.

mod host;
mod render;
mod slot;
mod view;

pub use pierre_diffs::highlighter_types::DiffsHighlighter;
pub use pierre_diffs::highlighter_types::PlainTextHighlighter;
pub use pierre_diffs::js_collections::PtrEq;
pub use pierre_diffs::types::options::DiffStyle;
pub use pierre_diffs::types::patch::FileContents;
pub use pierre_diffs::types::view::AnnotationSide;
pub use pierre_diffs::types::view::DiffLineAnnotation;
pub use slot::SlotContent;
pub use view::DiffInput;
pub use view::FileDiffView;
pub use view::FileDiffViewProps;
