//! Pierre's diff view as a plain Dioxus component, over the `pierre_diffs` port.
//!
//! [`FileDiffView`] renders a diff with syntax highlighting, split or unified, with line
//! annotations whose content is the caller's own Dioxus `Element`. It uses standard Dioxus only
//! (no JS, no `eval`, no web-sys), so it runs on any Dioxus renderer and inside a micro.
//!
//! With `FileDiffViewProps::virtualized` set the view renders only the rows near the viewport in
//! a scroller of its own, driven by the port's `Virtualizer`: geometry comes from `onresize`,
//! `onvisible` and `onscroll`, scroll writes go through `MountedData`, and timers through a
//! `sleep` function the embedder supplies (see `host.rs`). Selection and interaction come with
//! a later step.

mod events;
mod geometry;
mod host;
mod render;
mod slot;
mod view;

pub use host::SleepFuture;
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
pub use view::Virtualization;
