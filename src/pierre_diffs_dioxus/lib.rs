//! The Dioxus layer of the `pierre_diffs` port (placeholder).
//!
//! The web-sys `WebHost` of S10 was removed with the host-trait reshaping of
//! S12a-2: the structural half of every manager host is now the port's
//! `pierre_diffs::document::Document`, so a platform host supplies only
//! geometry, events, observers, timers and scroll writes over Dioxus's
//! standard events (`onscroll`, `onresize`, `onvisible`, `MountedData`), with
//! `NodeKey` as the element type. That host and the rendering of the
//! `Document` as Dioxus elements are built here next; nothing is assumed about
//! JavaScript or `web-sys` (see `docs/scratch/pierre-dioxus-host.md`).
