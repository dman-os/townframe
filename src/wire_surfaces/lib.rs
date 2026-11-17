//! The wire surfaces the corpus publishes, as data a reviewer can diff.
//!
//! Types are authored in Rust, so nothing in a pull request shows a datum or method shape change
//! the way `public-api.txt` shows an API change. [`snapshot`] renders every Lexicon the node
//! publishes (datums, methods, the type library) and each datum's sidecar into checked-in files;
//! [`evolution`] judges a change to a Lexicon against the checked-in copy.

pub mod evolution;
mod items;
pub mod snapshot;
