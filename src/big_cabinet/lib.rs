// The `Send` auto-trait proof for the drawer `AddChild` handler's future
// (drawer/add_child.rs) unfolds subduction_keyhive's deeply generic async
// chain reached via BigRepo::remove_member_from_group, where every opaque
// `impl Future` adds several trait-selection frames. That overflows rustc's
// default 128-step recursion budget, which nightly reports as the crate-wide
// `recursion_depth_exceeding_limit` lint (rust-lang/rust#159228); raising the
// limit lets the proof complete.
#![recursion_limit = "256"]

use sqlite_rs as sqlite_rs_alias;

pub mod authority;
mod capability;
pub mod doc;
pub mod drawer;
pub mod grants;
pub mod index;
#[cfg(test)]
mod peer_net;
pub mod schema;

pub use authority::Authority;
pub use authority::AuthorityError;
pub use authority::AuthorityHandler;
pub use authority::AuthorityService;
pub use authority::boot;
pub use doc::DocAccess;
pub use doc::DocsConfig;
pub use doc::DocsHandler;
pub use doc::DocsRepo;
pub use doc::DocsService;
pub use doc::LedgerAccess;
pub use doc::boot as boot_docs;
pub use drawer::DrawerAccess;
pub use drawer::DrawerConfig;
pub use drawer::DrawerService;
pub use drawer::Drawers;
pub use drawer::DrawersHandler;
pub use drawer::boot as boot_drawers;
pub use index::datum_ref::DatumRefStore;
pub use index::datum_set::DatumSetStore;
pub use schema::SchemaHandle;
pub use schema::SchemaRegistration;
pub use schema::SchemaRegistrationGroup;
pub use schema::SchemaRepo;
pub use schema::SchemaRepoError;
