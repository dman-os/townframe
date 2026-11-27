//! The replacement for `big_repo`'s runtime.
//!
//! One SQLite database holds all of a node's replicated state. Every state change is persisted
//! first and applied to memory after (persist-then-apply), and the protocol logic is sans-IO
//! machines that take events and return effects, so the same code runs on native, `wasm32-wasip3`
//! and the browser (`wasm32-unknown-unknown`).
//!
//! The crate sits on the `townframe-changes` forks of keyhive and subduction, which are newer
//! than the revisions `big_repo` is pinned to; the two coexist in one workspace until `big_repo`
//! is retired.

pub mod coordinator;
pub mod document;
pub mod executor;
pub mod hub;
pub mod keyhive_boundary;
pub mod sedimentree;

mod sqlite {
    pub mod queries {
        include!("sqlite/gen.rs");
    }
}

use sqlite_rs as sqlite_rs_alias;

pub use sqlite::queries::MIGRATION_TABLE;
pub use sqlite::queries::MIGRATIONS;

// Declared for the lanes that build on this skeleton; nothing here calls them yet.
use subduction_keyhive as _;

// Compile-time proof that the SQLite Keyline backend satisfies the fork's `Keyline` bound: the
// `keyline` named here and the one `keyline_sqlite` implements are the same crate, so this fails
// to compile if the two ever resolve to different sources.
const _: () = {
    use future_form::FutureForm;
    use future_form::Local;
    use future_form::Sendable;
    use keyhive_codec::traits::Decode;
    use keyhive_codec::traits::Encode;
    use keyline::contract::Keyline;
    use keyline_sqlite::SqliteKeyline;
    use sqlite_rs::SqliteDb;

    fn requires_keyline<F: FutureForm, K: Keyline<F>>(_: &K) {}

    // `keyline_sqlite` implements `Keyline` once per future form, with `Send + Sync` bounds on the
    // watermark for `Sendable`.
    #[expect(dead_code, reason = "exists only to be type checked")]
    fn sqlite_keyline_is_the_forks_keyline<W, D>(keyline: &SqliteKeyline<W, D>)
    where
        W: Encode + Decode + Send + Sync,
        D: SqliteDb,
    {
        requires_keyline::<Sendable, _>(keyline);
        requires_keyline::<Local, _>(keyline);
    }
};
