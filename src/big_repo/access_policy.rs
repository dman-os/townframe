//! Keyhive-governed access policy for big_repo's part store.
//!
//! Access control policy is built directly into [`big_sync::HostPartStore`].
//! Membership rules (`big_sync_syncable` SQL table) are queried directly by
//! [`crate::SqliteBigRepoStore`] and [`big_sync::SqlitePartStore`], and updated
//! atomically within store write transactions.
