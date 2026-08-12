//! LRU policy engine and keyed pool, moved to `utils_rs::lru`.
//!
//! Kept as a re-export module so existing paths
//! (`daybook_core::drawer::lru::KeyedLruPool`, `SharedKeyedLruPool`) keep
//! working unchanged.

pub use utils_rs::lru::*;
